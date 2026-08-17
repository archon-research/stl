"""Self-computed per-prime risk capital.

Composes three sources, with no dependency on the upstream Star feed:

- **exposure** and the allocation set: the prime's priced receipt-token positions.
- **total risk capital**: the on-chain SubProxy treasury (latest balance).
- **required risk capital**: the per-allocation RRC from the default model
  (``gap_sweep``), summed. Allocations the model cannot price contribute 0.

The result is model-derived and partial by design (see
``app/domain/entities/prime_risk_capital.py``).
"""

import asyncio
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal

from app.domain.chain_names import chain_is_served
from app.domain.entities.allocation import EthAddress
from app.domain.entities.backed_breakdown import BackedBreakdown
from app.domain.entities.prime_risk_capital import (
    AllocationRiskCapital,
    ChainRiskCapital,
    PrimeRiskCapital,
    UnpricedReason,
)
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.entities.risk import RrcResult
from app.domain.exceptions import AllocationUnpricedError
from app.domain.prime_registry import alm_proxies_for_prime, prime_name_for
from app.logging import get_logger
from app.ports.allocation_repository import AllocationRepositoryPort
from app.services.crypto_lending_risk_service import CryptoLendingRiskService
from app.services.model_registry import ModelRegistry

logger = get_logger(__name__)

# The default RRC model. SURAF is under development and CORE is not yet landed,
# so gap_sweep (on-chain crypto-lending stress) is the reliable default today.
DEFAULT_RISK_MODEL = "gap_sweep"

_RATIO = Decimal("0.0001")  # ratios/percentages to 4 dp

# ``unpriced_reason`` value for an allocation no default model applies to (as
# opposed to the ``AllocationUnpricedError.code`` values used when a model applies
# but the allocation cannot be priced — a share-data or price-data gap).
_UNPRICED_NO_MODEL: UnpricedReason = "no_model"


def _ratio(numerator: Decimal, denominator: Decimal | None) -> Decimal | None:
    """Return ``numerator / denominator`` to 4 dp, or ``None`` when undefined."""
    if denominator is None or denominator <= 0:
        return None
    return (numerator / denominator).quantize(_RATIO, rounding=ROUND_HALF_EVEN)


def _unpriced_allocation(position, exposure: Decimal, reason: UnpricedReason) -> AllocationRiskCapital:
    """Build an unpriced per-allocation entry (no RRC), tagged with ``reason``."""
    return AllocationRiskCapital(
        receipt_token_id=position.receipt_token_id,
        symbol=position.symbol,
        protocol_name=position.protocol_name,
        exposure_usd=exposure,
        applied=False,
        required_risk_capital_usd=None,
        crr_pct=None,
        model=None,
        unpriced_reason=reason,
    )


def _priced_allocation(position, exposure: Decimal, result: RrcResult) -> AllocationRiskCapital:
    """Build a priced per-allocation entry from a model ``result``."""
    return AllocationRiskCapital(
        receipt_token_id=position.receipt_token_id,
        symbol=position.symbol,
        protocol_name=position.protocol_name,
        exposure_usd=exposure,
        applied=True,
        required_risk_capital_usd=result.rrc_usd,
        crr_pct=result.comparable_crr_pct,
        model=result.risk_model,
        unpriced_reason=None,
    )


def _assemble_allocations(positions, models, results) -> tuple[Decimal, Decimal, Decimal, list[AllocationRiskCapital]]:
    """Fold per-allocation compute results into totals + a per-allocation list.

    ``results`` yields one entry (an ``RrcResult`` or an
    ``AllocationUnpricedError``) per position whose model is not ``None``, in
    ``positions`` order. An unpriceable allocation (share-data or price-data gap)
    degrades just that allocation to unpriced (logged so a persistent gap stays
    visible) instead of failing the whole prime; every other error already
    propagated out of the gather.

    Returns ``(exposure, modeled_exposure, required, per_allocation)``.
    """
    exposure = Decimal("0")
    modeled_exposure = Decimal("0")
    required = Decimal("0")
    per_allocation: list[AllocationRiskCapital] = []

    for position, model in zip(positions, models):
        position_exposure = position.amount_usd or Decimal("0")
        exposure += position_exposure

        if model is None:
            per_allocation.append(_unpriced_allocation(position, position_exposure, _UNPRICED_NO_MODEL))
            continue

        result = next(results)
        if isinstance(result, AllocationUnpricedError):
            logger.warning(
                "prime risk-capital: allocation receipt_token_id=%s unpriced (%s): %s",
                position.receipt_token_id,
                result.code,
                result,
            )
            per_allocation.append(_unpriced_allocation(position, position_exposure, result.code))
            continue

        required += result.rrc_usd
        modeled_exposure += position_exposure
        per_allocation.append(_priced_allocation(position, position_exposure, result))

    return exposure, modeled_exposure, required, per_allocation


def _chain_row(proxy_address: str, chain: str | None, totals: "_ProxyTotals | None") -> ChainRiskCapital:
    """One ``prime_per_chain`` row, ``null`` throughout when the chain went unqueried."""
    if totals is None:
        return ChainRiskCapital(
            proxy_address=proxy_address,
            chain=chain,
            exposure_usd=None,
            required_risk_capital_usd=None,
            allocation_count=None,
        )
    return ChainRiskCapital(
        proxy_address=proxy_address,
        chain=chain,
        exposure_usd=totals.exposure,
        required_risk_capital_usd=totals.required,
        allocation_count=len(totals.per_allocation),
    )


@dataclass(frozen=True)
class _ProxyTotals:
    """One proxy's contribution, before folding into the prime-level result."""

    proxy_address: str
    chain: str | None
    exposure: Decimal
    modeled_exposure: Decimal
    required: Decimal
    per_allocation: list[AllocationRiskCapital]


class PrimeRiskCapitalService:
    def __init__(self, repository: AllocationRepositoryPort, registry: ModelRegistry) -> None:
        self._repository = repository
        self._registry = registry

    async def prime_exists(self, prime_id: EthAddress) -> bool:
        return await self._repository.prime_exists(prime_id)

    async def compute(self, prime_id: EthAddress) -> PrimeRiskCapital:
        """Compute the queried proxy's figures plus the prime-wide aggregates.

        The unprefixed fields stay scoped to ``prime_id`` so existing consumers
        see unchanged numbers. The ``prime_*`` fields sum the numerator across the
        prime's ALM proxies on served chains so they match
        ``total_risk_capital_usd``, which is prime-wide and read once.
        """
        prime_name, proxies = self._proxies_to_aggregate(prime_id)

        per_proxy, total_rc = await asyncio.gather(
            asyncio.gather(
                *(self._compute_for_proxy(proxy, chain) for proxy, chain in self._proxies_to_query(prime_id, proxies))
            ),
            self._repository.get_latest_total_capital_usd(prime_id),
        )

        return self._assemble_result(prime_id, prime_name, proxies, per_proxy, total_rc)

    @staticmethod
    def _proxies_to_query(
        prime_id: EthAddress, proxies: tuple[tuple[EthAddress, str | None], ...]
    ) -> tuple[tuple[EthAddress, str | None], ...]:
        """Narrow the prime's proxies to the ones a query can answer for.

        A proxy on a chain no allocation tracker serves has no
        ``allocation_position`` rows at all, so computing it spends a connection
        per proxy to learn nothing and returns zeros that read as a genuine zero.
        Skipping it is what lets ``prime_per_chain`` report ``null`` for that chain
        instead. The queried proxy is always included: the unprefixed fields are
        its own, and ``prime_exists`` has already established it has rows.
        """
        queried = str(prime_id).lower()
        return tuple(
            (proxy, chain) for proxy, chain in proxies if chain_is_served(chain) or str(proxy).lower() == queried
        )

    async def _compute_for_proxy(self, proxy_address: EthAddress, chain: str | None) -> _ProxyTotals:
        """Run the per-allocation model pipeline over one ALM proxy's positions."""
        positions = await self._repository.list_receipt_token_positions(proxy_address)

        # Positions on a chain declared unserved mean the declaration is stale: a
        # tracker is writing rows for a chain SERVED_TRACKER_CHAINS says nothing
        # indexes, so every sibling on that chain is being skipped and reported as
        # null. Only reachable for the queried proxy, which is computed regardless.
        # A proxy absent from the contract has no chain to be stale about.
        if positions and chain is not None and not chain_is_served(chain):
            logger.warning(
                "prime risk-capital: proxy %s has %d positions on unserved chain %s; "
                "SERVED_TRACKER_CHAINS is stale against the deployed trackers",
                proxy_address,
                len(positions),
                chain,
            )

        # A zero-balance position contributes no required risk capital, so skip
        # its model compute entirely (each compute is several DB round trips).
        models = [
            self._default_model_for(position.receipt_token_id, proxy_address)
            if (position.amount_usd or Decimal("0")) > 0
            else None
            for position in positions
        ]

        # Pre-fetch every crypto-lending share AND backed breakdown up front and
        # pass them through ``compute_with_share``. Without this, each
        # per-allocation ``compute`` would hit ``get_share`` and the (expensive,
        # protocol-wide) breakdown query independently — a per-position fan-out.
        # Both are batched (the aave-like breakdown runs one query per protocol).
        # Non-crypto-lending models (none today; SURAF/CORE pending) fall through
        # to the unchanged ``model.compute`` path.
        prefetched_shares, prefetched_infos, prefetched_breakdowns = await self._prefetch_crypto_lending_inputs(
            positions, models, proxy_address
        )

        # Run the per-allocation model computes concurrently. Each compute is
        # still a DB round trip (liquidation params), so the gather keeps these
        # in flight in parallel.
        results = iter(
            await asyncio.gather(
                *(
                    self._dispatch_compute(
                        model,
                        position.receipt_token_id,
                        proxy_address,
                        prefetched_shares,
                        prefetched_infos,
                        prefetched_breakdowns,
                    )
                    for position, model in zip(positions, models)
                    if model is not None
                )
            )
        )

        exposure, modeled_exposure, required, per_allocation = _assemble_allocations(positions, models, results)
        return _ProxyTotals(
            # Lowercased so the prime-scoped lists built from these are byte-identical
            # whichever proxy was queried: EthAddress preserves the caller's casing,
            # while siblings come from the contract already lowercased. prime_id keeps
            # the caller's casing — it is proxy-scoped and documented as an echo.
            proxy_address=str(proxy_address).lower(),
            chain=chain,
            exposure=exposure,
            modeled_exposure=modeled_exposure,
            required=required,
            per_allocation=per_allocation,
        )

    @staticmethod
    def _proxies_to_aggregate(prime_id: EthAddress) -> tuple[str | None, tuple[tuple[EthAddress, str | None], ...]]:
        """Resolve the queried proxy to its prime's full set of ALM proxies.

        Returns ``(prime_name, ((proxy, chain), ...))`` with the queried proxy
        first. Order here is incidental to aggregation (``_assemble_result``
        locates the queried proxy's own totals by address, not by position) and
        is re-sorted by address before it reaches ``prime_proxies`` /
        ``prime_per_chain``, so those prime-scoped fields read identically
        whichever proxy was queried. A proxy absent from the axis-synome
        contract has no discoverable siblings and aggregates over itself alone.
        """
        prime_name = prime_name_for(prime_id)
        if prime_name is None:
            return None, ((prime_id, None),)

        siblings = [
            (EthAddress(entry.address), entry.chain)
            for entry in alm_proxies_for_prime(prime_name)
            if entry.address != str(prime_id).lower()
        ]
        queried_chain = next(
            (entry.chain for entry in alm_proxies_for_prime(prime_name) if entry.address == str(prime_id).lower()),
            None,
        )
        return prime_name, ((prime_id, queried_chain), *siblings)

    @staticmethod
    def _assemble_result(
        prime_id: EthAddress,
        prime_name: str | None,
        proxies: tuple[tuple[EthAddress, str | None], ...],
        per_proxy: list[_ProxyTotals],
        total_rc: Decimal | None,
    ) -> PrimeRiskCapital:
        """Build the response from the queried proxy's totals plus the prime-wide sums."""
        # Looked up by address, not read off index 0: per_proxy puts the queried
        # proxy first (see _proxies_to_aggregate), but prime_proxies/prime_per_chain
        # below are address-sorted, and the unprefixed fields must stay pinned to
        # the queried proxy regardless of where that sort places it.
        queried_address = str(prime_id).lower()
        computed = {totals.proxy_address.lower(): totals for totals in per_proxy}
        queried = computed[queried_address]

        # prime_ fields are reconciliation keys: identical from every proxy of a
        # prime, so a consumer can dedupe on them. Sorting by address (matching
        # alm_proxies_for_prime) makes prime_proxies/prime_per_chain identical
        # element-for-element too, not just as sets, regardless of which proxy
        # was queried. Built from the prime's whole proxy set rather than from the
        # computed subset, so an unserved chain is present-but-null instead of
        # missing — its absence would be indistinguishable from a prime that has no
        # proxy there at all.
        ordered = sorted(proxies, key=lambda entry: str(entry[0]).lower())

        prime_exposure = sum((totals.exposure for totals in per_proxy), Decimal("0"))
        prime_modeled = sum((totals.modeled_exposure for totals in per_proxy), Decimal("0"))
        prime_required = sum((totals.required for totals in per_proxy), Decimal("0"))

        return PrimeRiskCapital(
            proxy_address=str(prime_id),
            model=DEFAULT_RISK_MODEL,
            exposure_usd=queried.exposure,
            total_risk_capital_usd=total_rc,
            required_risk_capital_usd=queried.required,
            encumbrance_ratio=_ratio(queried.required, total_rc),
            modeled_exposure_usd=queried.modeled_exposure,
            modeled_pct=_ratio(queried.modeled_exposure, queried.exposure),
            per_allocation=queried.per_allocation,
            prime_name=prime_name,
            prime_exposure_usd=prime_exposure,
            prime_required_risk_capital_usd=prime_required,
            prime_modeled_exposure_usd=prime_modeled,
            prime_modeled_pct=_ratio(prime_modeled, prime_exposure),
            prime_encumbrance_ratio=_ratio(prime_required, total_rc),
            prime_proxies=tuple(str(proxy).lower() for proxy, _ in ordered),
            prime_per_chain=tuple(
                _chain_row(str(proxy).lower(), chain, computed.get(str(proxy).lower())) for proxy, chain in ordered
            ),
            prime_unserved_chains=tuple(
                sorted({chain for _, chain in ordered if chain is not None and not chain_is_served(chain)})
            ),
        )

    async def _prefetch_crypto_lending_inputs(
        self,
        positions,
        models,
        prime_id: EthAddress,
    ) -> tuple[dict[int, Decimal | Exception], dict[int, ReceiptTokenInfo], dict[int, BackedBreakdown]]:
        """Resolve shares, infos, and breakdowns for every crypto-lending position.

        Returns ``(shares, infos, breakdowns)`` keyed by ``receipt_token_id``.
        ``shares`` maps each asset to a resolved share or a stored share-lookup
        error (re-raised later by ``compute_with_share`` in the same place the
        un-batched path would have). ``infos`` carries the receipt-token records
        fetched to build the batches. ``breakdowns`` carries the backed breakdown
        per asset, resolved in one query per aave-like protocol. The per-allocation
        compute reuses all three instead of re-fetching them.
        """
        # All crypto-lending model instances share the same reader (constructed
        # once at startup), so the first one we see is enough to drive the
        # batch fetch.
        cl_model: CryptoLendingRiskService | None = None
        asset_ids: list[int] = []
        for position, model in zip(positions, models):
            if isinstance(model, CryptoLendingRiskService):
                if cl_model is None:
                    cl_model = model
                asset_ids.append(position.receipt_token_id)

        if cl_model is None or not asset_ids:
            return {}, {}, {}

        reader = cl_model.reader
        # Resolve receipt-token infos concurrently; this is the same per-asset
        # ``get_receipt_token`` ``compute`` would have done anyway, just hoisted
        # to feed the batch share lookup. (We accept this as a separate fan-out
        # for now — a future change can batch it too.)
        infos = await asyncio.gather(*(reader.get_receipt_token(aid) for aid in asset_ids))

        infos_by_id: dict[int, ReceiptTokenInfo] = {}
        for asset_id, info in zip(asset_ids, infos):
            if info is None:
                # A crypto-lending position whose receipt-token record is missing
                # is a data gap. We drop it from the prefetch batch here, but a
                # default model still applies to it (``applies_to`` keys off
                # ``supported_asset_ids``, not the record), so ``compute`` will
                # re-dispatch it to ``model.compute``, which raises ``ValueError``
                # → HTTP 500 for the whole prime. This is deliberate: a share-data
                # gap degrades to a 200 unpriced allocation, but a missing
                # receipt-token record fails hard rather than being silently
                # dropped. Logged so the gap is visible either way.
                logger.warning(
                    "prime risk-capital: no receipt-token record for asset_id=%s; excluding from prefetch", asset_id
                )
                continue
            infos_by_id[asset_id] = info
        if not infos_by_id:
            return {}, {}, {}

        valid_infos = list(infos_by_id.values())
        shares, breakdowns = await asyncio.gather(
            reader.batch_get_shares(valid_infos, prime_id),
            reader.batch_get_breakdowns(valid_infos),
        )
        return dict(shares), infos_by_id, dict(breakdowns)

    async def _dispatch_compute(
        self,
        model,
        asset_id: int,
        prime_id: EthAddress,
        prefetched_shares: dict[int, Decimal | Exception],
        prefetched_infos: dict[int, ReceiptTokenInfo],
        prefetched_breakdowns: dict[int, BackedBreakdown],
    ):
        """Run a model compute, plumbing the pre-fetched share, info, and breakdown.

        The share value (or share-lookup error) is handed to
        ``compute_with_share`` and only consumed *after* the empty-breakdown
        short-circuit inside the model. Assets with no backed-breakdown rows
        return zero items without surfacing the share-lookup error, matching
        the un-batched ``compute`` semantics where ``get_share`` was never
        called for empty breakdowns. The receipt-token ``info`` and backed
        ``breakdown`` fetched during prefetch are passed through so the model
        re-fetches neither.

        An unpriceable allocation (a share-data gap — warm-up window or un-indexed
        receipt token — or a price-data gap where the backed asset's loan token has
        no USD price) must not sink the whole prime, so it is returned as an
        ``AllocationUnpricedError`` value for the caller to render as an unpriced
        allocation. Any other error still propagates.
        """
        try:
            if isinstance(model, CryptoLendingRiskService) and asset_id in prefetched_shares:
                return await model.compute_with_share(
                    asset_id,
                    prime_id,
                    {},
                    prefetched_shares[asset_id],
                    info=prefetched_infos.get(asset_id),
                    breakdown_override=prefetched_breakdowns.get(asset_id),
                )
            return await model.compute(asset_id, prime_id, {})
        except AllocationUnpricedError as exc:
            return exc

    def _default_model_for(self, asset_id: int, prime_id: EthAddress):
        """Return the default RRC model if it applies to the asset, else None."""
        for model in self._registry.applicable(asset_id, prime_id):
            if model.risk_model == DEFAULT_RISK_MODEL:
                return model
        return None
