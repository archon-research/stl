"""Self-computed per-prime risk capital.

Composes three sources, with no dependency on the upstream Star feed:

- **exposure** and the allocation set: the prime's priced receipt-token positions.
- **total risk capital**: the on-chain SubProxy treasury (latest balance).
- **required risk capital**: the per-allocation RRC from the model this view
  prefers, summed. ``indexed`` prefers ``core_model``, falling back to
  ``gap_sweep`` where core has no data; ``both`` prefers ``core_model`` alone,
  since a null figure there still renders via the reference fallback. See
  ``_model_preference``. Allocations no preferred model can price contribute 0.

The result is model-derived and partial by design (see
``app/domain/entities/prime_risk_capital.py``).
"""

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal

from app.domain.chain_names import chain_name_for
from app.domain.entities.allocation import EthAddress
from app.domain.entities.backed_breakdown import BackedBreakdown
from app.domain.entities.prime import PrimeScope, ProxyWallet
from app.domain.entities.prime_risk_capital import (
    AllocationRiskCapital,
    ChainRiskCapital,
    PrimeRiskCapital,
    UnpricedReason,
)
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.entities.risk import ModelName, RrcResult
from app.domain.exceptions import AllocationUnpricedError, ModelDataUnavailableError
from app.domain.provenance import Provenance
from app.logging import get_logger
from app.ports.allocation_repository import AllocationRepositoryPort
from app.ports.risk_model import RiskModel
from app.services.crypto_lending_risk_service import CryptoLendingRiskService
from app.services.model_registry import ModelRegistry

logger = get_logger(__name__)

_RATIO = Decimal("0.0001")  # ratios/percentages to 4 dp

# ``unpriced_reason`` value for an allocation no default model applies to (as
# opposed to the ``AllocationUnpricedError.code`` values used when a model applies
# but the allocation cannot be priced — a share-data or price-data gap).
_UNPRICED_NO_MODEL: UnpricedReason = "no_model"


def _model_preference(source: Provenance) -> tuple[ModelName, ...]:
    """Discriminators to try, most preferred first, for the requested provenance.

    The composite view (``both``) never falls back to ``gap_sweep``: a position
    core_model can't price there is left with a null model figure rather than a
    gap_sweep one, so the UI's ``preferReference`` renders Sky's reference
    figure in its place instead of a figure the composite view isn't meant to
    show. ``indexed`` has no reference figure to fall back to, so it keeps
    gap_sweep as a second choice.
    """
    if source is Provenance.BOTH:
        return ("core_model",)
    return ("core_model", "gap_sweep")


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


def _assemble_allocations(
    positions, model_chains, results
) -> tuple[Decimal, Decimal, Decimal, list[AllocationRiskCapital]]:
    """Fold per-allocation compute results into totals + a per-allocation list.

    ``model_chains`` carries, per position, the preferred models still worth
    trying (see ``_model_preference``) — empty when none applies at all.
    ``results`` yields one entry (an ``RrcResult``, an ``AllocationUnpricedError``,
    or a ``ModelDataUnavailableError`` when every candidate in the chain had no
    data) per position with a non-empty chain, in ``positions`` order. Either
    error degrades just that allocation to unpriced (logged so a persistent gap
    stays visible) instead of failing the whole prime; every other error already
    propagated out of the gather.

    Returns ``(exposure, modeled_exposure, required, per_allocation)``.
    """
    exposure = Decimal("0")
    modeled_exposure = Decimal("0")
    required = Decimal("0")
    per_allocation: list[AllocationRiskCapital] = []

    for position, chain in zip(positions, model_chains):
        position_exposure = position.amount_usd or Decimal("0")
        exposure += position_exposure

        if not chain:
            per_allocation.append(_unpriced_allocation(position, position_exposure, _UNPRICED_NO_MODEL))
            continue

        result = next(results)
        if isinstance(result, ModelDataUnavailableError):
            logger.warning(
                "prime risk-capital: allocation receipt_token_id=%s unpriced; no preferred model had data (%s)",
                position.receipt_token_id,
                result,
            )
            per_allocation.append(_unpriced_allocation(position, position_exposure, _UNPRICED_NO_MODEL))
            continue
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


def _chain_of(wallet: ProxyWallet) -> str:
    """The wallet's internal chain name, or a loud failure.

    ``prime_proxy.chain_id`` references the ``chain`` registry, and
    ``CHAIN_ID_TO_NAME`` is held in lockstep with the Go tracker's vocabulary by
    ``test_chain_names.py``. A miss means a chain was onboarded without teaching
    this side its name, which would silently drop a wallet's exposure out of the
    per-chain audit while leaving it in the total.
    """
    chain = chain_name_for(wallet.chain_id)
    if chain is None:
        raise ValueError(
            f"prime_proxy lists wallet {wallet.address} on chain id {wallet.chain_id}, "
            f"which app.domain.chain_names has no name for"
        )
    return chain


def _chain_row(chain: str, contributions: "Sequence[_ProxyTotals]") -> ChainRiskCapital:
    """One ``prime_per_chain`` row, summed over the prime's wallets on that chain.

    No contributions means the chain went unqueried — it is served by no
    tracker — so every figure is ``null`` rather than a zero that would claim
    the prime holds nothing there.
    """
    if not contributions:
        return ChainRiskCapital(chain=chain, exposure_usd=None, required_risk_capital_usd=None, allocation_count=None)
    return ChainRiskCapital(
        chain=chain,
        exposure_usd=sum((totals.exposure for totals in contributions), Decimal("0")),
        required_risk_capital_usd=sum((totals.required for totals in contributions), Decimal("0")),
        allocation_count=sum(len(totals.per_allocation) for totals in contributions),
    )


@dataclass(frozen=True)
class _ProxyTotals:
    """One wallet's contribution, before folding into the prime-level result."""

    chain: str
    exposure: Decimal
    modeled_exposure: Decimal
    required: Decimal
    per_allocation: list[AllocationRiskCapital]


class PrimeRiskCapitalService:
    def __init__(self, repository: AllocationRepositoryPort, registry: ModelRegistry) -> None:
        self._repository = repository
        self._registry = registry

    async def compute(self, scope: PrimeScope, source: Provenance = Provenance.INDEXED) -> PrimeRiskCapital:
        """Compute the whole prime's figures over its resolved wallet set.

        Exposure, modeled exposure and Required Risk Capital are ADDITIVE, so
        they sum across the prime's ALM proxies; Total Risk Capital is SHARED
        and is read once over its SubProxy treasury wallets (see
        ``PrimeScope``). Nothing is scoped to one proxy, so the answer is
        identical whichever identifier named the prime.

        ``source`` is the caller's resolved provenance (``indexed`` or ``both``
        — never ``reference``, which runs no model and never reaches this
        method) and picks the model preference order via ``_model_preference``.
        """
        per_proxy, total_rc = await asyncio.gather(
            asyncio.gather(*(self._compute_for_proxy(wallet, source) for wallet in scope.alm_wallets)),
            self._repository.get_latest_total_capital_usd(scope.subproxies),
        )

        return self._assemble_result(scope, per_proxy, total_rc, source)

    async def _compute_for_proxy(self, wallet: ProxyWallet, source: Provenance) -> _ProxyTotals:
        """Run the per-allocation model pipeline over one ALM proxy's positions."""
        proxy_address = wallet.address
        chain = _chain_of(wallet)
        positions = await self._repository.list_receipt_token_positions(proxy_address)

        # A zero-balance position contributes no required risk capital, so skip
        # its model compute entirely (each compute is several DB round trips).
        # Each chain is the position's applicable models in preference order —
        # empty when none applies — and dispatch tries them in order, falling
        # through a ``ModelDataUnavailableError`` to the next one.
        model_chains = [
            self._candidate_models_for(position.receipt_token_id, proxy_address, source)
            if (position.amount_usd or Decimal("0")) > 0
            else ()
            for position in positions
        ]
        # The batch prefetch below only covers each position's *first*-preference
        # model: that is the one every position resolves to unless it falls back
        # at dispatch time, which is the rare, data-gap path.
        primary_models = [chain[0] if chain else None for chain in model_chains]

        # Pre-fetch every crypto-lending share AND backed breakdown up front and
        # pass them through ``compute_with_share``. Without this, each
        # per-allocation ``compute`` would hit ``get_share`` and the (expensive,
        # protocol-wide) breakdown query independently — a per-position fan-out.
        # Both are batched (the aave-like breakdown runs one query per protocol).
        # Non-crypto-lending models (SURAF, CORE) fall through to the unchanged
        # ``model.compute`` path.
        prefetched_shares, prefetched_infos, prefetched_breakdowns = await self._prefetch_crypto_lending_inputs(
            positions, primary_models, proxy_address
        )

        # Run the per-allocation model computes concurrently. Each compute is
        # still a DB round trip (liquidation params), so the gather keeps these
        # in flight in parallel.
        results = iter(
            await asyncio.gather(
                *(
                    self._dispatch_compute(
                        model_chain,
                        position.receipt_token_id,
                        proxy_address,
                        prefetched_shares,
                        prefetched_infos,
                        prefetched_breakdowns,
                    )
                    for position, model_chain in zip(positions, model_chains)
                    if model_chain
                )
            )
        )

        exposure, modeled_exposure, required, per_allocation = _assemble_allocations(positions, model_chains, results)
        return _ProxyTotals(
            chain=chain,
            exposure=exposure,
            modeled_exposure=modeled_exposure,
            required=required,
            per_allocation=per_allocation,
        )

    @staticmethod
    def _assemble_result(
        scope: PrimeScope,
        per_proxy: list[_ProxyTotals],
        total_rc: Decimal | None,
        source: Provenance,
    ) -> PrimeRiskCapital:
        """Fold the per-wallet contributions into the prime's figures.

        ``model`` reports the top of this view's preference order (always
        ``core_model``), not a per-position tally — individual allocations can
        still resolve to a less-preferred model via ``per_allocation[*].model``,
        the same way the field always named ``gap_sweep`` before there was a
        second model to prefer.
        """
        by_chain: dict[str, list[_ProxyTotals]] = {}
        for totals in per_proxy:
            by_chain.setdefault(totals.chain, []).append(totals)
        # An unserved chain is present-but-null rather than missing: its absence
        # would be indistinguishable from a prime that has no proxy there at all.
        for chain in scope.unserved_chains:
            by_chain.setdefault(chain, [])

        exposure = sum((totals.exposure for totals in per_proxy), Decimal("0"))
        modeled = sum((totals.modeled_exposure for totals in per_proxy), Decimal("0"))
        required = sum((totals.required for totals in per_proxy), Decimal("0"))

        return PrimeRiskCapital(
            model=_model_preference(source)[0],
            exposure_usd=exposure,
            total_risk_capital_usd=total_rc,
            required_risk_capital_usd=required,
            encumbrance_ratio=_ratio(required, total_rc),
            modeled_exposure_usd=modeled,
            modeled_pct=_ratio(modeled, exposure),
            per_allocation=sorted(
                (allocation for totals in per_proxy for allocation in totals.per_allocation),
                key=lambda allocation: allocation.exposure_usd,
                reverse=True,
            ),
            prime_name=scope.identity.name,
            prime_per_chain=tuple(_chain_row(chain, by_chain[chain]) for chain in sorted(by_chain)),
            prime_unserved_chains=scope.unserved_chains,
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
        model_chain: tuple[RiskModel, ...],
        asset_id: int,
        prime_id: EthAddress,
        prefetched_shares: dict[int, Decimal | Exception],
        prefetched_infos: dict[int, ReceiptTokenInfo],
        prefetched_breakdowns: dict[int, BackedBreakdown],
    ):
        """Try each model in ``model_chain``, in order, plumbing the pre-fetched
        share/info/breakdown through the first (only that one was prefetched).

        The share value (or share-lookup error) is handed to
        ``compute_with_share`` and only consumed *after* the empty-breakdown
        short-circuit inside the model. Assets with no backed-breakdown rows
        return zero items without surfacing the share-lookup error, matching
        the un-batched ``compute`` semantics where ``get_share`` was never
        called for empty breakdowns. The receipt-token ``info`` and backed
        ``breakdown`` fetched during prefetch are passed through so the model
        re-fetches neither.

        Only ``ModelDataUnavailableError`` (the model applies but has nothing to
        compute from — the same signal ``/v1/risk/rrc``'s envelope already
        treats as a skip) advances to the next candidate. Any other error is
        specific to the model that raised it, not a reason to try a fallback:
        an unpriceable allocation (a share-data gap — warm-up window or
        un-indexed receipt token — or a price-data gap where the backed
        asset's loan token has no USD price) is returned as an
        ``AllocationUnpricedError`` value for the caller to render as unpriced,
        and any other error still propagates.
        """
        unavailable = ModelDataUnavailableError(f"no preferred model had data for asset_id={asset_id}")
        for model in model_chain:
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
            except ModelDataUnavailableError as exc:
                unavailable = exc
                continue
        return unavailable

    def _candidate_models_for(self, asset_id: int, prime_id: EthAddress, source: Provenance) -> tuple[RiskModel, ...]:
        """Applicable models for this asset, ordered by ``source``'s preference.

        Structural non-applicability (a model's own ``applies_to``) is filtered
        here; a preferred model that applies but has no data yet is handled at
        dispatch time via ``ModelDataUnavailableError``, not here.
        """
        applicable_by_name = {model.risk_model: model for model in self._registry.applicable(asset_id, prime_id)}
        return tuple(applicable_by_name[name] for name in _model_preference(source) if name in applicable_by_name)
