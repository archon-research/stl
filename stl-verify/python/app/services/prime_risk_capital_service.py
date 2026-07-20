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
from decimal import ROUND_HALF_EVEN, Decimal

from app.domain.entities.allocation import EthAddress
from app.domain.entities.backed_breakdown import BackedBreakdown
from app.domain.entities.prime_risk_capital import AllocationRiskCapital, PrimeRiskCapital, UnpricedReason
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.entities.risk import RrcResult
from app.domain.exceptions import AllocationShareError
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
# opposed to the ``AllocationShareError.code`` values used when a model applies
# but its share lookup fails).
_UNPRICED_NO_MODEL: UnpricedReason = "no_model"


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

    ``results`` yields one entry (an ``RrcResult`` or a share-lookup
    ``AllocationShareError``) per position whose model is not ``None``, in
    ``positions`` order. A share error degrades just that allocation to unpriced
    (logged so a persistent gap stays visible) instead of failing the whole
    prime; every other error already propagated out of the gather.

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
        if isinstance(result, AllocationShareError):
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


class PrimeRiskCapitalService:
    def __init__(self, repository: AllocationRepositoryPort, registry: ModelRegistry) -> None:
        self._repository = repository
        self._registry = registry

    async def prime_exists(self, prime_id: EthAddress) -> bool:
        return await self._repository.prime_exists(prime_id)

    async def compute(self, prime_id: EthAddress) -> PrimeRiskCapital:
        positions, total_rc = await asyncio.gather(
            self._repository.list_receipt_token_positions(prime_id),
            self._repository.get_latest_total_capital_usd(prime_id),
        )

        # A zero-balance position contributes no required risk capital, so skip
        # its model compute entirely (each compute is several DB round trips).
        models = [
            self._default_model_for(position.receipt_token_id, prime_id)
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
            positions, models, prime_id
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
                        prime_id,
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

        encumbrance_ratio = (
            (required / total_rc).quantize(_RATIO, rounding=ROUND_HALF_EVEN)
            if total_rc is not None and total_rc > 0
            else None
        )
        modeled_pct = (modeled_exposure / exposure).quantize(_RATIO, rounding=ROUND_HALF_EVEN) if exposure > 0 else None

        return PrimeRiskCapital(
            prime_id=str(prime_id),
            model=DEFAULT_RISK_MODEL,
            exposure_usd=exposure,
            total_risk_capital_usd=total_rc,
            required_risk_capital_usd=required,
            encumbrance_ratio=encumbrance_ratio,
            modeled_exposure_usd=modeled_exposure,
            modeled_pct=modeled_pct,
            per_allocation=per_allocation,
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

        A share-lookup failure for one allocation (a warm-up window, or an
        un-indexed receipt token) must not sink the whole prime, so it is
        returned as an ``AllocationShareError`` value for the caller to render as
        an unpriced allocation. Any other error still propagates.
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
        except AllocationShareError as exc:
            return exc

    def _default_model_for(self, asset_id: int, prime_id: EthAddress):
        """Return the default RRC model if it applies to the asset, else None."""
        for model in self._registry.applicable(asset_id, prime_id):
            if model.risk_model == DEFAULT_RISK_MODEL:
                return model
        return None
