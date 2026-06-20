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
from app.domain.entities.prime_risk_capital import AllocationRiskCapital, PrimeRiskCapital
from app.logging import get_logger
from app.ports.allocation_repository import AllocationRepositoryPort
from app.services.crypto_lending_risk_service import CryptoLendingRiskService
from app.services.model_registry import ModelRegistry

logger = get_logger(__name__)

# The default RRC model. SURAF is under development and CORE is not yet landed,
# so gap_sweep (on-chain crypto-lending stress) is the reliable default today.
DEFAULT_RISK_MODEL = "gap_sweep"

_RATIO = Decimal("0.0001")  # ratios/percentages to 4 dp


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

        # Pre-fetch every crypto-lending share in a single round-trip and pass
        # the result through ``compute_with_share``. Without this, each
        # per-allocation ``compute`` would hit ``get_share`` independently:
        # a fan-out of one ``allocation_position`` query per position.
        # Non-crypto-lending models (none today; SURAF/CORE pending) fall
        # through to the unchanged ``model.compute`` path.
        prefetched_shares = await self._prefetch_crypto_lending_shares(positions, models, prime_id)

        # Run the per-allocation model computes concurrently. Each compute is
        # still several DB round trips (breakdown + liquidation params), so
        # the gather keeps these in flight in parallel.
        results = iter(
            await asyncio.gather(
                *(
                    self._dispatch_compute(model, position.receipt_token_id, prime_id, prefetched_shares)
                    for position, model in zip(positions, models)
                    if model is not None
                )
            )
        )

        exposure = Decimal("0")
        modeled_exposure = Decimal("0")
        required = Decimal("0")
        per_allocation: list[AllocationRiskCapital] = []

        for position, model in zip(positions, models):
            position_exposure = position.amount_usd or Decimal("0")
            exposure += position_exposure

            if model is None:
                per_allocation.append(
                    AllocationRiskCapital(
                        receipt_token_id=position.receipt_token_id,
                        symbol=position.symbol,
                        protocol_name=position.protocol_name,
                        exposure_usd=position_exposure,
                        applied=False,
                        required_risk_capital_usd=None,
                        crr_pct=None,
                        model=None,
                    )
                )
                continue

            result = next(results)
            required += result.rrc_usd
            modeled_exposure += position_exposure
            per_allocation.append(
                AllocationRiskCapital(
                    receipt_token_id=position.receipt_token_id,
                    symbol=position.symbol,
                    protocol_name=position.protocol_name,
                    exposure_usd=position_exposure,
                    applied=True,
                    required_risk_capital_usd=result.rrc_usd,
                    crr_pct=result.comparable_crr_pct,
                    model=result.risk_model,
                )
            )

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

    async def _prefetch_crypto_lending_shares(
        self,
        positions,
        models,
        prime_id: EthAddress,
    ) -> dict[int, Decimal | Exception]:
        """Resolve shares for every crypto-lending position in one round-trip.

        Returns ``{receipt_token_id: share-or-error}``. Errors are stored as
        values (not raised) so the per-position ``compute_with_share`` call
        re-raises them in the same place the un-batched path would have.
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
            return {}

        reader = cl_model.reader
        # Resolve receipt-token infos concurrently; this is the same per-asset
        # ``get_receipt_token`` ``compute`` would have done anyway, just hoisted
        # to feed the batch share lookup. (We accept this as a separate fan-out
        # for now — a future change can batch it too.)
        infos = await asyncio.gather(*(reader.get_receipt_token(aid) for aid in asset_ids))

        valid_infos = [info for info in infos if info is not None]
        if not valid_infos:
            return {}

        shares = await reader.batch_get_shares(valid_infos, prime_id)
        return dict(shares)

    async def _dispatch_compute(
        self,
        model,
        asset_id: int,
        prime_id: EthAddress,
        prefetched_shares: dict[int, Decimal | Exception],
    ):
        """Run a model compute, plumbing a pre-fetched share when available.

        The share value (or share-lookup error) is handed to
        ``compute_with_share`` and only consumed *after* the empty-breakdown
        short-circuit inside the model. Assets with no backed-breakdown rows
        return zero items without surfacing the share-lookup error, matching
        the un-batched ``compute`` semantics where ``get_share`` was never
        called for empty breakdowns.
        """
        if isinstance(model, CryptoLendingRiskService) and asset_id in prefetched_shares:
            return await model.compute_with_share(asset_id, prime_id, {}, prefetched_shares[asset_id])
        return await model.compute(asset_id, prime_id, {})

    def _default_model_for(self, asset_id: int, prime_id: EthAddress):
        """Return the default RRC model if it applies to the asset, else None."""
        for model in self._registry.applicable(asset_id, prime_id):
            if model.risk_model == DEFAULT_RISK_MODEL:
                return model
        return None
