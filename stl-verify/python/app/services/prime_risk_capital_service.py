"""Self-computed per-prime risk capital.

Composes three sources, with no dependency on the upstream Star feed:

- **exposure** and the allocation set: the prime's priced receipt-token positions.
- **total risk capital**: the on-chain SubProxy treasury (latest balance).
- **required risk capital**: the per-allocation RRC from the default model
  (``gap_sweep``), summed. Allocations the model cannot price contribute 0.

The result is model-derived and partial by design (see
``app/domain/entities/prime_risk_capital.py``).
"""

from decimal import ROUND_HALF_EVEN, Decimal

from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime_risk_capital import AllocationRiskCapital, PrimeRiskCapital
from app.ports.allocation_repository import AllocationRepositoryPort
from app.services.model_registry import ModelRegistry

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
        positions = await self._repository.list_receipt_token_positions(prime_id)
        total_rc = await self._repository.get_latest_total_capital_usd(prime_id)

        exposure = Decimal("0")
        modeled_exposure = Decimal("0")
        required = Decimal("0")
        per_allocation: list[AllocationRiskCapital] = []

        for position in positions:
            position_exposure = position.amount_usd or Decimal("0")
            exposure += position_exposure

            model = self._default_model_for(position.receipt_token_id, prime_id)
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

            result = await model.compute(position.receipt_token_id, prime_id, {})
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

    def _default_model_for(self, asset_id: int, prime_id: EthAddress):
        """Return the default RRC model if it applies to the asset, else None."""
        for model in self._registry.applicable(asset_id, prime_id):
            if model.risk_model == DEFAULT_RISK_MODEL:
                return model
        return None
