from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.api._validators import EthAddressParam
from app.api.deps import get_engine, get_model_registry
from app.config import Settings, get_settings
from app.domain.entities.allocation import EthAddress
from app.services.model_registry import ModelRegistry
from app.services.prime_risk_capital_service import DEFAULT_RISK_MODEL, PrimeRiskCapitalService

router = APIRouter(tags=["primes", "capital"])


class AllocationRiskCapitalResponse(BaseModel):
    """Per-allocation risk capital from the default model."""

    receipt_token_id: int = Field(description="Surrogate id of the receipt token.")
    symbol: str = Field(description="Receipt-token symbol.")
    protocol_name: str = Field(description="Protocol the allocation sits in.")
    exposure_usd: Decimal = Field(description="On-chain USD exposure of the allocation.")
    applied: bool = Field(description="Whether the default model could price this allocation.")
    required_risk_capital_usd: Decimal | None = Field(
        default=None, description="Per-allocation RRC (USD). `null` when the model does not apply."
    )
    crr_pct: Decimal | None = Field(
        default=None, description="Comparable capital-risk ratio (0-100). `null` when the model does not apply."
    )
    model: str | None = Field(default=None, description="Model that produced the figure, or `null`.")


class PrimeRiskCapitalResponse(BaseModel):
    """Self-computed, model-derived capital metrics for a prime.

    Independent of the upstream Star feed. `required_risk_capital_usd` is the
    sum of per-allocation RRC from the default model (`model`); it is **partial**
    (only allocations the model can price contribute) and **will not** match
    Sky's dashboard. `modeled_pct` reports the priced share of exposure.
    """

    prime_id: str = Field(description="Prime's 0x-prefixed ALM proxy address.")
    model: str = Field(description="Default RRC model used (e.g. `gap_sweep`).", examples=["gap_sweep"])
    exposure_usd: Decimal = Field(description="Σ priced receipt-token allocation exposure (USD).")
    total_risk_capital_usd: Decimal | None = Field(
        default=None, description="On-chain SubProxy treasury balance (USD). `null` when absent."
    )
    required_risk_capital_usd: Decimal = Field(description="Σ per-allocation RRC from the default model (USD).")
    encumbrance_ratio: Decimal | None = Field(
        default=None, description="Required / Total Risk Capital. `null` when total is absent or zero."
    )
    modeled_exposure_usd: Decimal = Field(description="Exposure the default model could price (USD).")
    modeled_pct: Decimal | None = Field(
        default=None, description="`modeled_exposure_usd / exposure_usd` (0-1). `null` when exposure is zero."
    )
    per_allocation: list[AllocationRiskCapitalResponse] = Field(
        description="Per-allocation breakdown, newest-exposure first."
    )


async def _get_service(
    engine: AsyncEngine = Depends(get_engine),
    registry: ModelRegistry = Depends(get_model_registry),
) -> PrimeRiskCapitalService:
    return PrimeRiskCapitalService(AllocationRepository(engine), registry)


@router.get(
    "/primes/{prime_id}/risk-capital",
    response_model=PrimeRiskCapitalResponse,
    tags=["primes", "capital"],
    summary="Self-computed prime risk capital",
    description=(
        "Compute the prime's capital metrics from on-chain data and the default RRC model "
        "(`gap_sweep`), with no dependency on the upstream Star feed. Returns exposure (priced "
        "receipt-token allocations), Total Risk Capital (on-chain treasury), Required Risk Capital "
        "(sum of per-allocation model RRC), encumbrance, a `modeled_pct` coverage figure, and a "
        "per-allocation breakdown. The figures are model-derived and partial (only allocations the "
        "model can price contribute Required Risk Capital) and will not match Sky's dashboard. "
        "Returns `404` if the prime is unknown."
    ),
)
async def get_prime_risk_capital(
    prime_id: EthAddressParam,
    service: PrimeRiskCapitalService = Depends(_get_service),
    settings: Settings = Depends(get_settings),
) -> PrimeRiskCapitalResponse:
    prime_address = EthAddress(prime_id)
    if not await service.prime_exists(prime_address):
        raise HTTPException(status_code=404, detail="Prime not found")

    if settings.risk_capital_endpoint_enabled:
        result = await service.compute(prime_address)
        return PrimeRiskCapitalResponse(
            prime_id=result.prime_id,
            model=result.model,
            exposure_usd=result.exposure_usd,
            total_risk_capital_usd=result.total_risk_capital_usd,
            required_risk_capital_usd=result.required_risk_capital_usd,
            encumbrance_ratio=result.encumbrance_ratio,
            modeled_exposure_usd=result.modeled_exposure_usd,
            modeled_pct=result.modeled_pct,
            per_allocation=[AllocationRiskCapitalResponse(**alloc.__dict__) for alloc in result.per_allocation],
        )

    # Disabled by default: the per-allocation RRC fan-out bulk-decompresses the
    # allocation_position columnstore and OOMs the DB backend (500s). Return an
    # empty/zero envelope so the UI renders empty metrics instead of erroring.
    # Flip RISK_CAPITAL_ENDPOINT_ENABLED=true to re-enable once the query is cheap.
    return PrimeRiskCapitalResponse(
        prime_id=str(prime_address),
        model=DEFAULT_RISK_MODEL,
        exposure_usd=Decimal("0"),
        total_risk_capital_usd=None,
        required_risk_capital_usd=Decimal("0"),
        encumbrance_ratio=None,
        modeled_exposure_usd=Decimal("0"),
        modeled_pct=None,
        per_allocation=[],
    )
