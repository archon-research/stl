from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.api._validators import ProxyAddressPathParam
from app.api.deps import get_engine, get_model_registry
from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime_risk_capital import UnpricedReason
from app.services.model_registry import ModelRegistry
from app.services.prime_risk_capital_service import PrimeRiskCapitalService

router = APIRouter(tags=["primes", "capital"])


class AllocationRiskCapitalResponse(BaseModel):
    """Per-allocation risk capital from the default model."""

    receipt_token_id: int = Field(description="Surrogate id of the receipt token.")
    symbol: str = Field(description="Receipt-token symbol.")
    protocol_name: str = Field(description="Protocol the allocation sits in.")
    exposure_usd: Decimal = Field(description="On-chain USD exposure of the allocation.")
    applied: bool = Field(description="Whether the default model priced this allocation.")
    required_risk_capital_usd: Decimal | None = Field(
        default=None, description="Per-allocation RRC (USD). `null` when the allocation is unpriced."
    )
    crr_pct: Decimal | None = Field(
        default=None, description="Comparable capital-risk ratio (0-100). `null` when the allocation is unpriced."
    )
    model: str | None = Field(default=None, description="Model that produced the figure, or `null`.")
    unpriced_reason: UnpricedReason | None = Field(
        default=None,
        description=(
            "Why the allocation is unpriced (`null` when `applied`): `no_model` (no default model applies), "
            "or `share_data_missing` / `share_data_stale` (a model applies but its pool-share lookup could "
            "not be resolved, e.g. a warm-up window or an un-indexed receipt token)."
        ),
    )


class ChainRiskCapitalResponse(BaseModel):
    """One ALM proxy's contribution to the prime's aggregated figures."""

    proxy_address: str = Field(description="0x-prefixed ALM proxy address.")
    chain: str | None = Field(
        default=None, description="Internal chain name. `null` for an unrecognised chain id.", examples=["avalanche-c"]
    )
    exposure_usd: Decimal = Field(description="Priced receipt-token exposure held through this proxy (USD).")
    required_risk_capital_usd: Decimal = Field(description="Required Risk Capital from this proxy's positions (USD).")
    allocation_count: int = Field(description="Number of allocations this proxy contributed.")


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
        default=None,
        deprecated=True,
        description=(
            "DEPRECATED — divides this proxy's Required Risk Capital by the whole prime's Total "
            "Risk Capital, mixing scopes, so the figure is not meaningful for either. Its value "
            "is unchanged for backwards compatibility. Use `prime_encumbrance_ratio`."
        ),
    )
    modeled_exposure_usd: Decimal = Field(description="Exposure the default model could price (USD).")
    modeled_pct: Decimal | None = Field(
        default=None, description="`modeled_exposure_usd / exposure_usd` (0-1). `null` when exposure is zero."
    )
    per_allocation: list[AllocationRiskCapitalResponse] = Field(
        description="Per-allocation breakdown, newest-exposure first."
    )
    prime_name: str | None = Field(
        default=None,
        description="Prime this proxy belongs to. `null` for a proxy absent from the axis-synome contract.",
        examples=["spark"],
    )
    prime_exposure_usd: Decimal = Field(
        default=Decimal("0"),
        description="Σ priced exposure across every ALM proxy of the prime (USD). Prime-scoped: dedupe, never sum.",
    )
    prime_required_risk_capital_usd: Decimal = Field(
        default=Decimal("0"),
        description="Σ Required Risk Capital across every ALM proxy of the prime (USD). Prime-scoped.",
    )
    prime_modeled_exposure_usd: Decimal = Field(
        default=Decimal("0"), description="Σ exposure the default model could price, prime-wide (USD)."
    )
    prime_modeled_pct: Decimal | None = Field(
        default=None, description="`prime_modeled_exposure_usd / prime_exposure_usd` (0-1)."
    )
    prime_encumbrance_ratio: Decimal | None = Field(
        default=None,
        description=(
            "`prime_required_risk_capital_usd / total_risk_capital_usd` — the prime's true "
            "encumbrance. Both sides are prime-scoped, so this is identical whichever of the "
            "prime's proxies is queried. `null` when Total Risk Capital is absent or zero."
        ),
        examples=["0.9397"],
    )
    proxies: list[str] = Field(
        default_factory=list, description="ALM proxy addresses the `prime_*` figures were aggregated over."
    )
    per_chain: list[ChainRiskCapitalResponse] = Field(
        default_factory=list, description="Per-proxy breakdown of the aggregated numerator, so the sum is auditable."
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
        "A backed allocation whose pool-share lookup can't be resolved (e.g. a warm-up window or an "
        "un-indexed receipt token) is reported as unpriced (`applied=false` with an `unpriced_reason`) "
        "rather than failing the whole response. Returns `404` if the prime is unknown.\n\n"
        "Figures without a prefix are scoped to the proxy in the path and are unchanged from previous "
        "releases. Figures prefixed `prime_` are scoped to the whole prime — summed across every ALM "
        "proxy of the prime the given address belongs to — and are therefore identical whichever proxy "
        "you query; use `per_chain` for the split. `total_risk_capital_usd` is prime-wide despite having "
        "no prefix. `encumbrance_ratio` is deprecated because it mixes the two scopes; use "
        "`prime_encumbrance_ratio`."
    ),
)
async def get_prime_risk_capital(
    prime_id: ProxyAddressPathParam,
    service: PrimeRiskCapitalService = Depends(_get_service),
) -> PrimeRiskCapitalResponse:
    prime_address = EthAddress(prime_id)
    if not await service.prime_exists(prime_address):
        raise HTTPException(status_code=404, detail="Prime not found")

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
        prime_name=result.prime_name,
        prime_exposure_usd=result.prime_exposure_usd,
        prime_required_risk_capital_usd=result.prime_required_risk_capital_usd,
        prime_modeled_exposure_usd=result.prime_modeled_exposure_usd,
        prime_modeled_pct=result.prime_modeled_pct,
        prime_encumbrance_ratio=result.prime_encumbrance_ratio,
        proxies=list(result.proxies),
        per_chain=[ChainRiskCapitalResponse(**row.__dict__) for row in result.per_chain],
    )
