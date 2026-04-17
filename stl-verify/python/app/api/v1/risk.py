from collections.abc import AsyncGenerator
from decimal import Decimal

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine

from app.api.deps import get_asset_to_rating, get_engine, get_http_client, get_suraf_ratings
from app.config import Settings, get_settings
from app.risk_engine.rrc import compute_rrc
from app.risk_engine.suraf.result import SurafResult
from app.services.risk_calculation_service import RiskCalculationService
from app.services.risk_service_factory import RiskServiceFactory

router = APIRouter()

_ZERO = Decimal("0")
_ONE = Decimal("1")


class BadDebtResponse(BaseModel):
    receipt_token_id: int
    gap_pct: Decimal
    bad_debt_usd: Decimal


class RiskBreakdownItemResponse(BaseModel):
    token_id: int
    symbol: str
    amount: Decimal
    backing_pct: Decimal
    amount_usd: Decimal
    price_usd: Decimal
    liquidation_threshold: Decimal
    liquidation_bonus: Decimal


class RiskBreakdownResponse(BaseModel):
    receipt_token_id: int
    items: list[RiskBreakdownItemResponse]


class RrcRequest(BaseModel):
    asset: str
    usd_exposure: Decimal


class RrcResponse(BaseModel):
    asset: str
    usd_exposure: Decimal
    rating_id: str
    crr_pct: Decimal
    rrc_usd: Decimal
    source_commit_sha: str


async def _resolve(
    receipt_token_id: int,
    engine: AsyncEngine = Depends(get_engine),
    settings: Settings = Depends(get_settings),
    http_client: httpx.AsyncClient = Depends(get_http_client),
) -> AsyncGenerator[tuple[RiskCalculationService, int], None]:
    """Resolve receipt token into a service + backed_asset_id, or raise HTTP errors.

    Uses ``async with`` + ``yield`` so the DB connection is properly cleaned up.
    """
    alchemy_url = settings.alchemy_http_url
    factory = RiskServiceFactory(engine, alchemy_url=alchemy_url, http_client=http_client)
    try:
        result = await factory.create(receipt_token_id)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    if result is None:
        raise HTTPException(404, "receipt token not found")
    yield result


@router.get("/risk/{receipt_token_id}/bad-debt", response_model=BadDebtResponse)
async def get_bad_debt(
    receipt_token_id: int,
    gap_pct: Decimal,
    resolved: tuple[RiskCalculationService, int] = Depends(_resolve),
) -> BadDebtResponse:
    """Estimate bad debt for a receipt token position at the given collateral price gap."""
    if not (_ZERO <= gap_pct <= _ONE):
        raise HTTPException(status_code=422, detail="gap_pct must be between 0 and 1")

    service, asset_id = resolved
    try:
        bad_debt = await service.get_bad_debt(backed_asset_id=asset_id, gap_pct=gap_pct)
    except IOError as exc:
        # Transient RPC failure after all retries exhausted.
        raise HTTPException(status_code=502, detail=f"upstream RPC error: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return BadDebtResponse(
        receipt_token_id=receipt_token_id,
        gap_pct=gap_pct,
        bad_debt_usd=bad_debt,
    )


@router.get("/risk/{receipt_token_id}/breakdown", response_model=RiskBreakdownResponse)
async def get_risk_breakdown(
    receipt_token_id: int,
    resolved: tuple[RiskCalculationService, int] = Depends(_resolve),
) -> RiskBreakdownResponse:
    """Return the full risk-enriched collateral breakdown for a receipt token position."""
    service, asset_id = resolved
    try:
        breakdown = await service.get_risk_breakdown(backed_asset_id=asset_id)
    except IOError as exc:
        raise HTTPException(status_code=502, detail=f"upstream RPC error: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return RiskBreakdownResponse(
        receipt_token_id=receipt_token_id,
        items=[
            RiskBreakdownItemResponse(
                token_id=item.token_id,
                symbol=item.symbol,
                amount=item.amount,
                backing_pct=item.backing_pct,
                amount_usd=item.amount_usd,
                price_usd=item.price_usd,
                liquidation_threshold=item.liquidation_threshold,
                liquidation_bonus=item.liquidation_bonus,
            )
            for item in breakdown.items
        ],
    )


@router.post("/risk/rrc", response_model=RrcResponse)
async def post_rrc(
    body: RrcRequest,
    asset_to_rating: dict[str, str] = Depends(get_asset_to_rating),
    suraf_ratings: dict[str, SurafResult] = Depends(get_suraf_ratings),
) -> RrcResponse:
    """Return SURAF RRC for a single asset at the caller-supplied USD exposure.

    ``RRC = usd_exposure * CRR``, where CRR is the pre-computed SURAF rating
    for the asset. No DB lookup; a future GET endpoint will derive
    ``usd_exposure`` from current holdings.
    """
    if body.usd_exposure <= _ZERO:
        raise HTTPException(status_code=422, detail="usd_exposure must be positive")

    result = compute_rrc(body.asset, body.usd_exposure, asset_to_rating, suraf_ratings)
    if result is None:
        raise HTTPException(status_code=404, detail=f"no rating mapped for asset: {body.asset}")
    return RrcResponse(**result.model_dump())
