from collections.abc import AsyncGenerator
from decimal import Decimal

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine

from app.api.deps import get_asset_to_rating, get_engine, get_http_client, get_suraf_ratings
from app.config import Settings, get_settings
from app.risk_engine.rrc import (
    PortfolioRrcResult,
    ScenarioRrcResult,
    compute_portfolio_rrc,
    compute_scenario_rrc,
)
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


class RrcModelEntryResponse(BaseModel):
    name: str
    source_commit_sha: str
    modeled_exposure_usd: Decimal
    crr_pct: Decimal
    rrc_usd: Decimal


class RrcItemResponse(BaseModel):
    token_id: int
    symbol: str
    amount_usd: Decimal
    rating_id: str | None
    crr_pct: Decimal | None
    rrc_usd: Decimal | None


class PortfolioRrcResponse(BaseModel):
    receipt_token_id: int
    total_exposure_usd: Decimal
    modeled_exposure_usd: Decimal
    final_crr_pct: Decimal | None
    final_rrc_usd: Decimal | None
    models: list[RrcModelEntryResponse]
    items: list[RrcItemResponse]


class ScenarioRrcResponse(BaseModel):
    asset: str
    usd_exposure: Decimal
    final_crr_pct: Decimal
    final_rrc_usd: Decimal
    models: list[RrcModelEntryResponse]


class ScenarioRrcRequest(BaseModel):
    asset: str
    usd_exposure: Decimal


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


def _portfolio_response(receipt_token_id: int, result: PortfolioRrcResult) -> PortfolioRrcResponse:
    return PortfolioRrcResponse(
        receipt_token_id=receipt_token_id,
        total_exposure_usd=result.total_exposure_usd,
        modeled_exposure_usd=result.modeled_exposure_usd,
        final_crr_pct=result.final_crr_pct,
        final_rrc_usd=result.final_rrc_usd,
        models=[
            RrcModelEntryResponse(
                name=m.name,
                source_commit_sha=m.source_commit_sha,
                modeled_exposure_usd=m.modeled_exposure_usd,
                crr_pct=m.crr_pct,
                rrc_usd=m.rrc_usd,
            )
            for m in result.models
        ],
        items=[
            RrcItemResponse(
                token_id=i.token_id,
                symbol=i.symbol,
                amount_usd=i.amount_usd,
                rating_id=i.rating_id,
                crr_pct=i.crr_pct,
                rrc_usd=i.rrc_usd,
            )
            for i in result.items
        ],
    )


def _scenario_response(result: ScenarioRrcResult) -> ScenarioRrcResponse:
    return ScenarioRrcResponse(
        asset=result.asset,
        usd_exposure=result.usd_exposure,
        final_crr_pct=result.final_crr_pct,
        final_rrc_usd=result.final_rrc_usd,
        models=[
            RrcModelEntryResponse(
                name=m.name,
                source_commit_sha=m.source_commit_sha,
                modeled_exposure_usd=m.modeled_exposure_usd,
                crr_pct=m.crr_pct,
                rrc_usd=m.rrc_usd,
            )
            for m in result.models
        ],
    )


@router.get("/risk/{receipt_token_id}/rrc", response_model=PortfolioRrcResponse)
async def get_rrc(
    receipt_token_id: int,
    resolved: tuple[RiskCalculationService, int] = Depends(_resolve),
    asset_to_rating: dict[str, str] = Depends(get_asset_to_rating),
    suraf_ratings: dict[str, SurafResult] = Depends(get_suraf_ratings),
) -> PortfolioRrcResponse:
    """Return per-collateral RRC computed from the receipt token's current backing."""
    service, asset_id = resolved
    try:
        breakdown = await service.get_risk_breakdown(backed_asset_id=asset_id)
    except IOError as exc:
        raise HTTPException(status_code=502, detail=f"upstream RPC error: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    result = compute_portfolio_rrc(breakdown, asset_to_rating, suraf_ratings)
    return _portfolio_response(receipt_token_id, result)


@router.post("/risk/rrc", response_model=ScenarioRrcResponse)
async def post_rrc(
    body: ScenarioRrcRequest,
    asset_to_rating: dict[str, str] = Depends(get_asset_to_rating),
    suraf_ratings: dict[str, SurafResult] = Depends(get_suraf_ratings),
) -> ScenarioRrcResponse:
    """Return RRC for a single asset at a caller-supplied USD exposure. No DB lookup."""
    if body.usd_exposure <= _ZERO:
        raise HTTPException(status_code=422, detail="usd_exposure must be positive")

    result = compute_scenario_rrc(body.asset, body.usd_exposure, asset_to_rating, suraf_ratings)
    if result is None:
        raise HTTPException(status_code=404, detail=f"no rating mapped for asset: {body.asset}")
    return _scenario_response(result)
