from collections.abc import AsyncGenerator
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_share_repository import (
    AllocationShareError,
    MissingShareError,
    StaleShareError,
)
from app.api.deps import get_engine, get_suraf_rrc_service
from app.config import Settings, get_settings
from app.services.crypto_lending_risk_service import CryptoLendingRiskService
from app.services.risk_service_factory import RiskServiceFactory
from app.services.suraf_rrc_service import SurafRrcService

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


class ScenarioRrcRequest(BaseModel):
    receipt_token_id: int
    usd_exposure: Decimal


class ScenarioRrcResponse(BaseModel):
    receipt_token_id: int
    usd_exposure: Decimal
    rating_id: str
    rating_version: str
    crr_pct: Decimal
    rrc_usd: Decimal
    source_commit_sha: str


def _share_error_503(exc: AllocationShareError) -> HTTPException:
    """Translate an AllocationShareError subtype into a 503 with a distinct code."""
    if isinstance(exc, StaleShareError):
        code = "share_data_stale"
    elif isinstance(exc, MissingShareError):
        code = "share_data_missing"
    else:
        code = "share_data_unavailable"
    return HTTPException(status_code=503, detail={"code": code, "message": str(exc)})


async def _resolve(
    receipt_token_id: int,
    engine: AsyncEngine = Depends(get_engine),
    settings: Settings = Depends(get_settings),
) -> AsyncGenerator[tuple[CryptoLendingRiskService, int], None]:
    """Resolve receipt token into a service + backed_asset_id, or raise HTTP errors.

    Uses ``async with`` + ``yield`` so the DB connection is properly cleaned up.
    """
    factory = RiskServiceFactory(
        engine,
        allocation_share_max_stale_seconds=settings.allocation_share_max_stale_seconds,
        default_gap_pct=settings.risk_default_gap_pct,
    )
    try:
        result = await factory.create(receipt_token_id)
    except AllocationShareError as exc:
        raise _share_error_503(exc) from exc
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    if result is None:
        raise HTTPException(404, "receipt token not found")
    yield result


@router.get("/risk/{receipt_token_id}/bad-debt", response_model=BadDebtResponse)
async def get_bad_debt(
    receipt_token_id: int,
    gap_pct: Decimal,
    resolved: tuple[CryptoLendingRiskService, int] = Depends(_resolve),
) -> BadDebtResponse:
    """Estimate bad debt for a receipt token position at the given collateral price gap."""
    if not (_ZERO <= gap_pct <= _ONE):
        raise HTTPException(status_code=422, detail="gap_pct must be between 0 and 1")

    service, asset_id = resolved
    try:
        bad_debt = await service.get_bad_debt(backed_asset_id=asset_id, gap_pct=gap_pct)
    except AllocationShareError as exc:
        raise _share_error_503(exc) from exc
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
    resolved: tuple[CryptoLendingRiskService, int] = Depends(_resolve),
) -> RiskBreakdownResponse:
    """Return the full risk-enriched collateral breakdown for a receipt token position."""
    service, asset_id = resolved
    try:
        breakdown = await service.get_risk_breakdown(backed_asset_id=asset_id)
    except AllocationShareError as exc:
        raise _share_error_503(exc) from exc
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


@router.post("/risk/rrc/scenario", response_model=ScenarioRrcResponse)
async def post_rrc_scenario(
    body: ScenarioRrcRequest,
    service: SurafRrcService = Depends(get_suraf_rrc_service),
) -> ScenarioRrcResponse:
    """Return SURAF RRC for a hypothetical ``(receipt_token_id, usd_exposure)`` pair.

    ``RRC = usd_exposure * CRR``, where CRR is the pre-computed SURAF rating
    for the receipt token. Pure scenario calculation — no position state.
    """
    if body.usd_exposure <= _ZERO:
        raise HTTPException(status_code=422, detail="usd_exposure must be positive")

    result = service.compute_legacy(body.receipt_token_id, body.usd_exposure)
    if result is None:
        raise HTTPException(status_code=404, detail=f"no rating mapped for receipt_token_id: {body.receipt_token_id}")
    return ScenarioRrcResponse(**result.model_dump())
