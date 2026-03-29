from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine

from app.services.risk_calculation_service import RiskCalculationService
from app.services.risk_service_factory import RiskServiceFactory

router = APIRouter()


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


def _get_engine(request: Request) -> AsyncEngine:
    return request.app.state.engine


async def _resolve(engine: AsyncEngine, receipt_token_id: int) -> tuple[RiskCalculationService, int]:
    """Resolve receipt token into a service + backed_asset_id, or raise HTTP errors."""
    factory = RiskServiceFactory(engine)
    try:
        result = await factory.create(receipt_token_id)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    if result is None:
        raise HTTPException(404, "receipt token not found")
    return result


@router.get("/risk/{receipt_token_id}/bad-debt", response_model=BadDebtResponse)
async def get_bad_debt(
    receipt_token_id: int,
    gap_pct: Decimal,
    engine: AsyncEngine = Depends(_get_engine),
) -> BadDebtResponse:
    """Estimate bad debt for a receipt token position at the given collateral price gap."""
    if gap_pct < Decimal("0") or gap_pct > Decimal("1"):
        raise HTTPException(status_code=422, detail="gap_pct must be between 0 and 1")

    service, asset_id = await _resolve(engine, receipt_token_id)
    bad_debt = await service.get_bad_debt(backed_asset_id=asset_id, gap_pct=gap_pct)
    return BadDebtResponse(
        receipt_token_id=receipt_token_id,
        gap_pct=gap_pct,
        bad_debt_usd=bad_debt,
    )


@router.get("/risk/{receipt_token_id}/breakdown", response_model=RiskBreakdownResponse)
async def get_risk_breakdown(
    receipt_token_id: int,
    engine: AsyncEngine = Depends(_get_engine),
) -> RiskBreakdownResponse:
    """Return the full risk-enriched collateral breakdown for a receipt token position."""
    service, asset_id = await _resolve(engine, receipt_token_id)
    breakdown = await service.get_risk_breakdown(backed_asset_id=asset_id)
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
