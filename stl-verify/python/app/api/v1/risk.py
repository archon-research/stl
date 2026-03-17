from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.backed_breakdown_repository import BackedBreakdownRepository
from app.adapters.postgres.backed_breakdown_repository_morpho import MorphoBackedBreakdownRepository
from app.adapters.postgres.morpho_liquidation_params_repository import MorphoLiquidationParamsRepository
from app.adapters.postgres.protocol_metadata_repository import ProtocolMetadataRepository
from app.adapters.postgres.sparklend_liquidation_params_repository import SparkLendLiquidationParamsRepository
from app.adapters.postgres.token_price_repository import OffchainTokenPriceRepository
from app.services.backed_breakdown_repository_resolver import BackedBreakdownRepositoryResolver
from app.services.liquidation_params_repository_resolver import LiquidationParamsRepositoryResolver
from app.services.risk_calculation_service import RiskCalculationService

router = APIRouter()


class BadDebtResponse(BaseModel):
    protocol_id: int
    backed_asset_id: int
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
    protocol_id: int
    backed_asset_id: int
    items: list[RiskBreakdownItemResponse]


def _get_engine(request: Request) -> AsyncEngine:
    return request.app.state.engine


def _get_service(
    protocol_id: int,
    engine: AsyncEngine = Depends(_get_engine),
) -> RiskCalculationService:
    metadata_repo = ProtocolMetadataRepository(engine)
    price_repo = OffchainTokenPriceRepository(engine)

    breakdown_resolver = BackedBreakdownRepositoryResolver(
        protocol_metadata_repository=metadata_repo,
        aave_like_repository=BackedBreakdownRepository(engine, protocol_id=protocol_id),
        morpho_repository=MorphoBackedBreakdownRepository(engine, protocol_id=protocol_id),
    )

    liq_params_resolver = LiquidationParamsRepositoryResolver(
        protocol_metadata_repository=metadata_repo,
        aave_like_repository=SparkLendLiquidationParamsRepository(engine, protocol_id=protocol_id),
        morpho_repository=MorphoLiquidationParamsRepository(engine),
    )

    return RiskCalculationService(
        backed_breakdown_resolver=breakdown_resolver,
        liquidation_params_resolver=liq_params_resolver,
        token_price_repository=price_repo,
    )


@router.get(
    "/risk/{protocol_id}/{backed_asset_id}/bad-debt",
    response_model=BadDebtResponse,
)
async def get_bad_debt(
    protocol_id: int,
    backed_asset_id: int,
    gap_pct: Decimal,
    service: RiskCalculationService = Depends(_get_service),
) -> BadDebtResponse:
    """Estimate bad debt for a backed position at the given collateral price gap."""
    if gap_pct < Decimal("0") or gap_pct > Decimal("1"):
        raise HTTPException(status_code=422, detail="gap_pct must be between 0 and 1")

    bad_debt = await service.get_bad_debt(
        protocol_id=protocol_id,
        backed_asset_id=backed_asset_id,
        gap_pct=gap_pct,
    )
    return BadDebtResponse(
        protocol_id=protocol_id,
        backed_asset_id=backed_asset_id,
        gap_pct=gap_pct,
        bad_debt_usd=bad_debt,
    )


@router.get(
    "/risk/{protocol_id}/{backed_asset_id}/breakdown",
    response_model=RiskBreakdownResponse,
)
async def get_risk_breakdown(
    protocol_id: int,
    backed_asset_id: int,
    service: RiskCalculationService = Depends(_get_service),
) -> RiskBreakdownResponse:
    """Return the full risk-enriched collateral breakdown for a backed position."""
    breakdown = await service.get_risk_breakdown(
        protocol_id=protocol_id,
        backed_asset_id=backed_asset_id,
    )
    return RiskBreakdownResponse(
        protocol_id=breakdown.protocol_id,
        backed_asset_id=breakdown.backed_asset_id,
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
