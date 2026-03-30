import logging
from decimal import Decimal

from app.domain.entities.backed_breakdown import CollateralContribution
from app.domain.entities.risk import LiquidationParams, RiskBreakdown, RiskEnrichedCollateral
from app.ports.allocation_share_port import AllocationSharePort
from app.ports.backed_breakdown_repository import BackedBreakdownRepository
from app.ports.liquidation_params_repository import LiquidationParamsRepository
from app.risk_engine.crypto_lending import gap_sweep

logger = logging.getLogger(__name__)


class RiskCalculationService:
    """Orchestrate bad-debt estimation for a backed asset at a given price gap."""

    def __init__(
        self,
        breakdown_repo: BackedBreakdownRepository,
        liq_params_repo: LiquidationParamsRepository,
        share_port: AllocationSharePort,
    ) -> None:
        self._breakdown_repo = breakdown_repo
        self._liq_params_repo = liq_params_repo
        self._share_port = share_port

    async def get_risk_breakdown(self, backed_asset_id: int) -> RiskBreakdown:
        items = await self._build_enriched_items(backed_asset_id)
        return RiskBreakdown(backed_asset_id=backed_asset_id, items=tuple(items))

    async def get_bad_debt(self, backed_asset_id: int, gap_pct: Decimal) -> Decimal:
        items = await self._build_enriched_items(backed_asset_id)
        raw = gap_sweep.total_bad_debt(items, gap_pct)
        return abs(raw)

    async def _build_enriched_items(self, backed_asset_id: int) -> list[RiskEnrichedCollateral]:
        breakdown = await self._breakdown_repo.get_backed_breakdown(backed_asset_id)
        if not breakdown.items:
            return []

        share = await self._share_port.get_share()

        token_ids = [item.token_id for item in breakdown.items]
        liq_params = await self._liq_params_repo.get_params(backed_asset_id, token_ids)

        enriched: list[RiskEnrichedCollateral] = []
        for item in breakdown.items:
            if item.token_id not in liq_params:
                logger.warning(
                    "Dropping collateral item token_id=%d symbol=%s: missing liquidation params",
                    item.token_id,
                    item.symbol,
                )
                continue
            if item.price_usd is None:
                logger.warning(
                    "Dropping collateral item token_id=%d symbol=%s: missing price",
                    item.token_id,
                    item.symbol,
                )
                continue
            enriched.append(self._enrich(item, share, item.price_usd, liq_params[item.token_id]))
        return enriched

    @staticmethod
    def _enrich(
        item: CollateralContribution,
        share: Decimal,
        price_usd: Decimal,
        params: LiquidationParams,
    ) -> RiskEnrichedCollateral:
        scaled_backing_value = item.backing_value * share
        return RiskEnrichedCollateral(
            token_id=item.token_id,
            symbol=item.symbol,
            amount=scaled_backing_value / price_usd,
            backing_pct=item.backing_pct,
            amount_usd=scaled_backing_value,
            price_usd=price_usd,
            liquidation_threshold=params.liquidation_threshold,
            liquidation_bonus=params.liquidation_bonus,
        )
