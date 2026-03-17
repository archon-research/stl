from decimal import Decimal

from app.domain.entities.backed_breakdown import CollateralContribution
from app.ports.backed_breakdown_repository_resolver import BackedBreakdownRepositoryResolver
from app.risk_engine import gap_sweep
from app.risk_engine.entities import LiquidationParams, RiskBreakdown, RiskEnrichedCollateral
from app.services.liquidation_params_repository_resolver import LiquidationParamsRepositoryResolver


class RiskCalculationService:
    """Orchestrate bad-debt estimation for a backed asset at a given price gap.

    Two-query flow:
      1. Fetch collateral breakdown (backed_breakdown_resolver) — includes backing_usd and price_usd.
      2. Fetch liquidation params for the collateral tokens.
      3. Enrich each CollateralContribution → RiskEnrichedCollateral (skip missing liq params or price).
      4. Apply gap_sweep formula.
    """

    def __init__(
        self,
        backed_breakdown_resolver: BackedBreakdownRepositoryResolver,
        liquidation_params_resolver: LiquidationParamsRepositoryResolver,
    ) -> None:
        self._backed_breakdown_resolver = backed_breakdown_resolver
        self._liquidation_params_resolver = liquidation_params_resolver

    async def get_risk_breakdown(self, protocol_id: int, backed_asset_id: int) -> RiskBreakdown:
        """Return the enriched breakdown for a backed asset (without gap calculation)."""
        items = await self._build_enriched_items(protocol_id, backed_asset_id)
        return RiskBreakdown(
            backed_asset_id=backed_asset_id,
            protocol_id=protocol_id,
            items=tuple(items),
        )

    async def get_bad_debt(
        self, protocol_id: int, backed_asset_id: int, gap_pct: Decimal
    ) -> Decimal:
        """Return the estimated bad debt (as a positive USD value) at the given gap percentage.

        Items with missing price or liquidation params are excluded (treated as
        non-volatile collateral that doesn't contribute to gap-based bad debt).
        """
        items = await self._build_enriched_items(protocol_id, backed_asset_id)
        raw = gap_sweep.total_bad_debt(items, gap_pct)
        return abs(raw)

    async def _build_enriched_items(
        self, protocol_id: int, backed_asset_id: int
    ) -> list[RiskEnrichedCollateral]:
        breakdown_repo = await self._backed_breakdown_resolver.resolve(protocol_id)
        breakdown = await breakdown_repo.get_backed_breakdown(backed_asset_id)

        if not breakdown.items:
            return []

        token_ids = [item.token_id for item in breakdown.items]
        liq_params_repo = await self._liquidation_params_resolver.resolve(protocol_id)
        liq_params = await liq_params_repo.get_params(backed_asset_id, token_ids)

        return [
            self._enrich(item, liq_params)
            for item in breakdown.items
            if item.token_id in liq_params and item.price_usd is not None
        ]

    @staticmethod
    def _enrich(
        item: CollateralContribution,
        liq_params: dict[int, LiquidationParams],
    ) -> RiskEnrichedCollateral:
        params = liq_params[item.token_id]
        return RiskEnrichedCollateral(
            token_id=item.token_id,
            symbol=item.symbol,
            amount=item.backing_usd / item.price_usd,
            backing_pct=item.backing_pct,
            amount_usd=item.backing_usd,
            price_usd=item.price_usd,
            liquidation_threshold=params.liquidation_threshold,
            liquidation_bonus=params.liquidation_bonus,
        )
