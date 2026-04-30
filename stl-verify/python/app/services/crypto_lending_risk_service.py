from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from app.domain.entities.allocation import EthAddress
from app.domain.entities.backed_breakdown import CollateralContribution
from app.domain.entities.risk import (
    GapSweepDetails,
    LiquidationParams,
    RiskBreakdown,
    RiskEnrichedCollateral,
    RrcResult,
)
from app.logging import get_logger
from app.ports.allocation_share_port import AllocationSharePort
from app.ports.backed_breakdown_repository import BackedBreakdownRepository
from app.ports.liquidation_params_repository import LiquidationParamsRepository
from app.risk_engine.crypto_lending import gap_sweep

logger = get_logger(__name__)


_ALLOWED_OVERRIDES = frozenset({"gap_pct"})


class CryptoLendingRiskService:
    """Orchestrate bad-debt estimation for a backed asset at a given price gap.

    Implements the :class:`~app.ports.risk_model.RiskModel` protocol so it can
    be used by the unified risk-model registry.
    """

    model: str = "gap_sweep"

    def __init__(
        self,
        breakdown_repo: BackedBreakdownRepository,
        liq_params_repo: LiquidationParamsRepository,
        share_port: AllocationSharePort,
        default_gap_pct: Decimal,
    ) -> None:
        self._breakdown_repo = breakdown_repo
        self._liq_params_repo = liq_params_repo
        self._share_port = share_port
        self._default_gap_pct = default_gap_pct

    # ------------------------------------------------------------------
    # RiskModel interface implementation
    # ------------------------------------------------------------------

    def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool:  # noqa: ARG002
        # TODO(VEC-178): Move protocol-level filtering from RiskServiceFactory
        # into this method so the registry can stay model-agnostic.
        return True

    async def compute(
        self,
        asset_id: int,
        prime_id: EthAddress,
        overrides: Mapping[str, Any],
    ) -> RrcResult:
        """Compute the RRC for the given asset and prime via gap-sweep."""
        gap_pct = self._resolve_gap_pct(overrides)
        bad_debt = await self.get_bad_debt(asset_id, gap_pct)
        rrc_usd = abs(bad_debt)
        return RrcResult(
            asset_id=asset_id,
            prime_id=prime_id,
            rrc_usd=rrc_usd,
            model="gap_sweep",
            details=GapSweepDetails(gap_pct=gap_pct, bad_debt_usd=rrc_usd),
        )

    def _resolve_gap_pct(self, overrides: Mapping[str, Any]) -> Decimal:
        """Extract and validate gap_pct from overrides, falling back to the default."""
        unknown = set(overrides) - _ALLOWED_OVERRIDES
        if unknown:
            raise ValueError(f"unknown override keys: {sorted(unknown)}")

        gap_pct: Decimal = overrides.get("gap_pct", self._default_gap_pct)

        if not (Decimal("0") <= gap_pct <= Decimal("1")):
            raise ValueError(f"gap_pct must be in [0, 1], got {gap_pct}")

        return gap_pct

    # ------------------------------------------------------------------
    # Legacy public API — used by old endpoints, will be removed in VEC-183.
    # ------------------------------------------------------------------

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
                    "Dropping collateral item backed_asset_id=%d token_id=%d symbol=%s: missing liquidation params",
                    backed_asset_id,
                    item.token_id,
                    item.symbol,
                )
                continue
            if item.price_usd is None or item.price_usd == 0:
                logger.warning(
                    "Dropping collateral item backed_asset_id=%d token_id=%d symbol=%s: missing or zero price",
                    backed_asset_id,
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
