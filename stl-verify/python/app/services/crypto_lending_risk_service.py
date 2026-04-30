from collections.abc import Collection, Mapping
from decimal import Decimal
from typing import Any

from app.domain.entities.allocation import EthAddress
from app.domain.entities.backed_breakdown import BackedBreakdown, CollateralContribution
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.entities.risk import (
    GapSweepDetails,
    LiquidationParams,
    RiskBreakdown,
    RiskEnrichedCollateral,
    RrcResult,
)
from app.logging import get_logger
from app.ports.crypto_lending_reader import CryptoLendingReader
from app.risk_engine.crypto_lending import gap_sweep

logger = get_logger(__name__)

_ALLOWED_OVERRIDES = frozenset({"gap_pct"})


class CryptoLendingRiskService:
    """RiskModel for crypto-lending assets using the gap-sweep stress."""

    model: str = "gap_sweep"

    def __init__(
        self,
        reader: CryptoLendingReader,
        default_gap_pct: Decimal,
        supported_asset_ids: Collection[int],
    ) -> None:
        self._reader = reader
        self._default_gap_pct = default_gap_pct
        self._supported_asset_ids = frozenset(supported_asset_ids)

    # ------------------------------------------------------------------
    # RiskModel interface implementation
    # ------------------------------------------------------------------

    def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool:  # noqa: ARG002
        return asset_id in self._supported_asset_ids  # maybe this should just call reader.list_supproted...

    async def compute(
        self,
        asset_id: int,
        prime_id: EthAddress,
        overrides: Mapping[str, Any],
    ) -> RrcResult:
        """Compute the RRC for the given asset and prime via gap-sweep."""
        if not self.applies_to(asset_id, prime_id):
            raise ValueError(f"unsupported asset_id={asset_id}")

        gap_pct = self._resolve_gap_pct(overrides)
        items = await self._load_enriched_items(receipt_token_id=asset_id, prime_id=prime_id)
        raw = gap_sweep.total_bad_debt(items, gap_pct)
        rrc_usd = abs(raw)
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

    async def _load_enriched_items(self, receipt_token_id: int, prime_id: EthAddress) -> list[RiskEnrichedCollateral]:
        info = await self._reader.get_receipt_token(receipt_token_id)
        if info is None:
            raise ValueError(f"receipt token not found: {receipt_token_id}")
        _, items = await self._load_enriched_items_for_info(info, prime_id=prime_id)
        return items

    # ------------------------------------------------------------------
    # Legacy public API — used by old endpoints, will be removed in VEC-183.
    # ------------------------------------------------------------------

    async def get_risk_breakdown_legacy(self, receipt_token_id: int) -> RiskBreakdown | None:
        """Return the legacy risk breakdown for a receipt token, or ``None`` if unknown."""
        resolved = await self._load_enriched_items_or_none(receipt_token_id, prime_id=None)
        if resolved is None:
            return None
        backed_asset_id, items = resolved
        return RiskBreakdown(backed_asset_id=backed_asset_id, items=tuple(items))

    async def get_bad_debt_legacy(self, receipt_token_id: int, gap_pct: Decimal) -> Decimal | None:
        """Return legacy bad debt for a receipt token, or ``None`` if unknown."""
        resolved = await self._load_enriched_items_or_none(receipt_token_id, prime_id=None)
        if resolved is None:
            return None
        _, items = resolved
        raw = gap_sweep.total_bad_debt(items, gap_pct)
        return abs(raw)

    async def _load_enriched_items_or_none(
        self,
        receipt_token_id: int,
        prime_id: EthAddress | None,
    ) -> tuple[int, list[RiskEnrichedCollateral]] | None:
        info = await self._reader.get_receipt_token(receipt_token_id)
        if info is None:
            return None
        return await self._load_enriched_items_for_info(info, prime_id=prime_id)

    async def _load_enriched_items_for_info(
        self,
        info: ReceiptTokenInfo,
        prime_id: EthAddress | None,
    ) -> tuple[int, list[RiskEnrichedCollateral]]:
        breakdown = await self._reader.get_breakdown(info)
        if not breakdown.items:
            return breakdown.backed_asset_id, []

        if prime_id is None:
            share = await self._reader.get_legacy_share(info)
        else:
            share = await self._reader.get_share(info, prime_id)

        token_ids = [item.token_id for item in breakdown.items]
        liq_params = await self._reader.get_liquidation_params(info, breakdown.backed_asset_id, token_ids)
        return breakdown.backed_asset_id, self._build_enriched_items(breakdown, share, liq_params)

    def _build_enriched_items(
        self,
        breakdown: BackedBreakdown,
        share: Decimal,
        liq_params: dict[int, LiquidationParams],
    ) -> list[RiskEnrichedCollateral]:
        enriched: list[RiskEnrichedCollateral] = []
        for item in breakdown.items:
            if item.token_id not in liq_params:
                logger.warning(
                    "Dropping collateral item backed_asset_id=%d token_id=%d symbol=%s: missing liquidation params",
                    breakdown.backed_asset_id,
                    item.token_id,
                    item.symbol,
                )
                continue
            if item.price_usd is None or item.price_usd == 0:
                logger.warning(
                    "Dropping collateral item backed_asset_id=%d token_id=%d symbol=%s: missing or zero price",
                    breakdown.backed_asset_id,
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
