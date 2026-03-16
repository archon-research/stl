from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class LiquidationParams:
    """Normalised liquidation parameters for a single collateral token.

    Both fields are in [0, ∞) range after protocol-specific normalisation:
    - liquidation_threshold: e.g. 0.825 for 82.5%
    - liquidation_bonus:     e.g. 1.05 for a 5% bonus (always > 1.0)
    """

    token_id: int
    liquidation_threshold: Decimal
    liquidation_bonus: Decimal


@dataclass(frozen=True)
class RiskEnrichedCollateral:
    """A single collateral contribution enriched with USD value and liquidation params."""

    token_id: int
    symbol: str
    amount: Decimal           # human-readable token units
    backing_pct: Decimal      # 0..100
    amount_usd: Decimal       # amount × price_usd
    price_usd: Decimal        # USD spot price used
    liquidation_threshold: Decimal
    liquidation_bonus: Decimal


@dataclass(frozen=True)
class RiskBreakdown:
    """Full enriched breakdown for a backed asset — input to gap_sweep."""

    backed_asset_id: int
    protocol_id: int
    items: tuple[RiskEnrichedCollateral, ...]
