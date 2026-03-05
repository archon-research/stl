from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class CollateralContribution:
    """A single collateral asset's contribution to backing a debt token."""

    token_id: int
    symbol: str
    total_backing_usd: Decimal
    backing_pct: Decimal


@dataclass(frozen=True)
class BackedBreakdown:
    """The full breakdown of which collateral assets back a given debt token."""

    debt_token_id: int
    protocol_id: int
    items: tuple[CollateralContribution, ...]
