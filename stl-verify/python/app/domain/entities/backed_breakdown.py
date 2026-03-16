from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class CollateralContribution:
    """A single collateral asset contribution, with amount before percentage."""

    token_id: int
    symbol: str
    amount: Decimal
    backing_pct: Decimal


@dataclass(frozen=True)
class BackedBreakdown:
    """The full amount-first breakdown of collateral backing a debt token."""

    debt_token_id: int
    protocol_id: int
    items: tuple[CollateralContribution, ...]
