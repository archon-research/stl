from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class CollateralContribution:
    """A single collateral asset contribution.

    backing_usd is the USD value of this asset's contribution to the total backing.
    It is computed as: (collateral_usd / total_collateral_usd_for_user) * usds_debt_usd,
    summed across all borrowers. This ensures SUM(backing_usd) == total backed asset debt.
    """

    token_id: int
    symbol: str
    backing_usd: Decimal
    backing_pct: Decimal
    price_usd: Decimal | None


@dataclass(frozen=True)
class BackedBreakdown:
    """The full amount-first breakdown of collateral backing a debt token."""

    backed_asset_id: int
    items: tuple[CollateralContribution, ...]
