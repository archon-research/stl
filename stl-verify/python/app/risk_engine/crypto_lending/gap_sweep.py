from collections.abc import Iterable
from decimal import Decimal

from app.domain.entities.risk import RiskEnrichedCollateral


def bad_debt_at_gap(item: RiskEnrichedCollateral, gap_pct: Decimal) -> Decimal:
    """Compute bad debt for one collateral item at a given price gap.

    Assumes the position is at HF=1 (liquidation trigger). The gap is the
    fraction of the collateral price that drops before liquidation executes.

    Formula:
        gross     = amount_usd / liquidation_threshold
        recoverable = gross × (1 - gap_pct) / liquidation_bonus
        bad_debt  = min(0, recoverable - amount_usd)

    Returns a value ≤ 0. Zero means no bad debt; negative means bad debt.
    """
    gross = item.amount_usd / item.liquidation_threshold
    recoverable = gross * (1 - gap_pct) / item.liquidation_bonus
    return min(Decimal("0"), recoverable - item.amount_usd)


def total_bad_debt(items: Iterable[RiskEnrichedCollateral], gap_pct: Decimal) -> Decimal:
    """Sum bad debt across all collateral items at a given gap percentage.

    Returns a value ≤ 0. Call abs() on the result for a positive USD amount.
    """
    return sum((bad_debt_at_gap(item, gap_pct) for item in items), Decimal("0"))
