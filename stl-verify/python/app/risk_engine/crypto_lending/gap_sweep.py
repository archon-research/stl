from collections.abc import Iterable
from decimal import Decimal

from app.domain.entities.risk import RiskEnrichedCollateral
from app.risk_engine._vendored_synome.risk_capital.formulas.required_risk_capital import (
    loss_given_default,
)


def bad_debt_at_gap(item: RiskEnrichedCollateral, gap_pct: Decimal) -> Decimal:
    """Compute bad debt (≤ 0) for one collateral item at a given price gap.

    Assumes the position is at HF=1 (liquidation trigger). The gap models the
    price decline between liquidation trigger and liquidation execution.

    Formula:
        collateral_at_trigger = amount_usd / liquidation_threshold
        recovered_after_gap   = collateral_at_trigger × (1 - gap_pct) / liquidation_bonus
        bad_debt              = min(0, recovered_after_gap - amount_usd)

    Returns ≤ 0. Zero = fully covered; negative = bad debt in USD.
    """
    if item.liquidation_threshold is None or item.liquidation_bonus is None:
        # Symbol-keyed collateral without per-asset liquidation params (e.g. Maple
        # custody assets) is not modellable by gap-sweep, so it contributes no
        # modelled bad debt rather than raising on float(None).
        return Decimal("0")
    bonus = float(item.liquidation_bonus)
    liquidation_penalty = 1 - 1 / bonus
    lgd = loss_given_default(
        liquidation_threshold=float(item.liquidation_threshold),
        liquidation_penalty=liquidation_penalty,
        slippage=float(gap_pct),
    )
    return -Decimal.from_float(lgd) * item.amount_usd


def total_bad_debt(items: Iterable[RiskEnrichedCollateral], gap_pct: Decimal) -> Decimal:
    """Sum bad debt across all collateral items at a given gap percentage.

    Returns a value ≤ 0. Call abs() on the result for a positive USD amount.
    """
    return sum((bad_debt_at_gap(item, gap_pct) for item in items), Decimal("0"))
