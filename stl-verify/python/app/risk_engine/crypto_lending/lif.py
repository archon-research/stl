"""Morpho Liquidation Incentive Factor (LIF) computation.

Spec: https://docs.morpho.org/learn/concepts/liquidation/
"""

from decimal import Decimal

_BETA = Decimal("0.3")  # LIF formula steepness parameter
_MAX_LIF = Decimal("1.15")  # upper cap on liquidation incentive factor


def compute_lif(lltv: Decimal) -> Decimal:
    """Compute the Morpho Liquidation Incentive Factor from LLTV.

    Formula (from https://docs.morpho.org/learn/concepts/liquidation/):
        LIF = min(MAX_LIF, 1 / (β × lltv + (1 - β)))
        where β = 0.3, MAX_LIF = 1.15

    lltv must be in [0, 1] range.
    Returns a value in [1.0, 1.15].
    """
    denominator = _BETA * lltv + (1 - _BETA)
    return min(_MAX_LIF, Decimal("1") / denominator)
