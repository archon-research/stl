from decimal import Decimal

_BETA = Decimal("0.3")
_M = Decimal("1.15")


def compute_lif(lltv: Decimal) -> Decimal:
    """Compute the Morpho Liquidation Incentive Factor from LLTV.

    Formula (from https://docs.morpho.org/learn/concepts/liquidation/):
        LIF = min(M, 1 / (β × lltv + (1 - β)))
        where β = 0.3, M = 1.15

    lltv must be in [0, 1] range.
    Returns a value in [1.0, 1.15].
    """
    denominator = _BETA * lltv + (1 - _BETA)
    return min(_M, Decimal("1") / denominator)
