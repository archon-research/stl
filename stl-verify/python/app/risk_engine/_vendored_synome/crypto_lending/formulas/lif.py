"""
Liquidation Incentive Factor (LIF) Formula

Computes the liquidation incentive factor for a given loan-to-value ratio,
following the Morpho protocol specification.

Reference: https://docs.morpho.org/learn/concepts/liquidation/
"""

from typing import Final

BETA: Final[float] = 0.3
"""Weight parameter for the LLTV in the LIF denominator calculation."""

M: Final[float] = 1.15
"""Maximum liquidation incentive factor cap."""


def lif(
    lltv: float,
) -> float:
    """
    Calculate the liquidation incentive factor (LIF) for a given LLTV.

    Returns a value between 1.0 and M (1.15).
    """
    denominator: float = BETA * lltv + (1.0 - BETA)
    return min(M, 1.0 / denominator)
