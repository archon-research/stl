"""Morpho Liquidation Incentive Factor (LIF) computation.

Spec: https://docs.morpho.org/learn/concepts/liquidation/
"""

from decimal import Decimal

from axis.crypto_lending.formulas.lif import lif as compute_lif_float


def compute_lif(lltv: Decimal) -> Decimal:
    """Compute the Morpho Liquidation Incentive Factor from LLTV.

    Formula (from https://docs.morpho.org/learn/concepts/liquidation/):
        LIF = min(MAX_LIF, 1 / (β × lltv + (1 - β)))
        where β = 0.3, MAX_LIF = 1.15

    lltv must be in [0, 1] range.
    Returns a value in [1.0, 1.15].
    """
    return Decimal.from_float(compute_lif_float(float(lltv)))
