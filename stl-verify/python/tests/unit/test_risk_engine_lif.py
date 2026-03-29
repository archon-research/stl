# tests/unit/test_risk_engine_lif.py
from decimal import Decimal

import pytest

from app.risk_engine.crypto_lending.lif import compute_lif


@pytest.mark.parametrize(
    "lltv, expected",
    [
        # lltv=0.86 → 1 / (0.3×0.86 + 0.7) = 1 / (0.258 + 0.7) = 1 / 0.958 ≈ 1.0438
        (Decimal("0.86"), Decimal("1.0438")),
        # lltv=0.945 → 1 / (0.3×0.945 + 0.7) = 1 / (0.2835 + 0.7) = 1 / 0.9835 ≈ 1.0168
        (Decimal("0.945"), Decimal("1.0168")),
        # lltv=0.3 → 1 / (0.3×0.3 + 0.7) = 1 / (0.09 + 0.7) = 1 / 0.79 ≈ 1.2658 → capped at 1.15
        (Decimal("0.3"), Decimal("1.15")),
        # lltv=1.0 → 1 / (0.3×1.0 + 0.7) = 1 / 1.0 = 1.0 (no bonus)
        (Decimal("1.0"), Decimal("1.0")),
    ],
)
def test_compute_lif(lltv: Decimal, expected: Decimal) -> None:
    result = compute_lif(lltv)
    assert abs(result - expected) < Decimal("0.0001")


def test_lif_never_exceeds_cap() -> None:
    for lltv_int in range(1, 100):
        lltv = Decimal(lltv_int) / 100
        assert compute_lif(lltv) <= Decimal("1.15")


def test_lif_always_at_least_one() -> None:
    for lltv_int in range(1, 101):
        lltv = Decimal(lltv_int) / 100
        assert compute_lif(lltv) >= Decimal("1.0")
