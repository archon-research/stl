from decimal import Decimal

import pytest

from app.api.output.formatters.units_formatter import UnitsFormatter


@pytest.mark.parametrize(
    ("value", "precision", "expected"),
    [
        (Decimal("1234567.89"), 2, "$1,234,567.89"),
        (Decimal("0.001"), 2, "$0.00"),
        (Decimal("-12.34"), 2, "$-12.34"),
    ],
)
def test_format_usd_amount(value: Decimal, precision: int, expected: str) -> None:
    assert UnitsFormatter.format_usd_amount(value, precision=precision) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("0.1234"), "12.34%"),
        (Decimal("0.05"), "5.00%"),
        (Decimal("-0.25"), "-25.00%"),
    ],
)
def test_format_percentage(value: Decimal, expected: str) -> None:
    assert UnitsFormatter.format_percentage(value) == expected


def test_format_token_amount_raw_scales_by_decimals() -> None:
    result = UnitsFormatter.format_token_amount(Decimal("1500000"), symbol="USDC", decimals=6, is_raw=True)
    assert result == "1.5 USDC"


@pytest.mark.parametrize(
    ("current", "prior", "unit", "expected"),
    [
        (Decimal("100"), Decimal("90"), "%", "+11.11%"),
        (Decimal("50"), Decimal("60"), "$", "-$10.00"),
        (Decimal("2"), Decimal("1"), "x", "+1.0000x"),
        (Decimal("10"), Decimal("0"), "%", "N/A"),
    ],
)
def test_format_delta_variants(current: Decimal, prior: Decimal, unit: str, expected: str) -> None:
    assert UnitsFormatter.format_delta(current, prior, unit=unit) == expected


@pytest.mark.parametrize(
    ("timestamp_seconds", "now_seconds", "expected"),
    [
        (1000, 1000, "just now"),
        (1000, 1030, "just now"),
        (1000, 1060, "1m ago"),
        (1000, 4600, "1h ago"),
        (1000, 200000, "2d ago"),
        (1000, 700000, "1w ago"),
        (1100, 1000, "in the future"),
    ],
)
def test_get_freshness_label(timestamp_seconds: int, now_seconds: int, expected: str) -> None:
    assert UnitsFormatter.get_freshness_label(timestamp_seconds, now_seconds) == expected
