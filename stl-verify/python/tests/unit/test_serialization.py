from decimal import Decimal

import pytest
from pydantic import BaseModel

from app.domain.serialization import PlainDecimal


class _Model(BaseModel):
    value: PlainDecimal
    optional: PlainDecimal | None = None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # Positive-exponent Decimals (as asyncpg hands back large trailing-zero
        # NUMERICs) must expand to their full fixed-point digits.
        (Decimal((0, (2, 5, 7, 0, 7, 1, 4), 21)), "2570714000000000000000000000"),
        (Decimal("2.199E+7"), "21990000"),
        (Decimal("1.23456789E+21"), "1234567890000000000000"),
        # Plain and fractional values pass through unchanged and exact.
        (Decimal("0"), "0"),
        (Decimal("1.0001"), "1.0001"),
        (Decimal("3.5E-9"), "0.0000000035"),
        (Decimal("-4.2E+3"), "-4200"),
    ],
)
def test_plain_decimal_serializes_without_exponent(raw: Decimal, expected: str):
    assert _Model(value=raw).model_dump_json() == f'{{"value":"{expected}","optional":null}}'


def test_plain_decimal_none_serializes_as_null():
    assert _Model(value=Decimal("0"), optional=None).model_dump(mode="json")["optional"] is None
