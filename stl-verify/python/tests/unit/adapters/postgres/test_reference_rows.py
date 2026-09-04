"""Column readers shared by the reference snapshot repositories."""

from decimal import Decimal

import pytest

from app.adapters.postgres._reference_rows import (
    optional_decimal,
    receipt_token_join,
    required_decimal,
    text_or_empty,
    token_address_bytes,
)


def test_required_decimal_rejects_a_null_the_column_forbids() -> None:
    # Defaulting to zero would serve a NOT NULL column the driver could not
    # decode as a real figure of zero.
    with pytest.raises(ValueError, match="assets_usd"):
        required_decimal(None, "assets_usd")


def test_optional_decimal_keeps_an_omitted_figure_distinct_from_zero() -> None:
    assert optional_decimal(None, "idle_assets_usd") is None


@pytest.mark.parametrize("value", [object(), "n/a", "", "1.2.3"])
def test_optional_decimal_names_the_column_it_could_not_decode(value) -> None:
    with pytest.raises(ValueError, match="crr"):
        optional_decimal(value, "crr")


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")])
def test_optional_decimal_rejects_a_non_finite_figure(value) -> None:
    # NUMERIC admits NaN and no CHECK forbids it. Left through, it poisons every
    # total it reaches and makes sorting the rows raise, both far from the column.
    with pytest.raises(ValueError, match="crr"):
        optional_decimal(value, "crr")


def test_optional_decimal_reads_an_ordinary_figure() -> None:
    assert optional_decimal(Decimal("787379142.91"), "assets_usd") == Decimal("787379142.91")


@pytest.mark.parametrize(("value", "expected"), [(None, ""), ("Spark USDS", "Spark USDS")])
def test_text_or_empty_folds_an_omitted_label(value, expected) -> None:
    assert text_or_empty(value) == expected


def test_the_join_key_is_guarded_against_a_value_that_is_not_an_address() -> None:
    # `decode` raises on non-hex, and SQL does not promise to evaluate a
    # conjunction left to right, so the guard has to be a CASE.
    key = token_address_bytes("p.token_address")

    assert key.startswith("CASE WHEN p.token_address ~ ")
    assert "decode(substring(p.token_address FROM 3), 'hex')" in key


def test_the_registry_join_binds_the_alias_it_is_given() -> None:
    # The fragment is the only cross-file SQL here that depends on the caller's
    # alias, so the alias is passed rather than assumed.
    assert "rt.chain_id = r.chain_id" in receipt_token_join("r")
    assert "rt.chain_id = other.chain_id" in receipt_token_join("other")


def test_the_registry_join_keys_on_chain_and_address_together() -> None:
    # Joining on the address alone would attach a mainnet id to a position held
    # on another chain.
    join = receipt_token_join("r")

    assert "rt.chain_id = r.chain_id" in join
    assert "rt.receipt_token_address = r.token_bytes" in join
