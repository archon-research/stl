"""Unit tests for the positions frame assembly (the model-semantics core)."""

import pytest

from app.adapters.postgres.core_model_positions_reader import (
    PositionRow,
    build_market_frame,
    build_users_frame,
)

_PRICES = {"WETH": 2000.0, "WSTETH": 2400.0, "USDT": 1.0, "USDS": 1.0, "DAI": 1.0}
_RESERVES = {"WETH": (0.86, 1.05), "WSTETH": (0.84, 1.07), "USDS": (0.0, 0.0)}


def _user(address="0xaaa", supplies=(), borrows=()):
    rows = [PositionRow("supply", address, sym, qty, enabled) for sym, qty, enabled in supplies]
    rows += [PositionRow("borrow", address, sym, qty, True) for sym, qty in borrows]
    return rows


def test_wide_columns_match_the_parquet_shape():
    rows = _user(supplies=[("WETH", 10.0, True)], borrows=[("USDT", 5000.0)])
    df = build_users_frame(rows, _RESERVES, _PRICES, "USDT")
    row = df.iloc[0]
    assert row["weth_supply"] == 10.0
    assert row["weth_supply_usd"] == 20000.0
    assert row["usdt_borrow"] == 5000.0
    assert row["usdt_borrow_usd"] == 5000.0
    assert row["wallet_address"] == "0xaaa"
    assert row["emode_category"] == 0


def test_aggregates_reproduce_ba_semantics():
    # Mixed collateral incl. an LT=0 asset: it counts toward total collateral
    # but adds nothing to the LT / bonus numerators -- exactly how BA's rows
    # treat USDS-style collateral.
    rows = _user(
        supplies=[("WETH", 10.0, True), ("WSTETH", 5.0, True), ("USDS", 1000.0, True)],
        borrows=[("USDT", 10000.0)],
    )
    df = build_users_frame(rows, _RESERVES, _PRICES, "USDT")
    row = df.iloc[0]
    total = 20000.0 + 12000.0 + 1000.0
    lt = 20000.0 * 0.86 + 12000.0 * 0.84
    bonus = 20000.0 * 1.05 + 12000.0 * 1.07
    assert row["total_collateral_usd"] == total
    assert row["lltv"] == pytest.approx(lt / total)
    assert row["health_factor"] == pytest.approx(lt / 10000.0)
    assert row["liquidation_incentive"] == pytest.approx(bonus / total)
    assert row["ltv"] == pytest.approx(10000.0 / total)


def test_disabled_collateral_keeps_its_supply_but_protects_nothing():
    enabled = build_users_frame(
        _user(supplies=[("WETH", 10.0, True)], borrows=[("USDT", 1000.0)]), _RESERVES, _PRICES, "USDT"
    ).iloc[0]
    disabled = build_users_frame(
        _user(supplies=[("WETH", 10.0, False)], borrows=[("USDT", 1000.0)]), _RESERVES, _PRICES, "USDT"
    ).iloc[0]
    assert disabled["weth_supply_usd"] == enabled["weth_supply_usd"] == 20000.0
    assert enabled["health_factor"] > 0
    assert disabled["health_factor"] == 0.0


def test_users_not_borrowing_the_loan_token_are_excluded():
    rows = _user("0xaaa", supplies=[("WETH", 1.0, True)], borrows=[("USDT", 100.0)])
    rows += _user("0xbbb", supplies=[("WETH", 1.0, True)], borrows=[("DAI", 100.0)])
    df = build_users_frame(rows, _RESERVES, _PRICES, "USDT")
    assert list(df["wallet_address"]) == ["0xaaa"]


def test_loan_token_all_keeps_every_borrower():
    rows = _user("0xaaa", supplies=[("WETH", 1.0, True)], borrows=[("USDT", 100.0)])
    rows += _user("0xbbb", supplies=[("WETH", 1.0, True)], borrows=[("DAI", 100.0)])
    df = build_users_frame(rows, _RESERVES, _PRICES, "ALL")
    assert len(df) == 2


def test_an_unpriced_token_fails_the_build():
    rows = _user(supplies=[("WETH", 1.0, True), ("EXOTIC", 5.0, True)], borrows=[("USDT", 100.0)])
    with pytest.raises(ValueError, match="EXOTIC"):
        build_users_frame(rows, _RESERVES, _PRICES, "USDT")


def test_zero_collateral_borrowers_are_excluded_not_nan_poison():
    # A borrower with no live collateral is existing bad debt; every downstream
    # ratio divides by collateral, so keeping them turns the whole CRR into NaN
    # (found running against real staging data).
    rows = _user("0xaaa", supplies=[("WETH", 1.0, True)], borrows=[("USDT", 100.0)])
    rows += _user("0xdust", supplies=[], borrows=[("USDT", 2.63)])
    df = build_users_frame(rows, _RESERVES, _PRICES, "USDT")
    assert list(df["wallet_address"]) == ["0xaaa"]
    assert not df.isin([float("inf")]).any().any()


def test_no_borrowers_fails_rather_than_returning_an_empty_market():
    rows = _user(supplies=[("WETH", 1.0, True)], borrows=[("DAI", 100.0)])
    with pytest.raises(ValueError, match="no active borrowers"):
        build_users_frame(rows, _RESERVES, _PRICES, "USDT")


def test_market_frame_contains_only_modeled_collaterals():
    df = build_market_frame({"WETH", "WSTETH", "USDS"}, _PRICES)
    assert list(df["token_symbol"]) == ["WETH", "WSTETH"]  # USDS is not simulated
    assert list(df["oracle_price"]) == [2000.0, 2400.0]


def test_market_frame_fails_on_a_modeled_collateral_without_a_price():
    with pytest.raises(ValueError, match="WBTC"):
        build_market_frame({"WETH", "WBTC"}, _PRICES)
