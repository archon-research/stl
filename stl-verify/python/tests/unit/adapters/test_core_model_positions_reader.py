"""Unit tests for the positions frame assembly (the model-semantics core)."""

from types import SimpleNamespace

import pytest

from app.adapters.postgres.core_model_positions_reader import (
    PositionRow,
    build_market_frame,
    build_morpho_users_frame,
    build_price_map,
    build_users_frame,
    morpho_liquidation_incentive,
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


# Morpho Blue


def _morpho_row(address_bytes=b"\xaa" * 20, lltv=0.86, collateral=430_000, borrow_assets=260_000_000):
    """One (user, tranche) row as the SQL returns it: raw units, cbBTC 8 dp, USDC 6 dp."""
    return SimpleNamespace(
        user_address=address_bytes,
        lltv=lltv,
        collateral_symbol="cbBTC",
        collateral_decimals=8,
        loan_symbol="USDC",
        loan_decimals=6,
        collateral=collateral,
        borrow_assets=borrow_assets,
    )


_MORPHO_PRICES = {"CBBTC": 70000.0, "USDC": 1.0}


def test_morpho_lif_matches_ba_parquet_value():
    # BA's users_morpho_cbbtc-usdc.parquet carries 1.04384134 for lltv 0.86 —
    # exactly the whitepaper formula min(1.15, 1/(0.3*lltv + 0.7)).
    assert morpho_liquidation_incentive(0.86) == pytest.approx(1.04384134, abs=1e-8)


def test_morpho_frame_scales_decimals_and_prices():
    df = build_morpho_users_frame([_morpho_row()], _MORPHO_PRICES)
    row = df.iloc[0]
    assert row["cbbtc_supply"] == pytest.approx(0.0043)  # 430_000 / 1e8
    assert row["cbbtc_supply_usd"] == pytest.approx(301.0)
    assert row["usdc_borrow"] == pytest.approx(260.0)  # 260_000_000 / 1e6
    assert row["health_factor"] == pytest.approx(0.86 * 301.0 / 260.0)
    assert row["lltv"] == pytest.approx(0.86)
    assert row["wallet_address"] == "0x" + "aa" * 20


def test_morpho_wallet_across_two_tranches_collapses_to_one_weighted_row():
    rows = [
        _morpho_row(lltv=0.86, collateral=100_000_000, borrow_assets=10_000_000),  # 1 cbBTC
        _morpho_row(lltv=0.945, collateral=300_000_000, borrow_assets=30_000_000),  # 3 cbBTC
    ]
    df = build_morpho_users_frame(rows, _MORPHO_PRICES)
    assert len(df) == 1
    row = df.iloc[0]
    assert row["cbbtc_supply"] == pytest.approx(4.0)
    assert row["lltv"] == pytest.approx((0.86 * 1 + 0.945 * 3) / 4)  # collateral-weighted


def test_morpho_zero_collateral_borrowers_are_excluded():
    rows = [_morpho_row(), _morpho_row(address_bytes=b"\xbb" * 20, collateral=0)]
    df = build_morpho_users_frame(rows, _MORPHO_PRICES)
    assert list(df["wallet_address"]) == ["0x" + "aa" * 20]


def test_morpho_unpriced_token_fails_the_build():
    with pytest.raises(ValueError, match="CBBTC"):
        build_morpho_users_frame([_morpho_row()], {"USDC": 1.0})


def _price_row(symbol: str, price: float):
    return SimpleNamespace(symbol=symbol, price_usd=price)


def test_price_map_refuses_a_symbol_collision_on_a_consumed_symbol():
    rows = [_price_row("USDC", 1.0), _price_row("usdc", 0.5), _price_row("WETH", 2000.0)]
    with pytest.raises(ValueError, match="ambiguous token symbol.*USDC"):
        build_price_map(rows, used_symbols={"USDC", "WETH"})


def test_price_map_ignores_collisions_on_symbols_nothing_consumes():
    rows = [_price_row("USDC", 1.0), _price_row("usdc", 0.5), _price_row("WETH", 2000.0)]
    assert build_price_map(rows, used_symbols={"WETH"}) == {"USDC": 0.5, "WETH": 2000.0}
