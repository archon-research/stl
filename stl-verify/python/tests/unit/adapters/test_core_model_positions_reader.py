"""Unit tests for the positions frame assembly (the model-semantics core)."""

from types import SimpleNamespace

import pytest

from app.adapters.postgres.core_model_positions_reader import (
    PositionRow,
    build_market_frame,
    build_morpho_users_frame,
    build_syrup_users_frame,
    build_users_frame,
    morpho_liquidation_incentive,
    supply_prices,
    syrup_attested_prices,
)

_PRICES = {"WETH": 2000.0, "WSTETH": 2400.0, "USDT": 1.0, "USDS": 1.0, "DAI": 1.0, "USDC": 1.0, "WBTC": 1.0}
_TOKEN_IDS = {"WETH": 1, "WSTETH": 2, "USDT": 3, "USDS": 4, "DAI": 5, "EXOTIC": 6, "USDC": 7, "WBTC": 8}
_RESERVES = {"WETH": (0.86, 1.05), "WSTETH": (0.84, 1.07), "USDS": (0.0, 0.0), "USDC": (0.0, 0.0), "WBTC": (0.75, 1.07)}


def _row(side, address, sym, qty, enabled, token_id=None):
    """A position row as the SQL returns it: priced by token id, or None when the oracle has no row."""
    return PositionRow(side, address, token_id or _TOKEN_IDS[sym], sym, qty, enabled, _PRICES.get(sym))


def _user(address="0xaaa", supplies=(), borrows=()):
    rows = [_row("supply", address, sym, qty, enabled) for sym, qty, enabled in supplies]
    rows += [_row("borrow", address, sym, qty, True) for sym, qty in borrows]
    return rows


def test_wide_columns_match_the_parquet_shape():
    rows = _user(supplies=[("WETH", 10.0, True)], borrows=[("USDT", 5000.0)])
    df = build_users_frame(rows, _RESERVES, "USDT")
    row = df.iloc[0]
    assert row["weth_supply"] == 10.0
    assert row["weth_supply_usd"] == 20000.0
    assert row["usdt_borrow"] == 5000.0
    assert row["usdt_borrow_usd"] == 5000.0
    assert row["wallet_address"] == "0xaaa"
    assert row["emode_category"] == 0


def test_aggregates_reproduce_ba_semantics():
    # Mixed collateral incl. an LT=0 asset: it is not collateral (total, lltv,
    # ltv exclude it) but it still dilutes the liquidation incentive, exactly
    # how BA's rows treat USDS-style supply.
    rows = _user(
        supplies=[("WETH", 10.0, True), ("WSTETH", 5.0, True), ("USDS", 1000.0, True)],
        borrows=[("USDT", 10000.0)],
    )
    df = build_users_frame(rows, _RESERVES, "USDT")
    row = df.iloc[0]
    collateral = 20000.0 + 12000.0
    supply = collateral + 1000.0
    lt = 20000.0 * 0.86 + 12000.0 * 0.84
    bonus = 20000.0 * 1.05 + 12000.0 * 1.07
    assert row["usds_supply_usd"] == 1000.0
    assert row["total_collateral_usd"] == collateral
    assert row["lltv"] == pytest.approx(lt / collateral)
    assert row["ltv"] == pytest.approx(10000.0 / collateral)
    assert row["health_factor"] == pytest.approx(lt / 10000.0)
    assert row["liquidation_incentive"] == pytest.approx(bonus / supply)


def test_totals_match_a_real_ba_row():
    # users_sparklend_dai.parquet: WETH 18,231 + WBTC 10,715 + USDC 40,079 supplied,
    # total_collateral_usd 28,946 and liquidation_incentive 0.4434.
    rows = _user(
        supplies=[("WETH", 18231.0 / 2000.0, True), ("WBTC", 10715.0, True), ("USDC", 40079.0, True)],
        borrows=[("DAI", 10000.0)],
    )
    row = build_users_frame(rows, _RESERVES, "DAI").iloc[0]
    assert row["total_collateral_usd"] == pytest.approx(28946.0)
    assert row["liquidation_incentive"] == pytest.approx(0.4434, abs=1e-4)


def test_disabled_collateral_keeps_its_supply_but_is_not_collateral():
    row = build_users_frame(
        _user(supplies=[("WETH", 10.0, False), ("WSTETH", 5.0, True)], borrows=[("USDT", 1000.0)]), _RESERVES, "USDT"
    ).iloc[0]
    assert row["weth_supply_usd"] == 20000.0
    assert row["total_collateral_usd"] == 12000.0
    assert row["lltv"] == pytest.approx(0.84)
    assert row["liquidation_incentive"] == pytest.approx(12000.0 * 1.07 / 32000.0)


def test_a_borrower_whose_only_collateral_is_disabled_is_excluded():
    rows = _user("0xaaa", supplies=[("WETH", 1.0, True)], borrows=[("USDT", 100.0)])
    rows += _user("0xbbb", supplies=[("WETH", 10.0, False)], borrows=[("USDT", 1000.0)])
    df = build_users_frame(rows, _RESERVES, "USDT")
    assert list(df["wallet_address"]) == ["0xaaa"]


def test_users_not_borrowing_the_loan_token_are_excluded():
    rows = _user("0xaaa", supplies=[("WETH", 1.0, True)], borrows=[("USDT", 100.0)])
    rows += _user("0xbbb", supplies=[("WETH", 1.0, True)], borrows=[("DAI", 100.0)])
    df = build_users_frame(rows, _RESERVES, "USDT")
    assert list(df["wallet_address"]) == ["0xaaa"]


def test_loan_token_all_keeps_every_borrower():
    rows = _user("0xaaa", supplies=[("WETH", 1.0, True)], borrows=[("USDT", 100.0)])
    rows += _user("0xbbb", supplies=[("WETH", 1.0, True)], borrows=[("DAI", 100.0)])
    df = build_users_frame(rows, _RESERVES, "ALL")
    assert len(df) == 2


def test_an_unpriced_token_fails_the_build():
    rows = _user(supplies=[("WETH", 1.0, True), ("EXOTIC", 5.0, True)], borrows=[("USDT", 100.0)])
    with pytest.raises(ValueError, match="EXOTIC"):
        build_users_frame(rows, _RESERVES, "USDT")


def test_two_distinct_tokens_sharing_a_held_symbol_are_refused():
    # The wide frame keys columns by symbol; a spoofed second "WETH" would
    # silently merge into the real one's column, so it is refused instead.
    rows = _user(supplies=[("WETH", 1.0, True)], borrows=[("USDT", 100.0)])
    rows.append(_row("supply", "0xbbb", "WETH", 1.0, True, token_id=42))
    with pytest.raises(ValueError, match=r"WETH.*more than one distinct token"):
        build_users_frame(rows, _RESERVES, "USDT")


def test_zero_collateral_borrowers_are_excluded_not_nan_poison():
    # A borrower with no live collateral is existing bad debt; every downstream
    # ratio divides by collateral, so keeping them turns the whole CRR into NaN
    # (found running against real staging data).
    rows = _user("0xaaa", supplies=[("WETH", 1.0, True)], borrows=[("USDT", 100.0)])
    rows += _user("0xdust", supplies=[], borrows=[("USDT", 2.63)])
    df = build_users_frame(rows, _RESERVES, "USDT")
    assert list(df["wallet_address"]) == ["0xaaa"]
    assert not df.isin([float("inf")]).any().any()


def test_no_borrowers_fails_rather_than_returning_an_empty_market():
    rows = _user(supplies=[("WETH", 1.0, True)], borrows=[("DAI", 100.0)])
    with pytest.raises(ValueError, match="no active borrowers"):
        build_users_frame(rows, _RESERVES, "USDT")


def test_supply_prices_cover_only_the_priced_supplied_tokens():
    rows = _user(supplies=[("WETH", 1.0, True), ("EXOTIC", 1.0, True)], borrows=[("USDT", 5.0)])
    assert supply_prices(rows) == {"WETH": 2000.0}


def test_market_frame_contains_only_modeled_collaterals():
    df = build_market_frame({"WETH", "WSTETH", "USDS"}, _PRICES)
    assert list(df["token_symbol"]) == ["WETH", "WSTETH"]  # USDS is not simulated
    assert list(df["oracle_price"]) == [2000.0, 2400.0]


def test_market_frame_fails_on_a_modeled_collateral_without_a_price():
    with pytest.raises(ValueError, match="LBTC"):
        build_market_frame({"WETH", "LBTC"}, _PRICES)


# Morpho Blue


def _morpho_row(
    address_bytes=b"\xaa" * 20,
    lltv=0.86,
    collateral=430_000,
    borrow_assets=260_000_000,
    collateral_price=70000.0,
    loan_price=1.0,
):
    """One (user, tranche) row as the SQL returns it: raw units, cbBTC 8 dp, USDC 6 dp, prices by token id."""
    return SimpleNamespace(
        user_address=address_bytes,
        lltv=lltv,
        collateral_symbol="cbBTC",
        collateral_decimals=8,
        collateral_price=collateral_price,
        loan_symbol="USDC",
        loan_decimals=6,
        loan_price=loan_price,
        collateral=collateral,
        borrow_assets=borrow_assets,
    )


def test_morpho_lif_matches_ba_parquet_value():
    # BA's users_morpho_cbbtc-usdc.parquet carries 1.04384134 for lltv 0.86 —
    # exactly the whitepaper formula min(1.15, 1/(0.3*lltv + 0.7)).
    assert morpho_liquidation_incentive(0.86) == pytest.approx(1.04384134, abs=1e-8)


def test_morpho_frame_scales_decimals_and_prices():
    df = build_morpho_users_frame([_morpho_row()])
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
    df = build_morpho_users_frame(rows)
    assert len(df) == 1
    row = df.iloc[0]
    assert row["cbbtc_supply"] == pytest.approx(4.0)
    assert row["lltv"] == pytest.approx((0.86 * 1 + 0.945 * 3) / 4)  # collateral-weighted


def test_morpho_zero_collateral_borrowers_are_excluded():
    rows = [_morpho_row(), _morpho_row(address_bytes=b"\xbb" * 20, collateral=0)]
    df = build_morpho_users_frame(rows)
    assert list(df["wallet_address"]) == ["0x" + "aa" * 20]


def test_morpho_unpriced_token_fails_the_build():
    with pytest.raises(ValueError, match="CBBTC"):
        build_morpho_users_frame([_morpho_row(collateral_price=None)])


# Syrup (Maple)


def _syrup_row(
    address_hex="aa" * 20,
    principal=25_000_000 * 10**6,
    acm_ratio=None,
    symbol="BTC",
    amount=505 * 10**8,
    decimals=8,
    value_usd=int(78276.425 * 10**8),
    liquidation_level=1_111_111,
):
    """One loan row as the SQL returns it: raw units, ratios in fixed-point x1e6 / x1e8."""
    return SimpleNamespace(
        borrower_address=bytes.fromhex(address_hex),
        principal_owed=principal,
        acm_ratio=acm_ratio,
        synced_at=None,
        asset_symbol=symbol,
        asset_amount=amount,
        asset_decimals=decimals,
        asset_value_usd=value_usd,
        liquidation_level=liquidation_level,
    )


def test_syrup_row_reproduces_a_real_ba_parquet_row():
    # users_syrup_usdc.parquet row 0x198aec...: 100M USDC against 2627.055713 BTC
    # at Maple's attested 71,809.935 — lltv 0.83001, ltv 0.530086, hf 1.565803,
    # liquidation_incentive 1.02. The staging loan of that borrower carries
    # liquidation_level 1204800, whose inverse is that exact lltv.
    row = build_syrup_users_frame(
        [
            _syrup_row(
                principal=100_000_000 * 10**6,
                amount=262_705_571_300,  # 2627.055713 BTC in 8 dp
                value_usd=int(71809.935 * 10**8),
                liquidation_level=1_204_800,
            )
        ],
        "USDC",
        6,
    ).iloc[0]
    assert row["lltv"] == pytest.approx(0.83001, abs=1e-5)
    assert row["ltv"] == pytest.approx(0.530086, abs=1e-6)
    assert row["health_factor"] == pytest.approx(1.565803, abs=1e-5)
    assert row["liquidation_incentive"] == pytest.approx(1.02)
    assert row["btc_supply"] == pytest.approx(2627.055713)
    assert row["btc_supply_usd"] == pytest.approx(188_648_700, rel=1e-6)
    assert row["usdc_borrow"] == row["usdc_borrow_usd"] == pytest.approx(100_000_000)
    assert row["total_borrow_usd"] == pytest.approx(100_000_000)
    assert row["total_collateral_usd"] == pytest.approx(row["btc_supply_usd"])
    assert row["wallet_address"] == "0x" + "aa" * 20


def test_syrup_keeps_one_row_per_loan_so_wallets_repeat():
    df = build_syrup_users_frame([_syrup_row(), _syrup_row()], "USDC", 6)
    assert list(df["wallet_address"]) == ["0x" + "aa" * 20] * 2


def test_syrup_par_coverage_trigger_loans_are_excluded(caplog):
    # liquidation_level <= 1e6 means the loan is margin-called at/above full
    # coverage (stable-on-stable terms): no price risk to simulate, and its
    # LT >= 1 would break the liquidator's -1 + LT*(1+bonus) < 0 guard.
    rows = [
        _syrup_row(),
        _syrup_row(address_hex="bb" * 20, symbol="PYUSD", value_usd=10**8, liquidation_level=900_000),
        _syrup_row(address_hex="cc" * 20, symbol="USDC", value_usd=10**8, liquidation_level=1_000_000),
    ]
    df = build_syrup_users_frame(rows, "USDC", 6)
    assert list(df["wallet_address"]) == ["0x" + "aa" * 20]
    assert (df["lltv"] < 1.0).all()
    assert "at/above par coverage" in caplog.text


@pytest.mark.parametrize(
    "broken",
    [
        dict(symbol=None, amount=None, decimals=8, value_usd=None, liquidation_level=None),  # no collateral row
        dict(symbol=""),
        dict(amount=None),
        dict(amount=0),
        dict(value_usd=None),
        dict(liquidation_level=None),
    ],
)
def test_syrup_loans_without_usable_collateral_are_excluded(broken, caplog):
    rows = [_syrup_row(), _syrup_row(address_hex="bb" * 20, **broken)]
    df = build_syrup_users_frame(rows, "USDC", 6)
    assert list(df["wallet_address"]) == ["0x" + "aa" * 20]
    assert "no usable collateral" in caplog.text


def test_syrup_all_loans_excluded_fails_rather_than_an_empty_market():
    with pytest.raises(ValueError, match="no simulatable external Active syrup loans"):
        build_syrup_users_frame([_syrup_row(liquidation_level=900_000)], "USDC", 6)


def test_syrup_two_attested_prices_for_one_symbol_are_refused():
    rows = [_syrup_row(), _syrup_row(address_hex="bb" * 20, value_usd=int(78277.0 * 10**8))]
    with pytest.raises(ValueError, match="two prices for BTC"):
        build_syrup_users_frame(rows, "USDC", 6)


def test_syrup_acm_disagreement_warns(caplog):
    # Computed coverage: 505 * 78276.425 / 25M = 1.5812; an acm_ratio of 2.0
    # is a >1% disagreement and must be surfaced.
    build_syrup_users_frame([_syrup_row(acm_ratio=2_000_000)], "USDC", 6)
    assert "acm_ratio" in caplog.text


def test_syrup_acm_agreement_stays_quiet(caplog):
    build_syrup_users_frame([_syrup_row(acm_ratio=1_581_184)], "USDC", 6)
    assert "acm_ratio" not in caplog.text


def test_syrup_attested_prices_cover_only_valued_symbols():
    rows = [_syrup_row(), _syrup_row(address_hex="bb" * 20, symbol="XRP", value_usd=None)]
    assert syrup_attested_prices(rows) == pytest.approx({"BTC": 78276.425})


def test_syrup_price_disagreement_on_an_excluded_stable_does_not_fail_the_market():
    # Two PYUSD loans attested a hair apart would otherwise fail the whole
    # market over a symbol the frame never uses (their loans are excluded).
    rows = [
        _syrup_row(),
        _syrup_row(address_hex="bb" * 20, symbol="PYUSD", value_usd=99982038, liquidation_level=900_000),
        _syrup_row(address_hex="cc" * 20, symbol="PYUSD", value_usd=99982040, liquidation_level=900_000),
    ]
    assert syrup_attested_prices(rows) == pytest.approx({"BTC": 78276.425})
    assert len(build_syrup_users_frame(rows, "USDC", 6)) == 1
