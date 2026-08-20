"""Unit tests for change_user_ltvs (worst-case HF=1 collateral rescaling)."""

from unittest.mock import patch

import pandas as pd

from app.risk_engine.core_model.importer import change_user_ltvs


def _users_df(**overrides) -> pd.DataFrame:
    base = {
        "wallet_address": ["0xabc"],
        "total_borrow_usd": [800.0],
        "lltv": [0.8],
        "health_factor": [1.5],
        "weth_supply_usd": [1500.0],
        "weth_supply": [0.75],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def _market_df(symbols_prices: dict[str, float]) -> pd.DataFrame:
    return pd.DataFrame({"token_symbol": list(symbols_prices), "oracle_price": list(symbols_prices.values())})


def test_collateral_rescaled_to_borrow_over_lltv() -> None:
    """New total collateral = borrow / lltv, and quantities follow the oracle price."""
    result = change_user_ltvs(_users_df(), _market_df({"WETH": 2000.0}))

    # 800 / 0.8 = 1000 USD of collateral, all in the single WETH column.
    assert result.loc[0, "weth_supply_usd"] == 1000.0
    assert result.loc[0, "weth_supply"] == 1000.0 / 2000.0
    assert result.loc[0, "new_health_factor"] == 1.0


def test_multi_asset_collateral_keeps_original_weights() -> None:
    """Rescaled collateral is split across assets proportionally to the original mix."""
    users = _users_df(
        weth_supply_usd=[600.0],
        weth_supply=[0.3],
        wbtc_supply_usd=[1400.0],
        wbtc_supply=[0.02],
    )
    result = change_user_ltvs(users, _market_df({"WETH": 2000.0, "WBTC": 70000.0}))

    # 800 / 0.8 = 1000 total; original weights 0.3 / 0.7.
    assert result.loc[0, "weth_supply_usd"] == 300.0
    assert result.loc[0, "wbtc_supply_usd"] == 700.0
    assert result.loc[0, "weth_supply"] == 300.0 / 2000.0
    assert result.loc[0, "wbtc_supply"] == 700.0 / 70000.0


def test_zero_lltv_propagates_na_instead_of_raising() -> None:
    """lltv=0 rows divide by NA, so the rescaled collateral is NA rather than an error."""
    result = change_user_ltvs(_users_df(lltv=[0.0]), _market_df({"WETH": 2000.0}))

    assert pd.isna(result.loc[0, "weth_supply_usd"])
    assert pd.isna(result.loc[0, "weth_supply"])


def test_missing_oracle_price_zeroes_quantity_and_warns() -> None:
    """A supply column with no matching market row zeroes the quantity and logs a warning."""
    users = _users_df(
        hype_supply_usd=[500.0],
        hype_supply=[12.5],
    )
    with patch("app.risk_engine.core_model.importer.logger.warning") as warn:
        result = change_user_ltvs(users, _market_df({"WETH": 2000.0}))

    assert result.loc[0, "hype_supply"] == 0.0
    assert any("no oracle price for" in str(c.args[0]) and "HYPE" in c.args for c in warn.call_args_list)
