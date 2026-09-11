"""Unit test for the runner's per-token jump-params wiring (VEC-768 / audit C-08).

Upstream calibrated jumps per token, reassigned one shared variable each
iteration, and never populated the per-token entry the simulator reads — so
every token simulated with the last token's jumps. This drives the real
pipeline loop over two tokens (calibration and simulation stubbed) and pins
that each token's entry carries the dict its own calibration returned.
"""

import numpy as np
import pandas as pd
import pytest

from app.risk_engine.core_model.calibrator import Calibrator
from app.risk_engine.core_model.config import load_params
from app.risk_engine.core_model.forecaster import Simulator
from app.risk_engine.core_model.runner import CoreModelConfig, run


class _StopAtSimulation(Exception):
    """Raised by the stubbed simulator so the test never reaches the liquidator."""


class _FakeReader:
    def __init__(self, prices_df: pd.DataFrame):
        self._prices_df = prices_df

    async def get_protocol_data(
        self, protocol: str, network: str, morpho_market: str, loan_token: str, galaxy_type: str
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        users_df = pd.DataFrame({"total_borrow_usd": [5_000.0, 7_000.0]})
        market_df = pd.DataFrame({"token_symbol": ["AAA", "BBB"]})
        return users_df, market_df

    async def get_prices(self, collateral_list: list[str]) -> pd.DataFrame:
        return self._prices_df

    async def get_orderbooks(self, collateral_list: list[str]) -> dict[str, pd.DataFrame]:
        raise AssertionError("the stubbed simulator stops the pipeline before order books are read")


async def test_each_token_entry_carries_its_own_calibrated_jump_params(monkeypatch, tmp_path):
    rng = np.random.default_rng(0)
    prices_df = pd.DataFrame(
        {
            "AAA": 100.0 * np.exp(np.cumsum(rng.normal(0, 0.01, 60))),
            "BBB": 50.0 * np.exp(np.cumsum(rng.normal(0, 0.03, 60))),
        }
    )

    monkeypatch.setattr(
        Calibrator,
        "total_fitter",
        lambda self, **_kw: (None, object(), None, {"dist": "normal", "vol": "GARCH", "p": 1, "o": 0, "q": 1}),
    )
    monkeypatch.setattr(Simulator, "arma_garch_refitter", lambda self, *_a: (None, None, pd.Series(np.zeros(10))))
    # Tag each calibration with the series it was fitted on, so the assertion
    # can tell whose params ended up in whose entry.
    monkeypatch.setattr(
        Calibrator,
        "fit_poisson_intensity",
        staticmethod(lambda hist_series, **_kw: {"fitted_on_mean": float(hist_series.mean())}),
    )

    captured: dict = {}

    def _capture(*, result_per_token, **_kw):
        captured.update(result_per_token)
        raise _StopAtSimulation

    monkeypatch.setattr(Simulator, "simulate_prices", staticmethod(_capture))

    params = load_params(overrides={"PROTOCOL": "MORPHO", "N_MC": 5})
    assert params["JUMPS"] is True

    with pytest.raises(_StopAtSimulation):
        await run(CoreModelConfig(market_key="test", params=params), _FakeReader(prices_df), tmp_path)

    returns_aaa = np.log(prices_df["AAA"] / prices_df["AAA"].shift(1)).dropna()
    returns_bbb = np.log(prices_df["BBB"] / prices_df["BBB"].shift(1)).dropna()
    assert captured["AAA"]["jump_params"]["fitted_on_mean"] == float(returns_aaa.mean())
    assert captured["BBB"]["jump_params"]["fitted_on_mean"] == float(returns_bbb.mean())
    assert captured["AAA"]["jump_params"] != captured["BBB"]["jump_params"]
