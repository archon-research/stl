"""Unit test for the calibrator's backtest plumbing (VEC-766 / Known Issue #3).

total_fitter must pass its own return-type and tail-probability conventions
through to every rolling hit_backtest call, instead of letting the backtest
run on defaults that can silently diverge from the fitted candidates.
"""

import numpy as np
import pandas as pd

import app.risk_engine.core_model.calibrator as calibrator_module
from app.risk_engine.core_model.backtester import Backtester
from app.risk_engine.core_model.calibrator import Calibrator


class _SequentialParallel:
    """In-process joblib.Parallel stand-in: loky workers would re-import the
    real Backtester and bypass the monkeypatched hit_backtest."""

    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, iterable):
        return [func(*args, **kwargs) for func, args, kwargs in iterable]


def test_total_fitter_passes_its_return_type_and_alpha_to_the_backtest(monkeypatch):
    rng = np.random.default_rng(0)
    prices = pd.Series(100.0 * np.exp(np.cumsum(rng.normal(0, 0.02, 30))), name="TKN")
    calibrator = Calibrator(price_series=prices, seed=0)
    calibrator.list_models = ["GARCH"]

    monkeypatch.setattr(Calibrator, "check_stationarity", staticmethod(lambda *a, **k: True))
    monkeypatch.setattr(Calibrator, "check_arch_effects", staticmethod(lambda *a, **k: True))
    fake_model = type("M", (), {"volatility": object()})()
    monkeypatch.setattr(Calibrator, "find_best_vol_model", lambda self, **_kw: (object(), fake_model, "Normal"))
    monkeypatch.setattr(Calibrator, "check_garch_residuals", staticmethod(lambda *a, **k: (True, None)))
    monkeypatch.setattr(calibrator_module, "Parallel", _SequentialParallel)

    recorded_kwargs: list[dict] = []

    def _record(self, i, **kwargs):
        recorded_kwargs.append(kwargs)
        return np.array([0])

    monkeypatch.setattr(Backtester, "hit_backtest", _record)

    calibrator.total_fitter(
        train_size=10,
        forecast_step=1,
        use_log_returns=True,
        use_arma_model=False,
        use_vol_model=True,
        backtest_alpha=0.03,
    )

    assert recorded_kwargs, "the rolling backtest never ran"
    assert all(kw == {"use_log_returns": True, "alpha": 0.03} for kw in recorded_kwargs)
