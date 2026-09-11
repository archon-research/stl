"""Unit tests for the Forecaster's innovation dispatch (VEC-765 / audit C-05).

The copula hands each token uniforms in (0,1); the Forecaster must map them to
innovations through the *fitted* GARCH distribution's own ppf. Upstream's
dispatch sent every fit to the Gaussian branch (name-prefix check that arch's
"Standardized …" names never match), and its t-branch used scipy's
unstandardized t. These tests pin both layers of the fix.
"""

from typing import Literal, cast

import numpy as np
import pandas as pd
import pytest
from arch import arch_model
from arch.univariate.base import ARCHModelResult
from scipy.stats import norm
from scipy.stats import t as scipy_t

from app.risk_engine.core_model.forecaster import Forecaster, Simulator

_U = np.linspace(0.01, 0.99, 25)


def _fit_garch(dist: Literal["normal", "t", "skewt"]):
    rng = np.random.default_rng(0)
    y = rng.standard_t(5, 400) * 1.2
    return arch_model(y, p=1, q=1, dist=dist, rescale=False).fit(disp="off", update_freq=0)


@pytest.mark.parametrize("dist", ["normal", "t", "skewt"])
def test_innovations_come_from_the_fitted_distribution(dist):
    garch = _fit_garch(dist)
    forecaster = Forecaster(arma_model=None, garch_model=garch, seed=0)

    innovations = forecaster._innovations_from_uniform(_U)

    d = garch.model.distribution
    n_shape = d.num_params
    shape = np.asarray(garch.params)[-n_shape:] if n_shape else np.empty(0)
    np.testing.assert_array_equal(innovations, np.asarray(d.ppf(_U, shape)))


def test_student_t_innovations_are_not_gaussian():
    garch = _fit_garch("t")
    forecaster = Forecaster(arma_model=None, garch_model=garch, seed=0)

    innovations = forecaster._innovations_from_uniform(_U)

    # Layer 1 of the bug: the dispatch fell through to the Normal branch.
    assert not np.allclose(innovations, norm.ppf(_U))
    # A standardized t has fatter tails than the Normal at the same quantile.
    assert innovations[-1] > norm.ppf(_U[-1])


def test_student_t_innovations_are_variance_standardized():
    garch = _fit_garch("t")
    forecaster = Forecaster(arma_model=None, garch_model=garch, seed=0)

    innovations = forecaster._innovations_from_uniform(_U)

    # Layer 2 of the bug: upstream's own t-branch used scipy's raw t.ppf,
    # whose variance is nu/(nu-2), not 1 — GARCH innovations must be unit-variance.
    nu = float(garch.params["nu"])
    raw_t = scipy_t.ppf(_U, df=nu)
    assert not np.allclose(innovations, raw_t)
    np.testing.assert_allclose(innovations, raw_t / np.sqrt(nu / (nu - 2)), rtol=1e-9)


def test_skew_t_innovations_use_the_fitted_skew():
    garch = _fit_garch("skewt")
    forecaster = Forecaster(arma_model=None, garch_model=garch, seed=0)

    innovations = forecaster._innovations_from_uniform(_U)

    # A skew-t with lambda != 0 is asymmetric: the two tails differ in size,
    # which a symmetric-t collapse (upstream's only non-Gaussian path) loses.
    assert abs(float(garch.params["lambda"])) > 1e-6
    assert not np.isclose(abs(innovations[0]), abs(innovations[-1]))


def test_returns_forecasting_uses_the_fitted_distribution_end_to_end():
    garch = _fit_garch("t")
    forecaster = Forecaster(arma_model=None, garch_model=garch, seed=0)
    prices = pd.Series(np.linspace(100.0, 110.0, 50))
    u = pd.Series(np.full(4, 0.99))

    combined, vol_forecast, _ = forecaster.returns_forecasting(
        step=4, prices=prices, correlated_uniform=u, jump_params=None, use_log_return=True
    )

    expected_innovation = forecaster._innovations_from_uniform(np.full(4, 0.99))
    np.testing.assert_allclose(combined.values, vol_forecast.values * expected_innovation, rtol=1e-12)


def test_missing_distribution_raises_instead_of_gaussian_fallback():
    garch = _fit_garch("t")

    class _NoDistModel:
        pass

    class _BrokenResult:
        model = _NoDistModel()
        params = garch.params

        def forecast(self, horizon):
            return garch.forecast(horizon=horizon)

    # cast: deliberately a foreign object — the test pins the fail-loud path.
    forecaster = Forecaster(arma_model=None, garch_model=cast(ARCHModelResult, _BrokenResult()), seed=0)
    prices = pd.Series(np.linspace(100.0, 110.0, 50))

    with pytest.raises(ValueError, match="no innovation distribution"):
        forecaster.returns_forecasting(
            step=2, prices=prices, correlated_uniform=pd.Series([0.5, 0.5]), jump_params=None, use_log_return=True
        )


@pytest.mark.parametrize(
    "grid_name",
    ["standardized student's t", "standardized skew student's t", "normal"],
)
def test_refit_preserves_the_calibrated_distribution(grid_name):
    """The Simulator refits the grid winner's spec; the innovation distribution
    must survive the calibrator -> spec dict -> refit round-trip, or the fixed
    dispatch would draw from the wrong distribution anyway."""
    rng = np.random.default_rng(1)
    prices = pd.Series(100.0 * np.exp(np.cumsum(rng.standard_t(5, 300) * 0.02)))
    garch_spec = {"dist": grid_name, "vol": "GARCH", "p": 1, "o": 0, "q": 1}

    simulator = Simulator(prices, arima_spec=None, garch_spec=garch_spec, seed=0)
    _, garch_refit, _ = simulator.arma_garch_refitter(train_size=250, use_log_returns=True)

    assert garch_refit.model.distribution.name.lower() == grid_name
