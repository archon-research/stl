"""Unit tests for the VaR backtest statistics (VEC-766 / audit C-06, C-07).

Kupiec must reject boundary exceedance rates instead of returning p=1, and
the Christoffersen independence statistic must survive long series without
underflowing to p=1.
"""

import numpy as np
import pytest
from scipy.stats import chi2

from app.risk_engine.core_model.backtester import Backtester

ALPHA = 0.05


@pytest.mark.parametrize(
    ("hits", "expected_lr"),
    [
        pytest.param(np.zeros(1280, dtype=int), -2 * 1280 * np.log(1 - ALPHA), id="no-hit"),
        pytest.param(np.ones(100, dtype=int), -2 * 100 * np.log(ALPHA), id="all-hit"),
    ],
)
def test_kupiec_rejects_boundary_exceedance_rates(hits, expected_lr):
    lr, p_value = Backtester.kupiec_test(hits, ALPHA)

    assert lr == pytest.approx(expected_lr)
    assert p_value < 1e-12


def test_kupiec_exact_target_rate_is_a_perfect_pass():
    hits = np.zeros(1280, dtype=int)
    hits[:64] = 1  # 64/1280 = exactly the 5% target

    lr, p_value = Backtester.kupiec_test(hits, ALPHA)

    assert lr == 0.0
    assert p_value == 1.0


def test_kupiec_interior_case_matches_the_direct_formula():
    n, x = 100, 10
    hits = np.zeros(n, dtype=int)
    hits[:x] = 1
    pi_hat = x / n
    expected_lr = -2 * (
        (n - x) * np.log(1 - ALPHA) + x * np.log(ALPHA) - (n - x) * np.log(1 - pi_hat) - x * np.log(pi_hat)
    )

    lr, p_value = Backtester.kupiec_test(hits, ALPHA)

    assert lr == pytest.approx(expected_lr)
    assert p_value == pytest.approx(1 - chi2.cdf(expected_lr, df=1))


def test_christoffersen_rejects_clustered_hits_on_a_long_series():
    # 64 consecutive hits in 1280 observations: the textbook clustering case.
    # The pre-fix product formula underflowed both likelihoods past the 1e-10
    # clip on any series past ~440 observations and returned p=1 here.
    hits = np.zeros(1280, dtype=int)
    hits[600:664] = 1

    lr_ind, p_value_ind, _, p_value_cc = Backtester.christoffersen_test(hits, ALPHA)

    assert np.isfinite(lr_ind)
    assert p_value_ind < 1e-6
    assert p_value_cc < 1e-6


def test_christoffersen_accepts_independent_hits_on_a_long_series():
    rng = np.random.default_rng(0)
    hits = (rng.random(1280) < ALPHA).astype(int)

    lr_ind, p_value_ind, _, _ = Backtester.christoffersen_test(hits, ALPHA)

    assert np.isfinite(lr_ind)
    assert lr_ind >= 0.0
    assert p_value_ind > 0.05


def test_christoffersen_log_space_matches_the_product_formula_where_it_does_not_underflow():
    # Small series with every transition count nonzero, so the raw product
    # formula is exact and the log-space refactor must reproduce it.
    hits = np.array([0, 0, 1, 0, 1, 1, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0])
    n = len(hits)
    n00 = n01 = n10 = n11 = 0
    for t in range(1, n):
        n00 += hits[t - 1] == 0 and hits[t] == 0
        n01 += hits[t - 1] == 0 and hits[t] == 1
        n10 += hits[t - 1] == 1 and hits[t] == 0
        n11 += hits[t - 1] == 1 and hits[t] == 1
    pi0 = n01 / (n00 + n01)
    pi1 = n11 / (n10 + n11)
    pi = (n01 + n11) / (n - 1)
    l0 = ((1 - pi) ** (n00 + n10)) * (pi ** (n01 + n11))
    l1 = ((1 - pi0) ** n00) * (pi0**n01) * ((1 - pi1) ** n10) * (pi1**n11)
    expected_lr_ind = -2 * np.log(l0 / l1)

    lr_ind, _, _, _ = Backtester.christoffersen_test(hits, ALPHA)

    assert lr_ind == pytest.approx(expected_lr_ind)
