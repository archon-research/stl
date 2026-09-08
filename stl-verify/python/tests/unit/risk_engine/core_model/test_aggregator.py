"""Unit tests for the copula Aggregator's correlation-matrix conditioning.

The live SparkLend oracle prices cbBTC, LBTC and tBTC off one BTC feed, so their
GARCH residual columns are identical and the Spearman correlation matrix is
exactly singular. Cholesky needs strictly positive definite, so the Aggregator
must regularize that case, not only a negative-eigenvalue one.
"""

import numpy as np
import pandas as pd
import pytest

from app.risk_engine.core_model.aggregator import Aggregator


def _residuals(*, n_rows: int = 200, duplicate: tuple[str, str] | None = None, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    frame = pd.DataFrame(
        rng.standard_normal((n_rows, 4)),
        columns=["WETH", "WBTC", "CBBTC", "RETH"],
    )
    if duplicate is not None:
        source, copy = duplicate
        frame[copy] = frame[source]
    return frame


def test_well_conditioned_residuals_keep_their_spearman_correlation():
    residuals = _residuals()
    rho = residuals.corr(method="spearman").sort_index(axis=0).sort_index(axis=1)
    expected = (2 * np.sin(np.pi * rho / 6)).to_numpy(copy=True)
    np.fill_diagonal(expected, 1.0)

    aggregator = Aggregator(residuals)

    assert list(aggregator.corr_matrix.columns) == list(rho.columns)
    np.testing.assert_allclose(aggregator.corr_matrix.to_numpy(), expected)


def test_identical_residual_columns_yield_a_positive_definite_matrix():
    aggregator = Aggregator(_residuals(duplicate=("WBTC", "CBBTC")))

    assert Aggregator.is_positive_definite(aggregator.corr_matrix)
    np.linalg.cholesky(aggregator.corr_matrix.to_numpy())  # must not raise


def test_regularization_keeps_the_matrix_a_correlation_matrix():
    aggregator = Aggregator(_residuals(duplicate=("WBTC", "CBBTC")))
    values = aggregator.corr_matrix.to_numpy()

    np.testing.assert_allclose(np.diag(values), 1.0)
    np.testing.assert_allclose(values, values.T)
    assert np.all(np.abs(values) <= 1.0 + 1e-12)
    # The duplicated pair stays (almost) perfectly correlated — regularization
    # must repair conditioning, not rewrite the dependence structure.
    assert aggregator.corr_matrix.loc["WBTC", "CBBTC"] > 0.999


@pytest.mark.parametrize("copula_type", ["t", "gaussian"])
def test_copula_samples_generate_for_identical_residual_columns(copula_type):
    aggregator = Aggregator(_residuals(duplicate=("WBTC", "CBBTC")), seed=1)

    samples = aggregator.copula_aggregator(copula_type, n_sims=8, forecasted_step=3)

    assert set(samples) == {"WETH", "WBTC", "CBBTC", "RETH"}
    for frame in samples.values():
        assert frame.shape == (8, 3)
        assert ((frame > 0) & (frame < 1)).all().all()


def test_is_positive_definite_rejects_a_singular_matrix_is_psd_accepts():
    singular = pd.DataFrame([[1.0, 1.0], [1.0, 1.0]], index=["A", "B"], columns=["A", "B"])

    assert Aggregator.is_psd(singular)
    assert not Aggregator.is_positive_definite(singular)
