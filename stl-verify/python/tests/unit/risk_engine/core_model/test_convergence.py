"""Unit tests for the Monte Carlo diagnostics and the convergence verdict."""

import numpy as np
import pytest

from app.risk_engine.core_model.convergence import (
    CATASTROPHIC_LOSS_PCT,
    MAX_REL_SE,
    MIN_CATASTROPHIC_SCENARIOS,
    MonteCarloDiagnostics,
    convergence_warnings,
    monte_carlo_diagnostics,
)

_TOT_DEBT = 1_000_000.0


def _diagnostics(
    *,
    n_scenarios: int = 10_000,
    se: float | None = 0.01,
    rel_se: float | None = 0.05,
    n_catastrophic: int = MIN_CATASTROPHIC_SCENARIOS,
) -> MonteCarloDiagnostics:
    return MonteCarloDiagnostics(
        n_scenarios=n_scenarios,
        n_loss_scenarios=120,
        n_catastrophic_scenarios=n_catastrophic,
        catastrophic_loss_pct=CATASTROPHIC_LOSS_PCT,
        crr_el_se_pct=se,
        crr_el_rel_se=rel_se,
    )


def test_standard_error_is_sample_std_over_sqrt_n():
    # Losses in % of debt: [0, 0, 0.5, 2.0, 0] -> mean 0.5, sample std 0.866025, SE 0.387298.
    diagnostics = monte_carlo_diagnostics(np.array([0.0, 0.0, 5_000.0, 20_000.0, 0.0]), _TOT_DEBT)

    assert diagnostics.crr_el_se_pct == pytest.approx(0.387298, abs=1e-6)
    assert diagnostics.crr_el_rel_se == pytest.approx(0.774597, abs=1e-6)


def test_scenario_counts_split_zero_loss_any_loss_and_catastrophic():
    catastrophic = _TOT_DEBT * (CATASTROPHIC_LOSS_PCT / 100) * 2
    losses = np.array([0.0, 100.0, catastrophic, 0.0])

    diagnostics = monte_carlo_diagnostics(losses, _TOT_DEBT)

    assert diagnostics.n_scenarios == 4
    assert diagnostics.n_loss_scenarios == 2
    assert diagnostics.n_catastrophic_scenarios == 1
    assert diagnostics.catastrophic_loss_pct == CATASTROPHIC_LOSS_PCT


def test_no_loss_in_any_scenario_leaves_the_relative_error_undefined():
    diagnostics = monte_carlo_diagnostics(np.zeros(50), _TOT_DEBT)

    assert diagnostics.crr_el_se_pct == 0.0
    assert diagnostics.crr_el_rel_se is None


def test_a_single_scenario_has_no_standard_error():
    diagnostics = monte_carlo_diagnostics(np.array([1_000.0]), _TOT_DEBT)

    assert diagnostics.crr_el_se_pct is None
    assert diagnostics.crr_el_rel_se is None


@pytest.mark.parametrize("tot_debt", [0.0, -1.0, float("nan")])
def test_a_non_positive_total_debt_is_refused(tot_debt):
    with pytest.raises(ValueError, match="total debt"):
        monte_carlo_diagnostics(np.array([1_000.0, 0.0]), tot_debt)


def test_a_non_finite_loss_is_refused():
    with pytest.raises(ValueError, match="NaN or infinite"):
        monte_carlo_diagnostics(np.array([1_000.0, float("nan")]), _TOT_DEBT)


@pytest.mark.parametrize("rel_se", [0.0, MAX_REL_SE])
def test_a_converged_run_raises_no_convergence_warning(rel_se):
    assert convergence_warnings(_diagnostics(rel_se=rel_se)) == []


@pytest.mark.parametrize(
    ("diagnostics", "fragment"),
    [
        (_diagnostics(rel_se=MAX_REL_SE * 2), "relative standard error"),
        (_diagnostics(rel_se=None), "no scenario lost anything"),
        (_diagnostics(n_scenarios=1, se=None, rel_se=None), "only 1 scenario"),
        (_diagnostics(n_catastrophic=MIN_CATASTROPHIC_SCENARIOS - 1), "catastrophic scenarios"),
    ],
)
def test_an_unconverged_run_names_the_reason(diagnostics, fragment):
    reasons = convergence_warnings(diagnostics)

    assert len(reasons) == 1
    assert fragment in reasons[0]


def test_every_failed_convergence_check_is_reported():
    reasons = convergence_warnings(_diagnostics(rel_se=MAX_REL_SE * 2, n_catastrophic=0))

    assert len(reasons) == 2
