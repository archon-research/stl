"""Sampling error of the Monte Carlo CRR and whether a run has converged.

Pure math over the per-scenario losses the liquidator produces. The service
stores the diagnostics next to the CRR and logs the convergence verdict.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

# Loss above this share of total debt, in %, makes a scenario catastrophic.
CATASTROPHIC_LOSS_PCT = 1.0
# Relative standard error and catastrophic-draw count a run needs before its EL is trusted.
MAX_REL_SE = 0.25
MIN_CATASTROPHIC_SCENARIOS = 25


@dataclass(frozen=True)
class MonteCarloDiagnostics:
    n_scenarios: int
    n_loss_scenarios: int
    n_catastrophic_scenarios: int
    catastrophic_loss_pct: float
    crr_el_se_pct: float | None
    """Standard error of the EL in % of total debt; None below two scenarios."""
    crr_el_rel_se: float | None
    """Standard error over the EL; None when the EL is zero or has no standard error."""


def monte_carlo_diagnostics(net_bad_debt: np.ndarray, tot_debt: float) -> MonteCarloDiagnostics:
    """Sampling error of the CRR (EL) across independent scenarios: std(loss) / sqrt(N).

    Refuses a non-positive total debt or a non-finite loss instead of producing
    NaN: the row is stored as JSON, which cannot carry NaN, so the failure has
    to be named here rather than surface as a database syntax error.
    """
    losses = np.asarray(net_bad_debt, dtype=np.float64)
    if not tot_debt > 0:
        raise ValueError(f"total debt must be positive to express losses as a share of debt, got {tot_debt!r}")
    if not np.isfinite(losses).all():
        raise ValueError(f"{int((~np.isfinite(losses)).sum())} of {losses.size} scenario losses are NaN or infinite")

    loss_pct = losses / tot_debt * 100
    n = int(loss_pct.size)
    el = float(loss_pct.mean()) if n else 0.0
    se = float(loss_pct.std(ddof=1) / np.sqrt(n)) if n >= 2 else None
    return MonteCarloDiagnostics(
        n_scenarios=n,
        n_loss_scenarios=int((loss_pct > 0).sum()),
        n_catastrophic_scenarios=int((loss_pct > CATASTROPHIC_LOSS_PCT).sum()),
        catastrophic_loss_pct=CATASTROPHIC_LOSS_PCT,
        crr_el_se_pct=se,
        crr_el_rel_se=se / el if se is not None and el > 0 else None,
    )


def convergence_warnings(diagnostics: MonteCarloDiagnostics) -> list[str]:
    """Reasons the Monte Carlo EL should not be trusted yet; empty when it can be.

    A small standard error alone is not evidence of convergence. The EL is
    carried by rare catastrophic draws: before enough of them land, the running
    mean sits flat and tiny with a small SE, then jumps by 10x when one does.
    Hence the catastrophic-count check next to the relative-SE check.
    """
    reasons: list[str] = []
    if diagnostics.crr_el_se_pct is None:
        reasons.append(f"only {diagnostics.n_scenarios} scenario(s), so the standard error is undefined")
    elif diagnostics.crr_el_rel_se is None:
        reasons.append("no scenario lost anything, so the relative standard error is undefined")
    elif diagnostics.crr_el_rel_se > MAX_REL_SE:
        reasons.append(f"relative standard error {diagnostics.crr_el_rel_se:.0%} exceeds {MAX_REL_SE:.0%}")
    if diagnostics.n_catastrophic_scenarios < MIN_CATASTROPHIC_SCENARIOS:
        reasons.append(
            f"{diagnostics.n_catastrophic_scenarios} catastrophic scenarios "
            f"(loss > {diagnostics.catastrophic_loss_pct:g}% of debt), fewer than {MIN_CATASTROPHIC_SCENARIOS}"
        )
    return reasons
