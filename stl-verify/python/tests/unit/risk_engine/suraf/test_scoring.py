"""End-to-end test for the vendored scorer.

Locks numerical output against the testdata sample rating so that any
upstream change in scoring behaviour (or a broken vendor copy) is caught
here rather than in the loader tests.
"""

from math import isclose
from pathlib import Path

from app.risk_engine.suraf.scoring import SURAFResults


def test_scorer_produces_expected_values(sample_version: Path) -> None:
    results = SURAFResults()
    results.run(
        weights_path=sample_version / "weights.csv",
        assessor_paths=sorted((sample_version / "scorecards").glob("*.csv")),
        crr_path=sample_version / "crr_mapping.csv",
        penalty_path=sample_version / "penalty.csv",
    )

    assert isclose(results.avg_score, 3.541628505214305, abs_tol=1e-12)
    assert isclose(results.unadjusted_crr, 13.666971958285561, abs_tol=1e-12)
    assert isclose(results.penalty, 20.0, abs_tol=1e-12)
    assert isclose(results.adjusted_crr, 33.66697195828556, abs_tol=1e-12)
    assert results.total_score_1 == 14
    assert results.total_scored == 96
