"""Parity test for the axis_synome-backed SURAF scoring path.

Locks the four float outputs of the scoring pipeline against the
testdata sample rating.  Any upstream change in axis_synome scoring
semantics (or a regression in our parser) is caught here rather than
in the loader tests.
"""

from math import isclose
from pathlib import Path

from app.risk_engine.suraf.loader import _score_version


def test_scoring_produces_expected_values(sample_version: Path) -> None:
    scored = _score_version(sample_version)

    assert isclose(scored.avg_score, 3.541628505214305, abs_tol=1e-12)
    assert isclose(scored.unadjusted_crr, 13.666971958285561, abs_tol=1e-12)
    assert isclose(scored.penalty, 20.0, abs_tol=1e-12)
    assert isclose(scored.adjusted_crr, 33.66697195828556, abs_tol=1e-12)
