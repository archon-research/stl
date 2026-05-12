"""Unit tests for the SURAF CSV -> axis_synome entity loader."""

from __future__ import annotations

from pathlib import Path

import pytest
from axis_synome.spec.suraf.entities.assessor_score import AssessorScore
from axis_synome.spec.suraf.entities.mappings import CRRMapping, PenaltyMapping

from app.risk_engine.suraf.parsing import ParsedRating, load_version
from app.risk_engine.suraf.validate import SurafValidationError


class TestLoadVersion:
    def test_returns_parsed_rating_for_sample(self, sample_version: Path) -> None:
        parsed = load_version(sample_version)

        assert isinstance(parsed, ParsedRating)
        assert len(parsed.assessors) == 3
        assert all(isinstance(a, AssessorScore) for a in parsed.assessors)
        assert isinstance(parsed.crr_mapping, CRRMapping)
        assert isinstance(parsed.penalty_mapping, PenaltyMapping)

    def test_assessor_pillar_weights_match_weights_csv(self, sample_version: Path) -> None:
        """Pillar weights are absolute integers from weights.csv (40, 20, 10, 20, 10)."""
        parsed = load_version(sample_version)

        for assessor in parsed.assessors:
            pillar_weights = sorted(pw for pw, _ in assessor.scores)
            assert pillar_weights == [10, 10, 20, 20, 40]

    def test_assessor_skips_blank_scores(self, sample_version: Path) -> None:
        """Sample data has blank rows (e.g. 1.F, 4.D); they must be filtered out."""
        parsed = load_version(sample_version)

        for assessor in parsed.assessors:
            for _, sections in assessor.scores:
                for section_weight, score in sections:
                    assert section_weight > 0
                    assert 1 <= score <= 5

    def test_crr_mapping_has_breakpoints(self, sample_version: Path) -> None:
        parsed = load_version(sample_version)

        scores = [s for s, _ in parsed.crr_mapping.table]
        assert scores == sorted(scores)
        assert len(scores) >= 2

    def test_penalty_mapping_has_breakpoints(self, sample_version: Path) -> None:
        parsed = load_version(sample_version)

        ns = [n for n, _ in parsed.penalty_mapping.table]
        assert ns == sorted(ns)
        assert all(n >= 0 for n in ns)
        assert len(ns) >= 2

    def test_raises_on_missing_score_column(self, sample_version: Path) -> None:
        scorecard = next((sample_version / "scorecards").glob("*.csv"))
        scorecard.write_text("pillar,subsection_ref,title\n1,1.A,Foo\n")

        with pytest.raises(SurafValidationError, match="missing required column"):
            load_version(sample_version)

    def test_raises_on_unparseable_score(self, sample_version: Path) -> None:
        scorecard = next((sample_version / "scorecards").glob("*.csv"))
        scorecard.write_text("pillar,subsection_ref,title,score\n1,1.A,Foo,not-a-number\n")

        with pytest.raises(SurafValidationError, match="cannot parse score"):
            load_version(sample_version)

    def test_raises_on_score_out_of_range(self, sample_version: Path) -> None:
        scorecard = next((sample_version / "scorecards").glob("*.csv"))
        scorecard.write_text("pillar,subsection_ref,title,score\n1,1.A,Foo,9\n")

        with pytest.raises(SurafValidationError, match="outside"):
            load_version(sample_version)

    def test_raises_on_crr_mapping_not_strictly_increasing(self, sample_version: Path) -> None:
        (sample_version / "crr_mapping.csv").write_text("score,crr\n2.0,40\n1.0,80\n")

        with pytest.raises(SurafValidationError, match="strictly increasing"):
            load_version(sample_version)

    def test_raises_on_negative_n_score_1(self, sample_version: Path) -> None:
        (sample_version / "penalty.csv").write_text("n_score_1,penalty_pp\n-1,0\n5,10\n")

        with pytest.raises(SurafValidationError, match="non-negative"):
            load_version(sample_version)

    def test_raises_on_missing_crr_column(self, sample_version: Path) -> None:
        (sample_version / "crr_mapping.csv").write_text("score,not_crr\n1.0,100\n5.0,5\n")

        with pytest.raises(SurafValidationError, match="missing required column"):
            load_version(sample_version)
