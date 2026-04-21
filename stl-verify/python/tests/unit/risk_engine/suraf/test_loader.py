import shutil
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from app.risk_engine.suraf.loader import load_all_ratings
from app.risk_engine.suraf.result import SurafResult
from app.risk_engine.suraf.validate import SurafValidationError


class TestLoadAllRatings:
    def test_returns_result_for_each_rating(self, inputs_dir: Path) -> None:
        results = load_all_ratings(inputs_dir, source_commit_sha="sha123")

        assert set(results.keys()) == {"sample_rating"}
        result = results["sample_rating"]
        assert isinstance(result, SurafResult)
        assert result.rating_id == "sample_rating"
        assert result.version == "v1"
        assert result.source_commit_sha == "sha123"
        assert isinstance(result.loaded_at, datetime)

    def test_decimal_fields_are_decimals(self, inputs_dir: Path) -> None:
        result = load_all_ratings(inputs_dir, source_commit_sha="x")["sample_rating"]

        for value in (result.crr_pct, result.unadjusted_crr_pct, result.penalty_pp, result.avg_score):
            assert isinstance(value, Decimal)

    def test_crr_within_expected_range(self, inputs_dir: Path) -> None:
        result = load_all_ratings(inputs_dir, source_commit_sha="x")["sample_rating"]

        # Adjusted CRR is a percentage clamped to [0, 100].
        assert Decimal("0") <= result.crr_pct <= Decimal("100")
        # Scorer computes adjusted = min(unadj + penalty, 100) in float
        # arithmetic, then we stringify-convert to Decimal. Re-adding in
        # Decimal may drift in the last digit, so allow a small delta.
        expected = min(result.unadjusted_crr_pct + result.penalty_pp, Decimal("100"))
        assert abs(result.crr_pct - expected) < Decimal("1e-10")

    def test_picks_highest_numeric_version(self, inputs_dir: Path) -> None:
        """When a rating has multiple ``v{N}`` dirs, the largest N wins.

        Uses ``v2`` and ``v10`` specifically because lex sort would pick
        ``v2`` — the loader must parse the integer and sort numerically.
        """
        rating_dir = inputs_dir / "ratings" / "sample_rating"
        shutil.copytree(rating_dir / "v1", rating_dir / "v2")
        shutil.copytree(rating_dir / "v1", rating_dir / "v10")

        result = load_all_ratings(inputs_dir, source_commit_sha="x")["sample_rating"]
        assert result.version == "v10"

    def test_raises_on_invalid_version_name(self, inputs_dir: Path) -> None:
        rating_dir = inputs_dir / "ratings" / "sample_rating"
        (rating_dir / "v1").rename(rating_dir / "2026-04-20")

        with pytest.raises(SurafValidationError, match="invalid version directory name"):
            load_all_ratings(inputs_dir, source_commit_sha="x")

    def test_raises_when_ratings_dir_missing(self, tmp_path: Path) -> None:
        with pytest.raises(SurafValidationError, match="ratings directory not found"):
            load_all_ratings(tmp_path, source_commit_sha="x")

    def test_raises_when_rating_has_no_versions(self, tmp_path: Path) -> None:
        (tmp_path / "ratings" / "empty_rating").mkdir(parents=True)

        with pytest.raises(SurafValidationError, match="no version directories"):
            load_all_ratings(tmp_path, source_commit_sha="x")

    def test_raises_on_bad_version(self, inputs_dir: Path) -> None:
        (inputs_dir / "ratings" / "sample_rating" / "v1" / "weights.csv").unlink()

        with pytest.raises(SurafValidationError):
            load_all_ratings(inputs_dir, source_commit_sha="x")

    def test_raises_on_malformed_csv(self, inputs_dir: Path) -> None:
        """Shape checks pass but the scorer raises — loader wraps it as a
        SurafValidationError with package context."""
        (inputs_dir / "ratings" / "sample_rating" / "v1" / "crr_mapping.csv").write_text("not,a,real,schema\n1,2,3,4\n")

        with pytest.raises(SurafValidationError, match="scoring failed"):
            load_all_ratings(inputs_dir, source_commit_sha="x")

    def test_empty_ratings_dir_returns_empty_dict(self, tmp_path: Path) -> None:
        (tmp_path / "ratings").mkdir()
        assert load_all_ratings(tmp_path, source_commit_sha="x") == {}
