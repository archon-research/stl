import shutil
from pathlib import Path

import pytest

from app.risk_engine.suraf.validate import SurafValidationError, validate_version


class TestValidateVersion:
    def test_happy_path(self, sample_version: Path) -> None:
        validate_version(sample_version)

    def test_not_a_directory(self, tmp_path: Path) -> None:
        missing = tmp_path / "nope"
        with pytest.raises(SurafValidationError, match="not a directory"):
            validate_version(missing)

    def test_wrong_scorecard_count(self, sample_version: Path) -> None:
        (sample_version / "scorecards" / "Assessor_3_scores.csv").unlink()

        with pytest.raises(SurafValidationError, match="expected 3"):
            validate_version(sample_version)

    def test_scorecards_dir_missing(self, sample_version: Path) -> None:
        shutil.rmtree(sample_version / "scorecards")

        with pytest.raises(SurafValidationError, match="expected 3"):
            validate_version(sample_version)

    def test_missing_weights(self, sample_version: Path) -> None:
        (sample_version / "weights.csv").unlink()

        with pytest.raises(SurafValidationError, match="missing required file"):
            validate_version(sample_version)

    def test_missing_crr_mapping(self, sample_version: Path) -> None:
        (sample_version / "crr_mapping.csv").unlink()

        with pytest.raises(SurafValidationError, match="missing required file"):
            validate_version(sample_version)

    def test_missing_penalty(self, sample_version: Path) -> None:
        (sample_version / "penalty.csv").unlink()

        with pytest.raises(SurafValidationError, match="missing required file"):
            validate_version(sample_version)

    def test_scorecard_filenames_are_free_form(self, sample_version: Path) -> None:
        """Scorecard filenames don't have to follow any pattern — just count."""
        scorecards = sample_version / "scorecards"
        for i, src in enumerate(sorted(scorecards.glob("*.csv")), start=1):
            src.rename(scorecards / f"reviewer_alpha_{i}.csv")

        validate_version(sample_version)
