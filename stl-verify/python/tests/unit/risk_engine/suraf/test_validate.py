from pathlib import Path

import pytest

from app.risk_engine.suraf.validate import SurafValidationError, validate_package


class TestValidatePackage:
    def test_happy_path(self, sample_package: Path) -> None:
        # Should not raise.
        validate_package(sample_package)

    def test_not_a_directory(self, tmp_path: Path) -> None:
        missing = tmp_path / "nope"
        with pytest.raises(SurafValidationError, match="not a directory"):
            validate_package(missing)

    def test_wrong_assessor_count(self, sample_package: Path) -> None:
        (sample_package / "Assessor_3_scores.csv").unlink()

        with pytest.raises(SurafValidationError, match="expected 3"):
            validate_package(sample_package)

    def test_missing_weights(self, sample_package: Path) -> None:
        (sample_package / "weights.csv").unlink()

        with pytest.raises(SurafValidationError, match="missing required file"):
            validate_package(sample_package)

    def test_missing_crr_mapping(self, sample_package: Path) -> None:
        (sample_package / "crr_mapping.csv").unlink()

        with pytest.raises(SurafValidationError, match="missing required file"):
            validate_package(sample_package)

    def test_missing_penalty(self, sample_package: Path) -> None:
        (sample_package / "penalty.csv").unlink()

        with pytest.raises(SurafValidationError, match="missing required file"):
            validate_package(sample_package)

    def test_dry_run_scoring_failure(self, sample_package: Path) -> None:
        # Clobber crr_mapping.csv so the scorer's read_csv raises on missing
        # required columns.
        (sample_package / "crr_mapping.csv").write_text("not,a,real,schema\n1,2,3,4\n")

        with pytest.raises(SurafValidationError, match="dry-run scoring failed"):
            validate_package(sample_package)
