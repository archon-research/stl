"""File-shape validation for a single SURAF rating package.

A rating package is a directory containing exactly three assessor CSVs,
plus ``weights.csv``, ``crr_mapping.csv`` and ``penalty.csv``. Validation
fails fast with ``SurafValidationError`` on the first problem.

Scoring-correctness (unparseable cells, weight/score mismatches, etc.) is
caught by the loader when it actually scores the package, and by CI's
dry-run on PRs that add or change rating packages.
"""

from __future__ import annotations

from pathlib import Path

ASSESSOR_GLOB = "Assessor_*_scores.csv"
REQUIRED_ASSESSOR_COUNT = 3
WEIGHTS_FILE = "weights.csv"
CRR_MAPPING_FILE = "crr_mapping.csv"
PENALTY_FILE = "penalty.csv"


class SurafValidationError(Exception):
    """Raised when a SURAF rating package fails validation."""


def assessor_paths(path: Path) -> list[Path]:
    """Return the sorted assessor CSVs for a package, without validating count."""
    return sorted(path.glob(ASSESSOR_GLOB))


def validate_package(path: Path) -> None:
    """Validate a rating package directory. Raises ``SurafValidationError``."""
    if not path.is_dir():
        raise SurafValidationError(f"rating package not a directory: {path}")

    assessors = assessor_paths(path)
    if len(assessors) != REQUIRED_ASSESSOR_COUNT:
        raise SurafValidationError(
            f"expected {REQUIRED_ASSESSOR_COUNT} {ASSESSOR_GLOB} files in {path}, found {len(assessors)}"
        )

    for required in (path / WEIGHTS_FILE, path / CRR_MAPPING_FILE, path / PENALTY_FILE):
        if not required.is_file():
            raise SurafValidationError(f"missing required file: {required}")
