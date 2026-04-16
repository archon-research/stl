"""Validation for a single SURAF rating package.

A rating package is a directory containing exactly three assessor CSVs,
plus ``weights.csv``, ``crr_mapping.csv`` and ``penalty.csv``. Validation
fails fast with ``SurafValidationError`` on the first problem.

The final check is a dry-run of the scorer, which is the cheapest way
to surface structural issues inside the CSVs (missing columns, unparseable
values, weight/score mismatches) without re-implementing the scorer's
internal assumptions here.
"""

from __future__ import annotations

from pathlib import Path

from .scoring import SURAFResults

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
            f"expected {REQUIRED_ASSESSOR_COUNT} {ASSESSOR_GLOB} files in {path}, "
            f"found {len(assessors)}"
        )

    weights = path / WEIGHTS_FILE
    crr_mapping = path / CRR_MAPPING_FILE
    penalty = path / PENALTY_FILE
    for required in (weights, crr_mapping, penalty):
        if not required.is_file():
            raise SurafValidationError(f"missing required file: {required}")

    try:
        SURAFResults().run(
            weights_path=weights,
            assessor_paths=list(assessors),
            crr_path=crr_mapping,
            penalty_path=penalty,
        )
    except Exception as exc:
        raise SurafValidationError(f"dry-run scoring failed for {path}: {exc}") from exc
