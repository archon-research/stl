"""File-shape validation for a single SURAF rating version.

A rating lives at ``ratings/{rating_id}/{version}/``. Each version must
contain:

- ``weights.csv``, ``crr_mapping.csv``, ``penalty.csv``
- a ``scorecards/`` directory with exactly three ``*.csv`` assessor files
  (filenames are free-form; only the count matters)

Scoring-correctness (unparseable cells, weight/score mismatches, etc.)
is caught by the loader when it actually scores the package, and by CI's
dry-run on PRs that add or change rating packages.
"""

from __future__ import annotations

from pathlib import Path

SCORECARDS_DIR = "scorecards"
SCORECARD_GLOB = "*.csv"
REQUIRED_ASSESSOR_COUNT = 3
WEIGHTS_FILE = "weights.csv"
CRR_MAPPING_FILE = "crr_mapping.csv"
PENALTY_FILE = "penalty.csv"


class SurafValidationError(Exception):
    """Raised when a SURAF rating version fails validation."""


def scorecard_paths(version_dir: Path) -> list[Path]:
    """Return the sorted assessor CSVs in ``{version_dir}/scorecards/``."""
    return sorted((version_dir / SCORECARDS_DIR).glob(SCORECARD_GLOB))


def validate_version(version_dir: Path) -> None:
    """Validate a single rating-version directory. Raises ``SurafValidationError``."""
    if not version_dir.is_dir():
        raise SurafValidationError(f"rating version not a directory: {version_dir}")

    scorecards = scorecard_paths(version_dir)
    if len(scorecards) != REQUIRED_ASSESSOR_COUNT:
        raise SurafValidationError(
            f"expected {REQUIRED_ASSESSOR_COUNT} {SCORECARD_GLOB} files under "
            f"{version_dir}/{SCORECARDS_DIR}, found {len(scorecards)}"
        )

    for required in (version_dir / WEIGHTS_FILE, version_dir / CRR_MAPPING_FILE, version_dir / PENALTY_FILE):
        if not required.is_file():
            raise SurafValidationError(f"missing required file: {required}")
