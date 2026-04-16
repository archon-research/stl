"""Startup loader: validate and score every SURAF rating package.

Called once from the FastAPI lifespan. Raises on any failure so a bad
rating configuration fails the app explicitly rather than silently
omitting a model.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from .result import SurafResult
from .scoring import SURAFResults
from .validate import (
    CRR_MAPPING_FILE,
    PENALTY_FILE,
    WEIGHTS_FILE,
    SurafValidationError,
    assessor_paths,
    validate_package,
)


def _score_package(path: Path) -> SURAFResults:
    results = SURAFResults()
    results.run(
        weights_path=path / WEIGHTS_FILE,
        assessor_paths=list(assessor_paths(path)),
        crr_path=path / CRR_MAPPING_FILE,
        penalty_path=path / PENALTY_FILE,
    )
    return results


def _build_result(rating_id: str, scored: SURAFResults, source_commit_sha: str) -> SurafResult:
    return SurafResult(
        rating_id=rating_id,
        crr_pct=Decimal(str(scored.adjusted_crr)),
        unadjusted_crr_pct=Decimal(str(scored.unadjusted_crr)),
        penalty_pp=Decimal(str(scored.penalty)),
        avg_score=Decimal(str(scored.avg_score)),
        source_commit_sha=source_commit_sha,
        loaded_at=datetime.now(timezone.utc),
    )


def load_all_ratings(inputs_dir: Path, source_commit_sha: str) -> dict[str, SurafResult]:
    """Validate and score every rating package under ``inputs_dir/ratings``.

    Each subdirectory name is treated as the ``rating_id``. Fails fast on
    the first invalid or unscorable package.
    """
    ratings_root = inputs_dir / "ratings"
    if not ratings_root.is_dir():
        raise SurafValidationError(f"ratings directory not found: {ratings_root}")

    out: dict[str, SurafResult] = {}
    for package in sorted(p for p in ratings_root.iterdir() if p.is_dir()):
        validate_package(package)
        scored = _score_package(package)
        out[package.name] = _build_result(package.name, scored, source_commit_sha)
    return out
