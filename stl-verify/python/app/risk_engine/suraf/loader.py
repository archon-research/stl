"""Startup loader: validate and score every SURAF rating at its latest version.

Called once from the FastAPI lifespan. Raises on any failure so a bad
rating configuration fails the app explicitly rather than silently
omitting a model.

Layout: ``ratings/{rating_id}/{version}/`` where ``{version}`` matches
``v{N}`` (``v1``, ``v2``, ...).
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import NamedTuple

from axis_synome.spec.suraf.formulas.scoring import (
    calculate_penalty,
    map_crr,
    total_n_score_1,
    weighted_average,
)

from app.logging import get_logger

from .parsing import load_version
from .result import SurafResult
from .validate import SurafValidationError, validate_version

logger = get_logger(__name__)

_VERSION_RE = re.compile(r"^v(\d+)$")


class ScoredRating(NamedTuple):
    avg_score: float
    unadjusted_crr: float
    penalty: float
    adjusted_crr: float


def _version_number(path: Path) -> int:
    match = _VERSION_RE.match(path.name)
    if match is None:
        raise SurafValidationError(f"invalid version directory name: {path} (expected 'v<N>', e.g. 'v1')")
    return int(match.group(1))


def _latest_version_dir(rating_dir: Path) -> Path:
    versions = [p for p in rating_dir.iterdir() if p.is_dir()]
    if not versions:
        raise SurafValidationError(f"no version directories under {rating_dir}")
    return max(versions, key=_version_number)


def _score_version(version_dir: Path) -> ScoredRating:
    try:
        parsed = load_version(version_dir)
        avg = weighted_average(parsed.assessors)
        unadj = map_crr(avg, parsed.crr_mapping)
        n1 = total_n_score_1(parsed.assessors)
        penalty = calculate_penalty(n1, parsed.penalty_mapping)
        adjusted = min(unadj + penalty, 100.0)
        return ScoredRating(avg_score=avg, unadjusted_crr=unadj, penalty=penalty, adjusted_crr=adjusted)
    except SurafValidationError:
        raise
    except Exception as exc:
        raise SurafValidationError(f"scoring failed for {version_dir}: {exc}") from exc


def _build_result(
    rating_id: str,
    version: str,
    scored: ScoredRating,
    source_commit_sha: str,
) -> SurafResult:
    return SurafResult(
        rating_id=rating_id,
        version=version,
        crr_pct=Decimal(str(scored.adjusted_crr)),
        unadjusted_crr_pct=Decimal(str(scored.unadjusted_crr)),
        penalty_pp=Decimal(str(scored.penalty)),
        avg_score=Decimal(str(scored.avg_score)),
        source_commit_sha=source_commit_sha,
        loaded_at=datetime.now(timezone.utc),
    )


def load_all_ratings(inputs_dir: Path, source_commit_sha: str) -> dict[str, SurafResult]:
    """Validate and score the latest version of every rating under ``inputs_dir/ratings``.

    Each top-level subdirectory name is the ``rating_id``; each contains
    one or more version subdirectories. Fails fast on the first invalid
    or unscorable version.
    """
    ratings_root = inputs_dir / "ratings"
    if not ratings_root.is_dir():
        raise SurafValidationError(f"ratings directory not found: {ratings_root}")

    logger.info("loading SURAF ratings from %s", ratings_root)
    out: dict[str, SurafResult] = {}
    for rating_dir in sorted(p for p in ratings_root.iterdir() if p.is_dir()):
        version_dir = _latest_version_dir(rating_dir)
        validate_version(version_dir)
        scored = _score_version(version_dir)
        result = _build_result(rating_dir.name, version_dir.name, scored, source_commit_sha)
        out[rating_dir.name] = result
        logger.info(
            "loaded SURAF rating rating_id=%s version=%s crr_pct=%s avg_score=%s",
            result.rating_id,
            result.version,
            result.crr_pct,
            result.avg_score,
        )

    logger.info("SURAF ratings loaded count=%d", len(out))
    return out
