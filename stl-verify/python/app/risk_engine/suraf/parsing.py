"""CSV -> axis_synome entity loader for a single SURAF rating version.

The on-disk layout is owned by this repo; the parser converts those CSVs
into the minimal dataclasses that the axis_synome scoring helpers expect.

All parse failures are wrapped as ``SurafValidationError`` so the startup
loader continues to fail fast with a single exception type.
"""

from __future__ import annotations

import csv
from collections.abc import Iterable
from pathlib import Path
from typing import NamedTuple

from axis_synome.spec.suraf.entities.assessor_score import (
    SCORE_MAX,
    SCORE_MIN,
    AssessorScore,
)
from axis_synome.spec.suraf.entities.mappings import CRRMapping, PenaltyMapping

from .validate import (
    CRR_MAPPING_FILE,
    PENALTY_FILE,
    WEIGHTS_FILE,
    SurafValidationError,
    scorecard_paths,
)

ROMAN_TO_INT = {"I": "1", "II": "2", "III": "3", "IV": "4", "V": "5"}


class ParsedRating(NamedTuple):
    """Library entities for a single rating version."""

    assessors: list[AssessorScore]
    crr_mapping: CRRMapping
    penalty_mapping: PenaltyMapping


def load_version(version_dir: Path) -> ParsedRating:
    """Read the four CSVs under ``version_dir`` into library entities."""
    subsection_weights, pillar_weights = _parse_weights(version_dir / WEIGHTS_FILE)
    assessors = [_parse_scorecard(path, subsection_weights, pillar_weights) for path in scorecard_paths(version_dir)]
    crr_mapping = _parse_crr_mapping(version_dir / CRR_MAPPING_FILE)
    penalty_mapping = _parse_penalty_mapping(version_dir / PENALTY_FILE)
    return ParsedRating(assessors=assessors, crr_mapping=crr_mapping, penalty_mapping=penalty_mapping)


def _require_columns(headers: Iterable[str] | None, required: set[str], path: Path) -> None:
    missing = required - set(headers or [])
    if missing:
        raise SurafValidationError(f"{path}: missing required column(s): {sorted(missing)}")


def _parse_weights(path: Path) -> tuple[dict[str, int], dict[str, int]]:
    """Parse weights.csv into (subsection_weights, pillar_weights) dicts.

    Subsection rows have a non-empty ``subsection_ref`` and a positive
    ``subsection_weight``. Pillar header rows have an empty
    ``subsection_ref`` and a positive ``pillar_weight``; the pillar Roman
    numeral is extracted from the ``title`` column.
    """
    required = {"pillar", "subsection_ref", "title", "pillar_weight", "subsection_weight"}
    subsection: dict[str, int] = {}
    pillar: dict[str, int] = {}
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        _require_columns(reader.fieldnames, required, path)
        for row_no, row in enumerate(reader, start=2):
            ref = row["subsection_ref"].strip()
            try:
                sw = int(row["subsection_weight"] or 0)
                pw = int(row["pillar_weight"] or 0)
            except ValueError as err:
                raise SurafValidationError(f"{path} row {row_no}: cannot parse weight: {err}") from err
            if ref and sw > 0:
                subsection[ref] = sw
            elif not ref and pw > 0:
                roman = row["title"].split(":")[0].replace("PILLAR", "").strip()
                if roman not in ROMAN_TO_INT:
                    raise SurafValidationError(f"{path} row {row_no}: unrecognised pillar Roman numeral '{roman}'")
                pillar[ROMAN_TO_INT[roman]] = pw
    if not subsection:
        raise SurafValidationError(f"{path}: no subsection weights parsed")
    if not pillar:
        raise SurafValidationError(f"{path}: no pillar weights parsed")
    return subsection, pillar


def _parse_scorecard(
    path: Path,
    subsection_weights: dict[str, int],
    pillar_weights: dict[str, int],
) -> AssessorScore:
    """Parse one scorecard CSV into an AssessorScore with absolute weights."""
    required = {"pillar", "subsection_ref", "score"}
    raw_scores: dict[str, int] = {}
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        _require_columns(reader.fieldnames, required, path)
        for row_no, row in enumerate(reader, start=2):
            pillar = row["pillar"].strip()
            ref = row["subsection_ref"].strip()
            if not (pillar.isdigit() and ref):
                continue
            raw = row["score"].strip()
            if not raw:
                continue
            try:
                value = int(raw)
            except ValueError as err:
                raise SurafValidationError(
                    f"{path} row {row_no} ref '{ref}': cannot parse score '{raw}': {err}"
                ) from err
            if not (SCORE_MIN <= value <= SCORE_MAX):
                raise SurafValidationError(
                    f"{path} row {row_no} ref '{ref}': score {value} outside [{SCORE_MIN}, {SCORE_MAX}]"
                )
            raw_scores[ref] = value

    if not raw_scores:
        raise SurafValidationError(f"{path}: no scoreable subsection rows")

    grouped: dict[str, list[str]] = {}
    for ref in raw_scores:
        grouped.setdefault(ref.split(".")[0], []).append(ref)

    pillar_entries: list[tuple[int, list[tuple[int, int]]]] = []
    for pillar_num, refs in grouped.items():
        pw = pillar_weights.get(pillar_num, 0)
        if not pw:
            continue
        sections = [(subsection_weights[ref], raw_scores[ref]) for ref in refs if subsection_weights.get(ref, 0) > 0]
        if sections:
            pillar_entries.append((pw, sections))

    if not pillar_entries:
        raise SurafValidationError(f"{path}: no scored subsections matched known weights")

    return AssessorScore(scores=pillar_entries)


def _parse_crr_mapping(path: Path) -> CRRMapping:
    required = {"score", "crr"}
    rows: list[tuple[float, float]] = []
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        _require_columns(reader.fieldnames, required, path)
        for row_no, row in enumerate(reader, start=2):
            try:
                rows.append((float(row["score"]), float(row["crr"])))
            except ValueError as err:
                raise SurafValidationError(f"{path} row {row_no}: cannot parse values: {err}") from err
    if len(rows) < 2:
        raise SurafValidationError(f"{path}: CRRMapping requires at least 2 breakpoints, got {len(rows)}")
    scores = [s for s, _ in rows]
    if scores != sorted(scores) or len(set(scores)) != len(scores):
        raise SurafValidationError(f"{path}: CRRMapping scores must be strictly increasing")
    return CRRMapping(table=tuple(rows))


def _parse_penalty_mapping(path: Path) -> PenaltyMapping:
    required = {"n_score_1", "penalty_pp"}
    rows: list[tuple[int, float]] = []
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        _require_columns(reader.fieldnames, required, path)
        for row_no, row in enumerate(reader, start=2):
            try:
                rows.append((int(row["n_score_1"]), float(row["penalty_pp"])))
            except ValueError as err:
                raise SurafValidationError(f"{path} row {row_no}: cannot parse values: {err}") from err
    if len(rows) < 2:
        raise SurafValidationError(f"{path}: PenaltyMapping requires at least 2 breakpoints, got {len(rows)}")
    ns = [n for n, _ in rows]
    if any(n < 0 for n in ns):
        raise SurafValidationError(f"{path}: PenaltyMapping n_score_1 values must be non-negative")
    if ns != sorted(ns) or len(set(ns)) != len(ns):
        raise SurafValidationError(f"{path}: PenaltyMapping n_score_1 values must be strictly increasing")
    return PenaltyMapping(table=tuple(rows))
