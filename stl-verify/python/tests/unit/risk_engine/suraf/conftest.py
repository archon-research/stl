"""Shared helpers for SURAF unit tests.

Tests copy ``testdata/sample_rating`` into tmp_path so each test can
mutate files without cross-test interference.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

SAMPLE_RATING = Path(__file__).parent / "testdata" / "sample_rating"
SAMPLE_VERSION_NAME = "v1"


@pytest.fixture
def sample_rating(tmp_path: Path) -> Path:
    """Return a writable copy of the testdata sample rating (with version subdirs)."""
    dest = tmp_path / "sample_rating"
    shutil.copytree(SAMPLE_RATING, dest)
    return dest


@pytest.fixture
def sample_version(sample_rating: Path) -> Path:
    """Return the single version directory inside the sample rating."""
    return sample_rating / SAMPLE_VERSION_NAME


@pytest.fixture
def inputs_dir(tmp_path: Path) -> Path:
    """Return an ``inputs/`` directory containing the sample rating."""
    inputs = tmp_path / "inputs"
    ratings = inputs / "ratings"
    ratings.mkdir(parents=True)
    shutil.copytree(SAMPLE_RATING, ratings / "sample_rating")
    return inputs
