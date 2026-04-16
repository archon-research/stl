"""Shared helpers for SURAF unit tests.

Tests copy ``testdata/sample_rating`` into tmp_path so each test can
mutate files without cross-test interference.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

SAMPLE_PACKAGE = Path(__file__).parent / "testdata" / "sample_rating"


@pytest.fixture
def sample_package(tmp_path: Path) -> Path:
    """Return a writable copy of the testdata sample rating package."""
    dest = tmp_path / "sample_rating"
    shutil.copytree(SAMPLE_PACKAGE, dest)
    return dest


@pytest.fixture
def inputs_dir(tmp_path: Path) -> Path:
    """Return an ``inputs/`` directory containing the sample rating package."""
    inputs = tmp_path / "inputs"
    ratings = inputs / "ratings"
    ratings.mkdir(parents=True)
    shutil.copytree(SAMPLE_PACKAGE, ratings / "sample_rating")
    return inputs
