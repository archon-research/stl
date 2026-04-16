"""App startup integration tests for SURAF loading.

Verifies the lifespan populates ``app.state.suraf_ratings`` when inputs
are valid, and that startup fails (raises) when any rating package is
invalid.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.config import Settings
from app.main import create_app
from app.risk_engine.suraf.result import SurafResult
from app.risk_engine.suraf.validate import SurafValidationError

SAMPLE_PACKAGE = (
    Path(__file__).resolve().parents[1]
    / "unit"
    / "risk_engine"
    / "suraf"
    / "testdata"
    / "sample_rating"
)


def _settings(async_db_url: str, suraf_inputs_dir: Path) -> Settings:
    return Settings(
        database_url=SecretStr(async_db_url),
        suraf_inputs_dir=suraf_inputs_dir,
    )


def _build_inputs_dir(tmp_path: Path) -> Path:
    inputs = tmp_path / "inputs"
    ratings = inputs / "ratings"
    ratings.mkdir(parents=True)
    shutil.copytree(SAMPLE_PACKAGE, ratings / "sample_rating")
    return inputs


def test_startup_populates_suraf_ratings(async_db_url: str, tmp_path: Path) -> None:
    inputs_dir = _build_inputs_dir(tmp_path)
    app = create_app(_settings(async_db_url, inputs_dir))

    with TestClient(app) as client:
        # Sanity check app is serving.
        assert client.get("/v1/status").status_code == 200

        ratings = app.state.suraf_ratings
        assert set(ratings.keys()) == {"sample_rating"}
        assert isinstance(ratings["sample_rating"], SurafResult)


def test_startup_fails_on_invalid_package(async_db_url: str, tmp_path: Path) -> None:
    inputs_dir = _build_inputs_dir(tmp_path)
    (inputs_dir / "ratings" / "sample_rating" / "weights.csv").unlink()

    app = create_app(_settings(async_db_url, inputs_dir))

    with pytest.raises(SurafValidationError), TestClient(app):
        pass
