"""App startup integration tests for SURAF loading.

Verifies ``create_app`` populates ``app.state.suraf_ratings`` and
``app.state.asset_to_rating`` when inputs are valid, and that it raises
(before acquiring any resources) when any rating package, the mapping
file, or the mapping-to-ratings references are invalid.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.config import Settings
from app.main import create_app
from app.risk_engine.mapping import MappingError
from app.risk_engine.suraf.result import SurafResult
from app.risk_engine.suraf.validate import SurafValidationError

SAMPLE_PACKAGE = Path(__file__).resolve().parents[1] / "unit" / "risk_engine" / "suraf" / "testdata" / "sample_rating"


def _settings(async_db_url: str, suraf_inputs_dir: Path, suraf_mappings_file: Path) -> Settings:
    return Settings(
        database_url=SecretStr(async_db_url),
        suraf_inputs_dir=suraf_inputs_dir,
        suraf_mappings_file=suraf_mappings_file,
    )


def _build_inputs_dir(tmp_path: Path) -> Path:
    inputs = tmp_path / "inputs"
    ratings = inputs / "ratings"
    ratings.mkdir(parents=True)
    shutil.copytree(SAMPLE_PACKAGE, ratings / "sample_rating")
    return inputs


def _write_mapping(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "asset_to_rating.json"
    path.write_text(content)
    return path


def test_startup_populates_suraf_ratings(async_db_url: str, tmp_path: Path) -> None:
    inputs_dir = _build_inputs_dir(tmp_path)
    mapping_file = _write_mapping(tmp_path, '{"aUSDC": "sample_rating"}')
    app = create_app(_settings(async_db_url, inputs_dir, mapping_file))

    with TestClient(app) as client:
        assert client.get("/v1/status").status_code == 200

        ratings = app.state.suraf_ratings
        assert set(ratings.keys()) == {"sample_rating"}
        assert isinstance(ratings["sample_rating"], SurafResult)

        # Mapping keys are casefolded at load time for case-insensitive lookup.
        assert app.state.asset_to_rating == {"ausdc": "sample_rating"}


def test_startup_fails_on_invalid_package(async_db_url: str, tmp_path: Path) -> None:
    inputs_dir = _build_inputs_dir(tmp_path)
    mapping_file = _write_mapping(tmp_path, "{}")
    (inputs_dir / "ratings" / "sample_rating" / "v1" / "weights.csv").unlink()

    with pytest.raises(SurafValidationError):
        create_app(_settings(async_db_url, inputs_dir, mapping_file))


def test_startup_fails_on_malformed_mapping(async_db_url: str, tmp_path: Path) -> None:
    inputs_dir = _build_inputs_dir(tmp_path)
    mapping_file = _write_mapping(tmp_path, "{ not json")

    with pytest.raises(MappingError):
        create_app(_settings(async_db_url, inputs_dir, mapping_file))


def test_startup_fails_when_mapping_file_missing(async_db_url: str, tmp_path: Path) -> None:
    inputs_dir = _build_inputs_dir(tmp_path)
    mapping_file = tmp_path / "does_not_exist.json"

    with pytest.raises(MappingError):
        create_app(_settings(async_db_url, inputs_dir, mapping_file))


def test_startup_fails_when_mapping_references_unknown_rating(async_db_url: str, tmp_path: Path) -> None:
    inputs_dir = _build_inputs_dir(tmp_path)
    mapping_file = _write_mapping(tmp_path, '{"aUSDC": "does_not_exist"}')

    with pytest.raises(MappingError, match="does_not_exist"):
        create_app(_settings(async_db_url, inputs_dir, mapping_file))
