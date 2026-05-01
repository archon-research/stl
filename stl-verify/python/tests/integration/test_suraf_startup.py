"""App startup integration tests for SURAF loading.

Verifies ``create_app`` populates ``app.state.suraf_ratings`` and
``app.state.asset_to_rating`` when inputs are valid, and that it raises
when any rating package, the mapping file, or the mapping-to-ratings
references are invalid.

File-based checks (malformed JSON, missing file, unknown rating_id)
are caught in ``create_app`` before the lifespan starts.  DB-dependent
checks (unknown receipt token address) are caught in the lifespan when
the mapping is resolved against the database.
"""

from __future__ import annotations

import asyncio
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
from tests.integration.conftest import composite_mapping_key, insert_receipt_token

SAMPLE_PACKAGE = Path(__file__).resolve().parents[1] / "unit" / "risk_engine" / "suraf" / "testdata" / "sample_rating"

# Deterministic test address — used for seeding and mapping.
_TEST_ADDRESS = bytes.fromhex("Bcca60bB61934080951369a648Fb03DF4F96263C")
_TEST_CHAIN_ID = 1


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


def test_startup_populates_suraf_ratings(async_db_url: str, db_url: str, tmp_path: Path) -> None:
    receipt_token_id = asyncio.run(insert_receipt_token(db_url, _TEST_CHAIN_ID, _TEST_ADDRESS))

    inputs_dir = _build_inputs_dir(tmp_path)
    key = composite_mapping_key(_TEST_CHAIN_ID, _TEST_ADDRESS)
    mapping_file = _write_mapping(tmp_path, "{" + f'"{key}": "sample_rating"' + "}")
    app = create_app(_settings(async_db_url, inputs_dir, mapping_file))

    with TestClient(app) as client:
        assert client.get("/v1/status").status_code == 200

        ratings = app.state.suraf_ratings
        assert set(ratings.keys()) == {"sample_rating"}
        assert isinstance(ratings["sample_rating"], SurafResult)

        assert app.state.asset_to_rating == {receipt_token_id: "sample_rating"}


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
    # _check_mapping_refs catches unknown rating_ids before the lifespan
    # starts — no DB seed needed.
    inputs_dir = _build_inputs_dir(tmp_path)
    key = composite_mapping_key(_TEST_CHAIN_ID, _TEST_ADDRESS)
    mapping_file = _write_mapping(tmp_path, "{" + f'"{key}": "does_not_exist"' + "}")

    with pytest.raises(MappingError, match="does_not_exist"):
        create_app(_settings(async_db_url, inputs_dir, mapping_file))


def test_startup_fails_when_mapping_references_unknown_address(async_db_url: str, tmp_path: Path) -> None:
    inputs_dir = _build_inputs_dir(tmp_path)
    zero_addr = bytes(20)
    key = composite_mapping_key(_TEST_CHAIN_ID, zero_addr)
    mapping_file = _write_mapping(tmp_path, "{" + f'"{key}": "sample_rating"' + "}")

    # DB resolution happens in the lifespan, so the error is raised when
    # TestClient triggers lifespan startup.
    app = create_app(_settings(async_db_url, inputs_dir, mapping_file))
    with pytest.raises(MappingError, match="unknown receipt token"):
        with TestClient(app):
            pass
