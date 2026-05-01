"""Integration tests for ``POST /v1/risk/rrc/scenario``.

Uses the real FastAPI app via ``create_app`` so SURAF startup wiring is
exercised end-to-end.
"""

from __future__ import annotations

import asyncio
import shutil
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.config import Settings
from app.main import create_app
from tests.integration.conftest import composite_mapping_key, insert_receipt_token

SAMPLE_PACKAGE = Path(__file__).resolve().parents[1] / "unit" / "risk_engine" / "suraf" / "testdata" / "sample_rating"

_TEST_ADDRESS = bytes.fromhex("Bcca60bB61934080951369a648Fb03DF4F96263C")
_TEST_CHAIN_ID = 1


def _inputs_dir(tmp_path: Path) -> Path:
    inputs = tmp_path / "inputs"
    ratings = inputs / "ratings"
    ratings.mkdir(parents=True)
    shutil.copytree(SAMPLE_PACKAGE, ratings / "sample_rating")
    return inputs


def _mapping_file(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "asset_to_rating.json"
    path.write_text(content)
    return path


@pytest.fixture
def app_factory(async_db_url: str, db_url: str, tmp_path: Path):
    receipt_token_id = asyncio.run(insert_receipt_token(db_url, _TEST_CHAIN_ID, _TEST_ADDRESS))

    def _build(mapping_json: str | None = None):
        if mapping_json is None:
            key = composite_mapping_key(_TEST_CHAIN_ID, _TEST_ADDRESS)
            mapping_json = "{" + f'"{key}": "sample_rating"' + "}"
        settings = Settings(
            database_url=SecretStr(async_db_url),
            suraf_inputs_dir=_inputs_dir(tmp_path),
            suraf_mappings_file=_mapping_file(tmp_path, mapping_json),
        )
        return create_app(settings), receipt_token_id

    return _build


def test_post_rrc_scenario_mapped_asset(app_factory) -> None:
    app, receipt_token_id = app_factory()

    with TestClient(app) as client:
        response = client.post(
            "/v1/risk/rrc/scenario",
            json={"receipt_token_id": receipt_token_id, "usd_exposure": "1000"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["receipt_token_id"] == receipt_token_id
    assert Decimal(body["usd_exposure"]) == Decimal("1000")
    assert body["rating_id"] == "sample_rating"
    assert body["rating_version"] == "v1"
    assert Decimal(body["crr_pct"]) > Decimal("0")
    assert Decimal(body["rrc_usd"]) == Decimal(body["usd_exposure"]) * Decimal(body["crr_pct"]) / Decimal("100")
    assert body["source_commit_sha"]


def test_post_rrc_scenario_unmapped_asset_returns_404(app_factory) -> None:
    app, _ = app_factory()

    with TestClient(app) as client:
        response = client.post(
            "/v1/risk/rrc/scenario",
            json={"receipt_token_id": 999999, "usd_exposure": "1000"},
        )

    assert response.status_code == 404
    assert "999999" in response.json()["detail"]


def test_post_rrc_scenario_rejects_non_positive_exposure(app_factory) -> None:
    app, receipt_token_id = app_factory()

    with TestClient(app) as client:
        zero = client.post(
            "/v1/risk/rrc/scenario",
            json={"receipt_token_id": receipt_token_id, "usd_exposure": "0"},
        )
        negative = client.post(
            "/v1/risk/rrc/scenario",
            json={"receipt_token_id": receipt_token_id, "usd_exposure": "-1"},
        )

    assert zero.status_code == 422
    assert negative.status_code == 422
