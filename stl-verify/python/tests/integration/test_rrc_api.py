"""Integration tests for ``POST /v1/risk/rrc/scenario``.

Uses the real FastAPI app via ``create_app`` so SURAF startup wiring is
exercised end-to-end.
"""

from __future__ import annotations

import shutil
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.config import Settings
from app.main import create_app

SAMPLE_PACKAGE = Path(__file__).resolve().parents[1] / "unit" / "risk_engine" / "suraf" / "testdata" / "sample_rating"


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
def app_factory(async_db_url: str, tmp_path: Path):
    def _build(mapping_json: str = '{"aUSDC": "sample_rating"}'):
        settings = Settings(
            database_url=SecretStr(async_db_url),
            suraf_inputs_dir=_inputs_dir(tmp_path),
            suraf_mappings_file=_mapping_file(tmp_path, mapping_json),
        )
        return create_app(settings)

    return _build


def test_post_rrc_scenario_mapped_asset(app_factory) -> None:
    app = app_factory()

    with TestClient(app) as client:
        response = client.post("/v1/risk/rrc/scenario", json={"asset": "aUSDC", "usd_exposure": "1000"})

    assert response.status_code == 200
    body = response.json()
    assert body["asset"] == "aUSDC"
    assert Decimal(body["usd_exposure"]) == Decimal("1000")
    assert body["rating_id"] == "sample_rating"
    assert body["rating_version"] == "v1"
    assert Decimal(body["crr_pct"]) > Decimal("0")
    assert Decimal(body["rrc_usd"]) == Decimal(body["usd_exposure"]) * Decimal(body["crr_pct"]) / Decimal("100")
    assert body["source_commit_sha"]


def test_post_rrc_scenario_unmapped_asset_returns_404(app_factory) -> None:
    app = app_factory()

    with TestClient(app) as client:
        response = client.post("/v1/risk/rrc/scenario", json={"asset": "WBTC", "usd_exposure": "1000"})

    assert response.status_code == 404
    assert "WBTC" in response.json()["detail"]


def test_post_rrc_scenario_rejects_non_positive_exposure(app_factory) -> None:
    app = app_factory()

    with TestClient(app) as client:
        zero = client.post("/v1/risk/rrc/scenario", json={"asset": "aUSDC", "usd_exposure": "0"})
        negative = client.post("/v1/risk/rrc/scenario", json={"asset": "aUSDC", "usd_exposure": "-1"})

    assert zero.status_code == 422
    assert negative.status_code == 422
