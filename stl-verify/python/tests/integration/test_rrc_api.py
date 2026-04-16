"""Integration tests for the GET and POST RRC endpoints.

Uses the real FastAPI app via ``create_app`` so SURAF startup wiring is
exercised end-to-end. ``_resolve`` is overridden for GET so tests don't
need a fully populated risk DB. POST does not touch the DB by design.
"""

from __future__ import annotations

import shutil
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.api.v1 import risk as risk_router
from app.config import Settings
from app.domain.entities.risk import RiskBreakdown, RiskEnrichedCollateral
from app.main import create_app
from app.services.risk_calculation_service import RiskCalculationService

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


def _item(symbol: str, amount_usd: str, token_id: int = 1) -> RiskEnrichedCollateral:
    return RiskEnrichedCollateral(
        token_id=token_id,
        symbol=symbol,
        amount=Decimal("1"),
        backing_pct=Decimal("100"),
        amount_usd=Decimal(amount_usd),
        price_usd=Decimal(amount_usd),
        liquidation_threshold=Decimal("0.85"),
        liquidation_bonus=Decimal("1.05"),
    )


def _stub_service(items: tuple[RiskEnrichedCollateral, ...]) -> AsyncMock:
    service = AsyncMock(spec=RiskCalculationService)
    service.get_risk_breakdown.return_value = RiskBreakdown(backed_asset_id=1, items=items)
    return service


def _override_resolve(service: RiskCalculationService | None):
    async def _dep():
        if service is None:
            from fastapi import HTTPException

            raise HTTPException(404, "receipt token not found")
        yield (service, 1)

    return _dep


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


def test_get_rrc_single_mapped_item(app_factory) -> None:
    app = app_factory()
    service = _stub_service((_item("aUSDC", "1000"),))
    app.dependency_overrides[risk_router._resolve] = _override_resolve(service)

    with TestClient(app) as client:
        response = client.get("/v1/risk/123/rrc")

    assert response.status_code == 200
    body = response.json()
    assert body["receipt_token_id"] == 123
    assert Decimal(body["total_exposure_usd"]) == Decimal("1000")
    assert Decimal(body["modeled_exposure_usd"]) == Decimal("1000")
    assert Decimal(body["final_crr_pct"]) > Decimal("0")
    assert Decimal(body["final_rrc_usd"]) > Decimal("0")

    assert len(body["models"]) == 1
    m = body["models"][0]
    assert m["name"] == "suraf"
    assert m["source_commit_sha"]

    assert len(body["items"]) == 1
    i = body["items"][0]
    assert i["symbol"] == "aUSDC"
    assert i["rating_id"] == "sample_rating"


def test_get_rrc_unmapped_item_surfaces_coverage_gap(app_factory) -> None:
    """Unmapped item appears in items[] with null fields; aggregates
    exclude it so coverage gaps don't hide as lower risk."""
    app = app_factory()  # maps aUSDC only
    service = _stub_service((_item("aUSDC", "600", token_id=1), _item("WBTC", "400", token_id=2)))
    app.dependency_overrides[risk_router._resolve] = _override_resolve(service)

    with TestClient(app) as client:
        response = client.get("/v1/risk/123/rrc")

    assert response.status_code == 200
    body = response.json()
    assert Decimal(body["total_exposure_usd"]) == Decimal("1000")
    assert Decimal(body["modeled_exposure_usd"]) == Decimal("600")

    symbols = {i["symbol"]: i for i in body["items"]}
    assert symbols["aUSDC"]["rating_id"] == "sample_rating"
    assert symbols["WBTC"]["rating_id"] is None
    assert symbols["WBTC"]["crr_pct"] is None
    assert symbols["WBTC"]["rrc_usd"] is None


def test_get_rrc_no_mapped_items_null_aggregates(app_factory) -> None:
    app = app_factory('{"spUSDC": "sample_rating"}')
    service = _stub_service((_item("aUSDC", "1000"),))
    app.dependency_overrides[risk_router._resolve] = _override_resolve(service)

    with TestClient(app) as client:
        response = client.get("/v1/risk/123/rrc")

    assert response.status_code == 200
    body = response.json()
    assert body["final_crr_pct"] is None
    assert body["final_rrc_usd"] is None
    assert body["models"] == []
    assert len(body["items"]) == 1
    assert body["items"][0]["rating_id"] is None


def test_get_rrc_unknown_receipt_token_returns_404(app_factory) -> None:
    app = app_factory()
    app.dependency_overrides[risk_router._resolve] = _override_resolve(None)

    with TestClient(app) as client:
        response = client.get("/v1/risk/999/rrc")

    assert response.status_code == 404


def test_post_rrc_mapped_asset(app_factory) -> None:
    app = app_factory()

    with TestClient(app) as client:
        response = client.post("/v1/risk/rrc", json={"asset": "aUSDC", "usd_exposure": "2000"})

    assert response.status_code == 200
    body = response.json()
    assert body["asset"] == "aUSDC"
    assert Decimal(body["usd_exposure"]) == Decimal("2000")
    assert Decimal(body["final_crr_pct"]) > Decimal("0")
    assert Decimal(body["final_rrc_usd"]) > Decimal("0")
    assert body["models"][0]["source_commit_sha"]


def test_post_rrc_unmapped_asset_returns_404(app_factory) -> None:
    app = app_factory()

    with TestClient(app) as client:
        response = client.post("/v1/risk/rrc", json={"asset": "WBTC", "usd_exposure": "1000"})

    assert response.status_code == 404
    assert "WBTC" in response.json()["detail"]


def test_post_rrc_rejects_non_positive_exposure(app_factory) -> None:
    app = app_factory()

    with TestClient(app) as client:
        response = client.post("/v1/risk/rrc", json={"asset": "aUSDC", "usd_exposure": "0"})

    assert response.status_code == 422


def test_post_rrc_does_not_require_db_resolve(app_factory) -> None:
    """POST must not depend on the receipt-token resolver; no override set."""
    app = app_factory()

    with TestClient(app) as client:
        response = client.post("/v1/risk/rrc", json={"asset": "aUSDC", "usd_exposure": "500"})

    assert response.status_code == 200
