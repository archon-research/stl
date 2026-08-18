"""Unit tests for GET /v1/risk/{asset_id}/core-model and
GET /v1/risk/{chain_id}/{token_address}/core-model endpoints."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from app.api.deps import get_core_model_risk_service, get_receipt_token_lookup
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.main import app
from app.ports.core_model_results_reader import CoreModelResult

_ASSET_ID = 42
_CHAIN_ID = 1
_TOKEN_ADDRESS = "0x" + "ab" * 20
_NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)

_RESULT = CoreModelResult(
    market_key="sparklend_usdc",
    crr_el_pct=Decimal("12.5"),
    crr_es_pct=Decimal("15.0"),
    crr_var_pct=Decimal("10.0"),
    hhi=Decimal("22.3"),
    protocol="SPARKLEND",
    forecast_step=14,
    n_mc=10000,
    copula_type="T-COPULA",
    computed_at=_NOW,
)

_RECEIPT_TOKEN_INFO = ReceiptTokenInfo(
    receipt_token_id=_ASSET_ID,
    protocol_id=10,
    underlying_token_id=20,
    receipt_token_address=bytes.fromhex("ab" * 20),
    chain_id=_CHAIN_ID,
    protocol_name="sparklend",
    receipt_token_token_id=30,
)


def _fake_service(result: CoreModelResult | None = _RESULT):
    svc = AsyncMock()
    svc.get_latest_result = AsyncMock(return_value=result)
    return svc


def _override_service(result: CoreModelResult | None = _RESULT):
    svc = _fake_service(result)

    def _dep():
        return svc

    return _dep, svc


def _override_lookup(info: ReceiptTokenInfo | None = _RECEIPT_TOKEN_INFO):
    lookup = AsyncMock()
    lookup.get = AsyncMock(return_value=info)
    lookup.get_by_chain_and_address = AsyncMock(return_value=info)

    def _dep():
        return lookup

    return _dep


@pytest.fixture
def client():
    dep, _ = _override_service()
    app.dependency_overrides[get_core_model_risk_service] = dep
    app.dependency_overrides[get_receipt_token_lookup] = _override_lookup()
    yield TestClient(app)
    app.dependency_overrides.pop(get_core_model_risk_service, None)
    app.dependency_overrides.pop(get_receipt_token_lookup, None)


# ---------------------------------------------------------------------------
# GET /v1/risk/{asset_id}/core-model
# ---------------------------------------------------------------------------


def test_get_by_asset_id_returns_result(client: TestClient) -> None:
    response = client.get(f"/v1/risk/{_ASSET_ID}/core-model")

    assert response.status_code == 200
    body = response.json()
    assert body["asset_id"] == _ASSET_ID
    assert body["market_key"] == "sparklend_usdc"
    assert Decimal(body["crr_el_pct"]) == Decimal("12.5")
    assert Decimal(body["crr_es_pct"]) == Decimal("15.0")
    assert Decimal(body["crr_var_pct"]) == Decimal("10.0")
    assert Decimal(body["hhi"]) == Decimal("22.3")
    assert body["protocol"] == "SPARKLEND"
    assert body["forecast_step"] == 14
    assert body["n_mc"] == 10000
    assert body["copula_type"] == "T-COPULA"
    assert body["computed_at"] is not None


def test_get_by_asset_id_returns_404_when_not_in_mapping(client: TestClient) -> None:
    dep, svc = _override_service(result=None)
    svc.get_latest_result = AsyncMock(return_value=None)
    app.dependency_overrides[get_core_model_risk_service] = dep

    response = client.get(f"/v1/risk/{_ASSET_ID}/core-model")

    assert response.status_code == 404


def test_get_by_asset_id_returns_404_when_asset_unknown(client: TestClient) -> None:
    app.dependency_overrides[get_receipt_token_lookup] = _override_lookup(info=None)

    response = client.get(f"/v1/risk/{_ASSET_ID}/core-model")

    assert response.status_code == 404


def test_get_by_asset_id_handles_null_hhi(client: TestClient) -> None:
    dep, _ = _override_service(
        result=CoreModelResult(
            market_key="sparklend_usdc",
            crr_el_pct=Decimal("12.5"),
            crr_es_pct=Decimal("15.0"),
            crr_var_pct=Decimal("10.0"),
            hhi=None,
            protocol="SPARKLEND",
            forecast_step=14,
            n_mc=10000,
            copula_type="T-COPULA",
            computed_at=_NOW,
        )
    )
    app.dependency_overrides[get_core_model_risk_service] = dep

    response = client.get(f"/v1/risk/{_ASSET_ID}/core-model")

    assert response.status_code == 200
    assert response.json()["hhi"] is None


# ---------------------------------------------------------------------------
# GET /v1/risk/{chain_id}/{token_address}/core-model
# ---------------------------------------------------------------------------


def test_get_by_address_returns_result(client: TestClient) -> None:
    response = client.get(f"/v1/risk/{_CHAIN_ID}/{_TOKEN_ADDRESS}/core-model")

    assert response.status_code == 200
    body = response.json()
    assert body["asset_id"] == _ASSET_ID
    assert body["market_key"] == "sparklend_usdc"
    assert Decimal(body["crr_el_pct"]) == Decimal("12.5")


def test_get_by_address_returns_404_when_token_unknown(client: TestClient) -> None:
    app.dependency_overrides[get_receipt_token_lookup] = _override_lookup(info=None)

    response = client.get(f"/v1/risk/{_CHAIN_ID}/{_TOKEN_ADDRESS}/core-model")

    assert response.status_code == 404


def test_get_by_address_returns_422_on_bad_address(client: TestClient) -> None:
    response = client.get(f"/v1/risk/{_CHAIN_ID}/not-an-address/core-model")

    assert response.status_code == 422


def test_get_by_address_returns_422_on_bad_chain_id(client: TestClient) -> None:
    response = client.get(f"/v1/risk/0/{_TOKEN_ADDRESS}/core-model")

    assert response.status_code == 422
