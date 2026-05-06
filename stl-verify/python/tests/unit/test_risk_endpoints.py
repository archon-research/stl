"""Router-level tests for the legacy ``/v1/risk/...`` endpoints.

These tests exercise the FastAPI translation layer in isolation by mocking
the ``CryptoLendingRiskService`` dependency. They lock down:

* ``service.get_*_legacy`` returning ``None`` -> ``404``
* the service raising ``ValueError`` -> ``422``
* the service raising ``AllocationShareError`` subtypes -> ``503`` with the
  matching ``share_data_*`` code

These mappings will need to be re-asserted against the unified
``/v1/risk/rrc`` endpoint in VEC-183, but the exception-translation contract
(422/503) should carry over and is worth pinning down now.
"""

from decimal import Decimal
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.api.deps import get_crypto_lending_risk_service
from app.domain.exceptions import MissingShareError, StaleShareError
from app.main import app
from app.services.crypto_lending_risk_service import CryptoLendingRiskService

_RECEIPT_TOKEN_ID = 1234


def _override_service(service: CryptoLendingRiskService):
    def _dep():
        return service

    return _dep


def _make_service() -> AsyncMock:
    return AsyncMock(spec=CryptoLendingRiskService)


def test_bad_debt_returns_404_when_service_returns_none() -> None:
    service = _make_service()
    service.get_bad_debt_legacy.return_value = None
    app.dependency_overrides[get_crypto_lending_risk_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/risk/{_RECEIPT_TOKEN_ID}/bad-debt?gap_pct=0.1")

        assert response.status_code == 404
        assert response.json()["detail"] == "receipt token not found"
        service.get_bad_debt_legacy.assert_awaited_once_with(_RECEIPT_TOKEN_ID, Decimal("0.1"))
    finally:
        app.dependency_overrides.pop(get_crypto_lending_risk_service, None)


def test_bad_debt_returns_422_on_value_error() -> None:
    service = _make_service()
    service.get_bad_debt_legacy.side_effect = ValueError("bad receipt token shape")
    app.dependency_overrides[get_crypto_lending_risk_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/risk/{_RECEIPT_TOKEN_ID}/bad-debt?gap_pct=0.1")

        assert response.status_code == 422
        assert response.json()["detail"] == "bad receipt token shape"
    finally:
        app.dependency_overrides.pop(get_crypto_lending_risk_service, None)


def test_bad_debt_returns_503_share_data_missing() -> None:
    service = _make_service()
    service.get_bad_debt_legacy.side_effect = MissingShareError("no active allocation")
    app.dependency_overrides[get_crypto_lending_risk_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/risk/{_RECEIPT_TOKEN_ID}/bad-debt?gap_pct=0.1")

        assert response.status_code == 503
        body = response.json()
        assert body["detail"]["code"] == "share_data_missing"
        assert body["detail"]["message"] == "no active allocation"
    finally:
        app.dependency_overrides.pop(get_crypto_lending_risk_service, None)


def test_bad_debt_returns_503_share_data_stale() -> None:
    service = _make_service()
    service.get_bad_debt_legacy.side_effect = StaleShareError("supply too old")
    app.dependency_overrides[get_crypto_lending_risk_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/risk/{_RECEIPT_TOKEN_ID}/bad-debt?gap_pct=0.1")

        assert response.status_code == 503
        assert response.json()["detail"]["code"] == "share_data_stale"
    finally:
        app.dependency_overrides.pop(get_crypto_lending_risk_service, None)


def test_breakdown_returns_404_when_service_returns_none() -> None:
    service = _make_service()
    service.get_risk_breakdown_legacy.return_value = None
    app.dependency_overrides[get_crypto_lending_risk_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/risk/{_RECEIPT_TOKEN_ID}/breakdown")

        assert response.status_code == 404
        assert response.json()["detail"] == "receipt token not found"
        service.get_risk_breakdown_legacy.assert_awaited_once_with(_RECEIPT_TOKEN_ID)
    finally:
        app.dependency_overrides.pop(get_crypto_lending_risk_service, None)


def test_breakdown_returns_422_on_value_error() -> None:
    service = _make_service()
    service.get_risk_breakdown_legacy.side_effect = ValueError("bad receipt token shape")
    app.dependency_overrides[get_crypto_lending_risk_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/risk/{_RECEIPT_TOKEN_ID}/breakdown")

        assert response.status_code == 422
        assert response.json()["detail"] == "bad receipt token shape"
    finally:
        app.dependency_overrides.pop(get_crypto_lending_risk_service, None)


def test_breakdown_returns_503_share_data_missing() -> None:
    service = _make_service()
    service.get_risk_breakdown_legacy.side_effect = MissingShareError("no active allocation")
    app.dependency_overrides[get_crypto_lending_risk_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/risk/{_RECEIPT_TOKEN_ID}/breakdown")

        assert response.status_code == 503
        assert response.json()["detail"]["code"] == "share_data_missing"
    finally:
        app.dependency_overrides.pop(get_crypto_lending_risk_service, None)
