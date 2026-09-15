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

from app.api.deps import get_crypto_lending_risk_service, get_direct_asset_lookup, get_receipt_token_lookup
from app.domain.entities.allocation import DirectAssetHolding
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.exceptions import MissingShareError, StaleShareError
from app.main import app
from app.services.crypto_lending_risk_service import CryptoLendingRiskService

_RECEIPT_TOKEN_ID = 1234
_RECEIPT_TOKEN_ADDRESS = "0x" + "cd" * 20


def _override_service(service: CryptoLendingRiskService):
    def _dep():
        return service

    return _dep


def _override_lookup(info: ReceiptTokenInfo | None = None):
    lookup = AsyncMock()
    lookup.get_by_chain_and_address = AsyncMock(return_value=info)

    def _dep():
        return lookup

    return _dep


def _override_direct_lookup(holding: DirectAssetHolding | None = None):
    lookup = AsyncMock()
    lookup.get_by_chain_and_address = AsyncMock(return_value=holding)

    def _dep():
        return lookup

    return _dep


def _make_service() -> AsyncMock:
    service = AsyncMock(spec=CryptoLendingRiskService)
    # Nothing to gate: the no-prime path here is a protocol whose legacy share
    # is genuinely pool-wide. tests/unit/auth/test_risk_authz.py drives the
    # other case against a real service.
    service.resolve_pool_prime.return_value = None
    return service


def test_bad_debt_returns_404_when_service_returns_none() -> None:
    service = _make_service()
    service.get_bad_debt_legacy.return_value = None
    app.dependency_overrides[get_crypto_lending_risk_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/risk/{_RECEIPT_TOKEN_ID}/bad-debt?gap_pct=0.1")

        assert response.status_code == 404
        assert response.json()["detail"] == "receipt token not found"
        service.get_bad_debt_legacy.assert_awaited_once_with(_RECEIPT_TOKEN_ID, Decimal("0.1"), None)
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
    service.get_risk_breakdown.return_value = None
    app.dependency_overrides[get_crypto_lending_risk_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/risk/{_RECEIPT_TOKEN_ID}/breakdown")

        assert response.status_code == 404
        assert response.json()["detail"] == "receipt token not found"
        # No prime_id query -> the no-prime path, with nothing to gate on.
        service.get_risk_breakdown.assert_awaited_once_with(_RECEIPT_TOKEN_ID, None, None)
    finally:
        app.dependency_overrides.pop(get_crypto_lending_risk_service, None)


def test_breakdown_forwards_prime_id_when_supplied() -> None:
    service = _make_service()
    service.get_risk_breakdown.return_value = None
    prime = "0x" + "ab" * 20
    app.dependency_overrides[get_crypto_lending_risk_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/risk/{_RECEIPT_TOKEN_ID}/breakdown?prime_id={prime}")

        assert response.status_code == 404
        awaited_id, awaited_prime, _ = service.get_risk_breakdown.await_args.args
        assert awaited_id == _RECEIPT_TOKEN_ID
        assert str(awaited_prime) == prime
    finally:
        app.dependency_overrides.pop(get_crypto_lending_risk_service, None)


def test_breakdown_returns_422_on_malformed_prime_id() -> None:
    service = _make_service()
    app.dependency_overrides[get_crypto_lending_risk_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/risk/{_RECEIPT_TOKEN_ID}/breakdown?prime_id=not-an-address")

        assert response.status_code == 422
        service.get_risk_breakdown.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(get_crypto_lending_risk_service, None)


def test_breakdown_by_address_forwards_prime_id() -> None:
    service = _make_service()
    service.get_risk_breakdown.return_value = None
    info = ReceiptTokenInfo(
        receipt_token_id=_RECEIPT_TOKEN_ID,
        protocol_id=3,
        underlying_token_id=42,
        receipt_token_address=bytes.fromhex("cd" * 20),
        chain_id=1,
        protocol_name="maple",
        receipt_token_token_id=555,
    )
    prime = "0x" + "ab" * 20
    app.dependency_overrides[get_crypto_lending_risk_service] = _override_service(service)
    app.dependency_overrides[get_receipt_token_lookup] = _override_lookup(info)
    app.dependency_overrides[get_direct_asset_lookup] = _override_direct_lookup(None)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/risk/1/{_RECEIPT_TOKEN_ADDRESS}/breakdown?prime_id={prime}")

        assert response.status_code == 404
        awaited_id, awaited_prime, _ = service.get_risk_breakdown.await_args.args
        assert awaited_id == _RECEIPT_TOKEN_ID
        assert str(awaited_prime) == prime
    finally:
        app.dependency_overrides.pop(get_crypto_lending_risk_service, None)
        app.dependency_overrides.pop(get_receipt_token_lookup, None)
        app.dependency_overrides.pop(get_direct_asset_lookup, None)


def test_breakdown_returns_422_on_value_error() -> None:
    service = _make_service()
    service.get_risk_breakdown.side_effect = ValueError("bad receipt token shape")
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
    service.get_risk_breakdown.side_effect = MissingShareError("no active allocation")
    app.dependency_overrides[get_crypto_lending_risk_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/risk/{_RECEIPT_TOKEN_ID}/breakdown")

        assert response.status_code == 503
        assert response.json()["detail"]["code"] == "share_data_missing"
    finally:
        app.dependency_overrides.pop(get_crypto_lending_risk_service, None)


_DIRECT_ADDRESS = "0x" + "ef" * 20


def test_breakdown_by_address_falls_back_to_direct_asset() -> None:
    holding = DirectAssetHolding(
        chain_id=1,
        token_id=42,
        token_address=_DIRECT_ADDRESS,
        symbol="RLUSD",
        balance=Decimal("1000"),
        amount_usd=Decimal("1000"),
    )
    service = _make_service()
    app.dependency_overrides[get_crypto_lending_risk_service] = _override_service(service)
    app.dependency_overrides[get_receipt_token_lookup] = _override_lookup(None)
    app.dependency_overrides[get_direct_asset_lookup] = _override_direct_lookup(holding)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/risk/1/{_DIRECT_ADDRESS}/breakdown")

        assert response.status_code == 200
        body = response.json()
        assert body["receipt_token_id"] == 42
        assert len(body["items"]) == 1
        assert body["items"][0]["symbol"] == "RLUSD"
        assert body["items"][0]["backing_pct"] == "100"
        service.get_risk_breakdown.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(get_crypto_lending_risk_service, None)
        app.dependency_overrides.pop(get_receipt_token_lookup, None)
        app.dependency_overrides.pop(get_direct_asset_lookup, None)


def test_breakdown_by_address_returns_404_when_neither_found() -> None:
    service = _make_service()
    app.dependency_overrides[get_crypto_lending_risk_service] = _override_service(service)
    app.dependency_overrides[get_receipt_token_lookup] = _override_lookup(None)
    app.dependency_overrides[get_direct_asset_lookup] = _override_direct_lookup(None)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/risk/1/{_DIRECT_ADDRESS}/breakdown")

        assert response.status_code == 404
        assert response.json()["detail"] == "Receipt token not found"
    finally:
        app.dependency_overrides.pop(get_crypto_lending_risk_service, None)
        app.dependency_overrides.pop(get_receipt_token_lookup, None)
        app.dependency_overrides.pop(get_direct_asset_lookup, None)


def test_breakdown_by_address_prefers_receipt_token_over_direct() -> None:
    info = ReceiptTokenInfo(
        receipt_token_id=_RECEIPT_TOKEN_ID,
        protocol_id=3,
        underlying_token_id=42,
        receipt_token_address=bytes.fromhex("cd" * 20),
        chain_id=1,
        protocol_name="sparklend",
        receipt_token_token_id=555,
    )
    service = _make_service()
    service.get_risk_breakdown.return_value = None
    app.dependency_overrides[get_crypto_lending_risk_service] = _override_service(service)
    app.dependency_overrides[get_receipt_token_lookup] = _override_lookup(info)
    app.dependency_overrides[get_direct_asset_lookup] = _override_direct_lookup(None)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/risk/1/{_RECEIPT_TOKEN_ADDRESS}/breakdown")

        assert response.status_code == 404
        service.get_risk_breakdown.assert_awaited_once()
    finally:
        app.dependency_overrides.pop(get_crypto_lending_risk_service, None)
        app.dependency_overrides.pop(get_receipt_token_lookup, None)
        app.dependency_overrides.pop(get_direct_asset_lookup, None)
