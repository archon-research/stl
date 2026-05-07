from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from app.domain.entities.token_catalog import TokenMetadata, TokenPriceQuote
from app.main import app
from app.services.token_catalog_service import TokenCatalogService


@pytest.fixture(autouse=True)
def _clear_dependency_overrides():
    yield
    app.dependency_overrides.clear()


def _make_service(
    *,
    tokens: list[TokenMetadata] | None = None,
    token: TokenMetadata | None = None,
    price: TokenPriceQuote | None = None,
) -> TokenCatalogService:
    service = AsyncMock(spec=TokenCatalogService)
    service.list_tokens.return_value = tokens or []
    service.get_token.return_value = token
    service.get_latest_price.return_value = price
    return service


def _override_service(service: TokenCatalogService):
    async def _dep():
        yield service

    return _dep


def _token() -> TokenMetadata:
    return TokenMetadata(
        id=1,
        chain_id=1,
        address="0x" + "ab" * 20,
        symbol="USDC",
        decimals=6,
        updated_at=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
        metadata={"kind": "stable"},
    )


def _price() -> TokenPriceQuote:
    return TokenPriceQuote(
        token_id=1,
        source_type="onchain",
        source_id=7,
        source_name="chainlink",
        source_display_name="Chainlink",
        price_usd=Decimal("1.00"),
        timestamp=datetime(2026, 3, 5, 12, 5, tzinfo=UTC),
        staleness_seconds=12,
    )


def test_list_tokens_returns_rows_and_applies_filters():
    from app.api.v1 import tokens

    token = _token()
    service = _make_service(tokens=[token])
    app.dependency_overrides[tokens._get_service] = _override_service(service)
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/v1/tokens", params={"chain_id": 1, "symbol": "US", "limit": 25})

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["symbol"] == "USDC"
    service.list_tokens.assert_awaited_once_with(chain_id=1, symbol="US", limit=25)


def test_list_tokens_returns_422_for_invalid_limit():
    from app.api.v1 import tokens

    service = _make_service(tokens=[])
    app.dependency_overrides[tokens._get_service] = _override_service(service)
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/v1/tokens", params={"limit": 0})

    assert response.status_code == 422
    service.list_tokens.assert_not_awaited()


def test_get_token_returns_404_when_missing():
    from app.api.v1 import tokens

    service = _make_service(token=None)
    app.dependency_overrides[tokens._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/tokens/999")

    assert response.status_code == 404
    assert response.json()["detail"] == "Token not found"


def test_get_token_price_returns_404_when_token_missing():
    from app.api.v1 import tokens

    service = _make_service(token=None, price=_price())
    app.dependency_overrides[tokens._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/tokens/1/price")

    assert response.status_code == 404
    assert response.json()["detail"] == "Token not found"
    service.get_latest_price.assert_not_awaited()


def test_get_token_price_returns_200_with_staleness_indicator_when_quote_missing():
    from app.api.v1 import tokens

    service = _make_service(token=_token(), price=None)
    app.dependency_overrides[tokens._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/tokens/1/price")

    assert response.status_code == 200
    payload = response.json()
    assert payload["token_id"] == 1
    assert payload["price_usd"] is None
    assert payload["timestamp"] is None
    assert payload["staleness_seconds"] is None
    assert payload["is_stale"] is True
    assert payload["staleness_reason"] == "missing_quote"


def test_get_token_price_returns_quote_payload():
    from app.api.v1 import tokens

    service = _make_service(token=_token(), price=_price())
    app.dependency_overrides[tokens._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/tokens/1/price")

    assert response.status_code == 200
    payload = response.json()
    assert payload["token_id"] == 1
    assert payload["source_name"] == "chainlink"
    assert payload["price_usd"] == "1.00"
    assert payload["is_stale"] is False
    assert payload["staleness_reason"] is None


def test_list_tokens_returns_empty_list_when_no_matches():
    from app.api.v1 import tokens

    service = _make_service(tokens=[])
    app.dependency_overrides[tokens._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/tokens", params={"symbol": "NONEXISTENT"})

    assert response.status_code == 200
    assert response.json() == []
    service.list_tokens.assert_awaited_once()


def test_list_tokens_handles_partial_symbol_match():
    from app.api.v1 import tokens

    token = _token()
    service = _make_service(tokens=[token])
    app.dependency_overrides[tokens._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/tokens", params={"symbol": "USD"})

    assert response.status_code == 200
    assert len(response.json()) == 1
    service.list_tokens.assert_awaited_once_with(chain_id=None, symbol="USD", limit=100)


def test_list_tokens_filters_by_chain_id():
    from app.api.v1 import tokens

    token = _token()
    service = _make_service(tokens=[token])
    app.dependency_overrides[tokens._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/tokens", params={"chain_id": 42161})

    assert response.status_code == 200
    service.list_tokens.assert_awaited_once_with(chain_id=42161, symbol=None, limit=100)


def test_list_tokens_returns_422_for_limit_too_large():
    from app.api.v1 import tokens

    service = _make_service(tokens=[])
    app.dependency_overrides[tokens._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/tokens", params={"limit": 600})

    assert response.status_code == 422
    service.list_tokens.assert_not_awaited()


def test_get_token_returns_row():
    from app.api.v1 import tokens

    token = _token()
    service = _make_service(token=token)
    app.dependency_overrides[tokens._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/tokens/1")

    assert response.status_code == 200
    payload = response.json()
    assert payload["id"] == 1
    assert payload["symbol"] == "USDC"
    assert payload["decimals"] == 6
    service.get_token.assert_awaited_once_with(1)


def test_list_tokens_returns_500_when_service_errors():
    from app.api.v1 import tokens

    service = _make_service(tokens=[])
    service.list_tokens.side_effect = ValueError("db failure")
    app.dependency_overrides[tokens._get_service] = _override_service(service)
    try:
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get("/v1/tokens")

        assert response.status_code == 500
    finally:
        app.dependency_overrides.pop(tokens._get_service, None)


def test_get_token_price_returns_500_when_service_errors():
    from app.api.v1 import tokens

    service = _make_service(token=_token())
    service.get_latest_price.side_effect = ValueError("db failure")
    app.dependency_overrides[tokens._get_service] = _override_service(service)
    try:
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get("/v1/tokens/1/price")

        assert response.status_code == 500
    finally:
        app.dependency_overrides.pop(tokens._get_service, None)
