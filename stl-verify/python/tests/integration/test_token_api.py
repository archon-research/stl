"""Integration tests for chain+address token endpoints
(``/v1/tokens/{chain_id}/{token_address}{,/price}``).

Exercises the FastAPI handlers through ``TokenCatalogService`` to
``PostgresTokenCatalogRepository`` against a real Postgres.
"""

from datetime import UTC, datetime
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.config import Settings
from app.main import create_app

_WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
_USDC_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _seed_weth_price(db_url: str) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        token_id = await conn.fetchval("SELECT id FROM token WHERE symbol = 'WETH' AND chain_id = 1")
        source_id = await conn.fetchval("SELECT id FROM offchain_price_source WHERE name = 'coingecko'")
        await conn.execute(
            """
            INSERT INTO offchain_token_price (token_id, source_id, timestamp, price_usd)
            VALUES ($1, $2, $3, $4)
            """,
            token_id,
            source_id,
            datetime(2026, 1, 1, tzinfo=UTC),
            Decimal("2500.50"),
        )
    finally:
        await conn.close()


@pytest.fixture(scope="module")
def client(async_db_url: str, tmp_path_factory, _seed_weth_price: None):
    empty_mapping = tmp_path_factory.mktemp("cfg") / "empty_mapping.json"
    empty_mapping.write_text("{}")
    test_app = create_app(
        Settings.model_validate({"database_url": SecretStr(async_db_url), "suraf_mappings_file": empty_mapping})
    )
    with TestClient(test_app) as c:
        yield c


# ---------------------------------------------------------------------------
# Chain+address endpoints: /v1/tokens/{chain_id}/{token_address}{,/price}
# ---------------------------------------------------------------------------


def test_get_token_by_chain_and_address_returns_row(client: TestClient) -> None:
    response = client.get(f"/v1/tokens/1/{_WETH_ADDRESS}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["chain_id"] == 1
    assert payload["symbol"] == "WETH"
    assert payload["decimals"] == 18
    assert payload["address"].lower() == _WETH_ADDRESS.lower()


def test_get_token_by_chain_and_address_accepts_mixed_case(client: TestClient) -> None:
    lower = _WETH_ADDRESS.lower()
    response_lower = client.get(f"/v1/tokens/1/{lower}")
    response_mixed = client.get(f"/v1/tokens/1/{_WETH_ADDRESS}")

    assert response_lower.status_code == 200
    assert response_mixed.status_code == 200
    assert response_lower.json()["id"] == response_mixed.json()["id"]


def test_get_token_by_chain_and_address_returns_404_when_unknown(client: TestClient) -> None:
    response = client.get("/v1/tokens/1/0x" + "ff" * 20)

    assert response.status_code == 404
    assert response.json()["detail"] == "Token not found"


def test_get_token_by_chain_and_address_returns_422_for_malformed_address(
    client: TestClient,
) -> None:
    response = client.get("/v1/tokens/1/0xnotanaddress")

    assert response.status_code == 422


def test_get_token_by_chain_and_address_returns_422_for_zero_chain_id(
    client: TestClient,
) -> None:
    response = client.get(f"/v1/tokens/0/{_WETH_ADDRESS}")

    assert response.status_code == 422


def test_get_token_price_by_chain_and_address_returns_quote(client: TestClient) -> None:
    response = client.get(f"/v1/tokens/1/{_WETH_ADDRESS}/price")

    assert response.status_code == 200
    payload = response.json()
    assert Decimal(payload["price_usd"]) == Decimal("2500.50")
    assert payload["source_name"] == "coingecko"
    assert payload["is_stale"] is False
    assert payload["staleness_reason"] is None


def test_get_token_price_by_chain_and_address_returns_404_when_token_missing(
    client: TestClient,
) -> None:
    response = client.get("/v1/tokens/1/0x" + "ff" * 20 + "/price")

    assert response.status_code == 404
    assert response.json()["detail"] == "Token not found"


def test_get_token_price_by_chain_and_address_returns_missing_quote_when_no_price(
    client: TestClient,
) -> None:
    """USDC seed exists, but no price was inserted — should report missing_quote."""
    response = client.get(f"/v1/tokens/1/{_USDC_ADDRESS}/price")

    assert response.status_code == 200
    payload = response.json()
    assert payload["is_stale"] is True
    assert payload["staleness_reason"] == "missing_quote"
    assert payload["price_usd"] is None
