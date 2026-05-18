"""Integration tests for the new chain+address read methods on ``PostgresTokenCatalogRepository``.

These exercise the SQL path that backs the new
``/v1/tokens/{chain_id}/{token_address}`` endpoints. They reuse the
shared seed data (WETH on chain 1) provided by the integration conftest.
"""

from datetime import UTC, datetime
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.token_catalog_repository import PostgresTokenCatalogRepository
from app.domain.entities.allocation import EthAddress


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _seed_price(db_url: str) -> None:
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


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repository(async_db_url: str, _seed_price: None):
    engine = create_async_engine(async_db_url)
    try:
        yield PostgresTokenCatalogRepository(engine)
    finally:
        await engine.dispose()


_WETH_ADDRESS = EthAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")


@pytest.mark.asyncio(loop_scope="module")
async def test_get_token_by_chain_and_address_returns_known_token(repository) -> None:
    token = await repository.get_token_by_chain_and_address(1, _WETH_ADDRESS)

    assert token is not None
    assert token.chain_id == 1
    assert token.symbol == "WETH"


@pytest.mark.asyncio(loop_scope="module")
async def test_get_token_by_chain_and_address_is_case_insensitive(repository) -> None:
    """Address bytes are compared by value, not hex case."""
    lower = EthAddress("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
    token = await repository.get_token_by_chain_and_address(1, lower)
    assert token is not None
    assert token.symbol == "WETH"


@pytest.mark.asyncio(loop_scope="module")
async def test_get_token_by_chain_and_address_returns_none_when_missing(repository) -> None:
    missing = EthAddress("0x" + "ff" * 20)
    result = await repository.get_token_by_chain_and_address(1, missing)
    assert result is None


@pytest.mark.asyncio(loop_scope="module")
async def test_get_latest_price_by_chain_and_address_returns_none_when_missing(repository) -> None:
    """No price seeded for the all-ff address — should return None (not raise)."""
    missing = EthAddress("0x" + "ff" * 20)
    result = await repository.get_latest_price_by_chain_and_address(1, missing)
    assert result is None


@pytest.mark.asyncio(loop_scope="module")
async def test_get_latest_price_by_chain_and_address_returns_quote(repository) -> None:
    quote = await repository.get_latest_price_by_chain_and_address(1, _WETH_ADDRESS)

    assert quote is not None
    assert quote.price_usd == Decimal("2500.50")
    assert quote.source_type == "offchain"
    assert quote.source_name == "coingecko"
