"""Integration tests for ``TokenCatalogRepository`` chain+address lookups.

Exercises the SQL path that backs the
``/v1/tokens/{chain_id}/{token_address}`` endpoint. Reuses the shared seed
data (WETH on chain 1) provided by the integration conftest.
"""

import datetime as dt
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.token_catalog_repository import TokenCatalogRepository
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import insert_token


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repository(async_db_url: str):
    engine = create_async_engine(async_db_url)
    try:
        yield TokenCatalogRepository(engine)
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
async def test_get_latest_price_tie_resolves_to_highest_oracle_id(repository, db_url) -> None:
    """Onchain rows tied on all four ordering keys resolve to the higher oracle_id.

    Same-block rows from two oracles share the block timestamp, so the
    frozen-source replant shape (a retired oracle re-emitting on a republished
    block next to a live one) ties (timestamp, block_number, block_version,
    processing_version); oracle_id DESC keeps the quote stable across calls.
    The stale lower-oracle_id row is inserted first to bias an un-tiebroken
    top-1 sort toward the wrong answer.
    """
    conn = await asyncpg.connect(db_url)
    try:
        token_id = await insert_token(conn, "tieCat", 6, b"\x7a" * 20)
        stale_id = await conn.fetchval(
            "INSERT INTO oracle (name, display_name, chain_id, address) "
            "VALUES ('cat_tie_stale', 'Catalog tie stale oracle', 1, $1) RETURNING id",
            b"\x7b" * 20,
        )
        fresh_id = await conn.fetchval(
            "INSERT INTO oracle (name, display_name, chain_id, address) "
            "VALUES ('cat_tie_fresh', 'Catalog tie fresh oracle', 1, $1) RETURNING id",
            b"\x7c" * 20,
        )
        assert stale_id < fresh_id, "seed premise broken: stale oracle must have the lower id"
        tied_at = dt.datetime(2026, 3, 1, tzinfo=dt.UTC)
        for oracle_id, price in ((stale_id, Decimal("1.00")), (fresh_id, Decimal("1.25"))):
            await conn.execute(
                "INSERT INTO onchain_token_price "
                "(token_id, oracle_id, block_number, block_version, timestamp, price_usd) "
                "VALUES ($1, $2, 5000, 1, $3, $4)",
                token_id,
                oracle_id,
                tied_at,
                price,
            )
    finally:
        await conn.close()

    quote = await repository.get_latest_price(token_id)
    assert quote is not None
    assert quote.source_name == "cat_tie_fresh"
    assert quote.price_usd == Decimal("1.25")
