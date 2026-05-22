"""Integration tests for ``PostgresTokenCatalogRepository`` chain+address lookups.

Exercises the SQL path that backs the
``/v1/tokens/{chain_id}/{token_address}`` endpoint. Reuses the shared seed
data (WETH on chain 1) provided by the integration conftest.
"""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.token_catalog_repository import PostgresTokenCatalogRepository
from app.domain.entities.allocation import EthAddress


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repository(async_db_url: str):
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
