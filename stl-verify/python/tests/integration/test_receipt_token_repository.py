from typing import cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.receipt_token_repository import ReceiptTokenRepository
from tests.integration.conftest import store_test_ids


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _seed_data(db_url: str) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        rt_id = cast(int, await conn.fetchval("SELECT id FROM receipt_token WHERE symbol = 'spWETH'"))
        await store_test_ids(conn, {"receipt_token_id": rt_id})
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def test_ids(db_url: str, _seed_data: None) -> dict[str, int]:
    conn = await asyncpg.connect(db_url)
    try:
        rows = await conn.fetch("SELECT key, val FROM _test_ids")
        return {row["key"]: row["val"] for row in rows}
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repository(async_db_url: str, _seed_data: None):
    engine = create_async_engine(async_db_url)
    try:
        yield ReceiptTokenRepository(engine)
    finally:
        await engine.dispose()


@pytest.mark.asyncio(loop_scope="module")
async def test_returns_receipt_token_info(repository, test_ids) -> None:
    result = await repository.get(test_ids["receipt_token_id"])
    assert result is not None
    assert result.protocol_name == "SparkLend"
    assert result.chain_id == 1
    assert result.receipt_token_id == test_ids["receipt_token_id"]


@pytest.mark.asyncio(loop_scope="module")
async def test_returns_none_for_unknown_id(repository) -> None:
    result = await repository.get(99999)
    assert result is None
