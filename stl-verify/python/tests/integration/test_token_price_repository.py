# tests/integration/test_token_price_repository.py
import datetime
from decimal import Decimal
from typing import cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.token_price_repository import OffchainTokenPriceRepository
from tests.integration.conftest import store_test_ids


async def _insert_offchain_price(
    conn: asyncpg.Connection,
    token_id: int,
    source_id: int,
    price_usd: str,
    ts_offset_seconds: int = 0,
) -> None:
    ts = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=ts_offset_seconds)
    await conn.execute(
        """
        INSERT INTO offchain_token_price (token_id, source_id, timestamp, price_usd)
        VALUES ($1, $2, $3, $4::numeric(30,18))
        ON CONFLICT (token_id, source_id, timestamp) DO NOTHING
        """,
        token_id, source_id, ts, price_usd,
    )


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _seed_data(db_url: str) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        source_id = cast(int, await conn.fetchval(
            "SELECT id FROM offchain_price_source WHERE name = 'coingecko'"
        ))
        weth_id = cast(int, await conn.fetchval("SELECT id FROM token WHERE symbol = 'WETH' AND chain_id = 1"))
        cbbtc_id = cast(int, await conn.fetchval("SELECT id FROM token WHERE symbol = 'cbBTC' AND chain_id = 1"))

        # Insert two prices for WETH (different timestamps); latest should be returned
        await _insert_offchain_price(conn, weth_id, source_id, "2000.000000000000000000", ts_offset_seconds=-60)
        await _insert_offchain_price(conn, weth_id, source_id, "2100.000000000000000000", ts_offset_seconds=0)
        await _insert_offchain_price(conn, cbbtc_id, source_id, "50000.000000000000000000")

        await store_test_ids(conn, {"weth_id": weth_id, "cbbtc_id": cbbtc_id})
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
        yield OffchainTokenPriceRepository(engine)
    finally:
        await engine.dispose()


@pytest.mark.asyncio(loop_scope="module")
async def test_returns_latest_price_per_token(repository, test_ids: dict[str, int]) -> None:
    result = await repository.get_prices([test_ids["weth_id"], test_ids["cbbtc_id"]])

    assert test_ids["weth_id"] in result
    assert result[test_ids["weth_id"]] == Decimal("2100.000000000000000000")

    assert test_ids["cbbtc_id"] in result
    assert result[test_ids["cbbtc_id"]] == Decimal("50000.000000000000000000")


@pytest.mark.asyncio(loop_scope="module")
async def test_missing_token_absent_from_result(repository) -> None:
    result = await repository.get_prices([99999])
    assert 99999 not in result


@pytest.mark.asyncio(loop_scope="module")
async def test_empty_token_ids_returns_empty_dict(repository) -> None:
    result = await repository.get_prices([])
    assert result == {}
