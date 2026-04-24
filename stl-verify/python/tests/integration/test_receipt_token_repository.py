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
        protocol_id = cast(
            int,
            await conn.fetchval("SELECT id FROM protocol WHERE name = 'SparkLend' AND chain_id = 1"),
        )
        token_id = cast(
            int,
            await conn.fetchval("SELECT id FROM token WHERE symbol = 'WETH' AND chain_id = 1"),
        )
        receipt_token_address = bytes.fromhex("59cD1C87501baa753d0B5B5Ab5D8416A45cD71DB")
        # Insert the receipt-token's own token row — the repository's JOIN to
        # `token` on (chain_id, address) requires this.
        await conn.execute(
            """
            INSERT INTO token (chain_id, address, symbol, decimals)
            VALUES (1, $1, 'spWETH', 18)
            ON CONFLICT (chain_id, address) DO NOTHING
            """,
            receipt_token_address,
        )
        rt_id = cast(
            int,
            await conn.fetchval(
                """
                INSERT INTO receipt_token
                    (protocol_id, underlying_token_id, receipt_token_address, symbol,
                     created_at_block, chain_id)
                VALUES ($1, $2, $3, 'spWETH', 16776401, 1)
                ON CONFLICT ON CONSTRAINT receipt_token_chain_address_unique
                    DO UPDATE SET symbol = EXCLUDED.symbol
                RETURNING id
                """,
                protocol_id,
                token_id,
                receipt_token_address,
            ),
        )
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
    # receipt_token_token_id is populated from the JOIN to token(chain_id, address);
    # it's the token's numeric id for the receipt token address, not receipt_token.id.
    assert isinstance(result.receipt_token_token_id, int)
    assert result.receipt_token_token_id > 0


@pytest.mark.asyncio(loop_scope="module")
async def test_returns_none_for_unknown_id(repository) -> None:
    result = await repository.get(99999)
    assert result is None
