"""token_total_supply_current: the trigger keeps the newest supply per key, and the share lookup reads it.

"Newest" is (block_number, block_version, processing_version), the order the
share lookup requires, so the cache must equal DISTINCT ON over
token_total_supply in that order.

Every scenario seeds its own token, so the module's shared database keeps the
scenarios independent of each other and of ordering.
"""

import datetime as dt
from collections.abc import AsyncIterator
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.adapters.postgres.allocation_share_repository import fetch_share
from tests.integration.seed import insert_allocation_position, insert_token, insert_token_total_supply

_BLOCK = 22_000_000
_OBSERVED_AT = dt.datetime(2026, 8, 27, 12, 0, tzinfo=dt.UTC)
_TX = "cd" * 32


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def conn(db_url: str) -> AsyncIterator[asyncpg.Connection]:
    """One connection for the module's isolated database."""
    connection = await asyncpg.connect(db_url)
    try:
        yield connection
    finally:
        await connection.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def engine(async_db_url: str) -> AsyncIterator[AsyncEngine]:
    """The engine the share lookup under test reads through."""
    engine = create_async_engine(async_db_url)
    try:
        yield engine
    finally:
        await engine.dispose()


async def _cached(conn: asyncpg.Connection, token_id: int) -> asyncpg.Record:
    return await conn.fetchrow(
        "SELECT total_supply, block_number, block_version, processing_version "
        "FROM token_total_supply_current WHERE chain_id = 1 AND token_id = $1",
        token_id,
    )


@pytest.mark.parametrize(
    ("label", "token_byte", "block", "advance", "winning_version"),
    [
        ("block", 0xB1, _BLOCK + 1, {}, (_BLOCK + 1, 0, 0)),
        ("reorg", 0xB2, _BLOCK, {"block_version": 1}, (_BLOCK, 1, 0)),
        ("repro", 0xB3, _BLOCK, {"build_id": 1}, (_BLOCK, 0, 1)),
    ],
)
@pytest.mark.asyncio(loop_scope="module")
async def test_row_newer_by_one_tuple_component_replaces_the_cached_row(
    conn: asyncpg.Connection,
    label: str,
    token_byte: int,
    block: int,
    advance: dict[str, int],
    winning_version: tuple[int, int, int],
) -> None:
    """A row that outranks the cached one on any component of the newer-wins tuple replaces it.

    The first row sits at (_BLOCK, 0, 0) and the second advances exactly one
    component, so each case fails if that component is dropped from the guard.
    """
    token_id = await insert_token(conn, f"TSC{label}", 18, bytes([token_byte]) * 20)

    await insert_token_total_supply(
        conn, token_id=token_id, total_supply=100, block=_BLOCK, block_timestamp=_OBSERVED_AT
    )
    await insert_token_total_supply(
        conn, token_id=token_id, total_supply=175, block=block, block_timestamp=_OBSERVED_AT, **advance
    )

    row = await _cached(conn, token_id)
    assert row["total_supply"] == 175
    assert (row["block_number"], row["block_version"], row["processing_version"]) == winning_version


@pytest.mark.asyncio(loop_scope="module")
async def test_backfilled_older_block_does_not_regress_the_cached_row(conn: asyncpg.Connection) -> None:
    """A backfill filling a gap below the current block leaves the cache alone."""
    token_id = await insert_token(conn, "TSCOLDER", 18, b"\xb6" * 20)

    for block, supply in [(_BLOCK + 500, 5000), (_BLOCK + 100, 1000)]:
        await insert_token_total_supply(conn, token_id=token_id, total_supply=supply, block=block)

    row = await _cached(conn, token_id)
    assert row["total_supply"] == 5000
    assert row["block_number"] == _BLOCK + 500


@pytest.mark.asyncio(loop_scope="module")
async def test_share_lookup_reads_the_supply_from_the_cache(conn: asyncpg.Connection, engine: AsyncEngine) -> None:
    """fetch_share divides the pinned balance by the cached supply, not by the history's.

    The cache row is overwritten after seeding so the two disagree; only the
    cache path can produce the expected ratio.
    """
    token_id = await insert_token(conn, "TSCSHARE", 18, b"\xb4" * 20)
    prime_id = await conn.fetchval(
        "INSERT INTO prime (name, vault_address) VALUES ('tts_current', $1) "
        "ON CONFLICT (name) DO UPDATE SET vault_address = EXCLUDED.vault_address RETURNING id",
        b"\x52" * 20,
    )
    proxy_hex = "b5" * 20

    await insert_allocation_position(
        conn,
        token_id=token_id,
        prime_id=prime_id,
        proxy_hex=proxy_hex,
        balance=25,
        block=_BLOCK,
        tx=_TX,
        direction="in",
    )
    await insert_token_total_supply(conn, token_id=token_id, total_supply=200, block=_BLOCK + 1)
    await conn.execute(
        "UPDATE token_total_supply_current SET total_supply = 400 WHERE chain_id = 1 AND token_id = $1", token_id
    )

    share = await fetch_share(engine, chain_id=1, token_id=token_id, wallet_address=bytes.fromhex(proxy_hex))

    assert share == Decimal(25) / Decimal(400)
