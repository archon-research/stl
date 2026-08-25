"""allocation_position_current: the trigger keeps the newest row per key.

The cache is what the receipt-position reads select from instead of walking the
allocation_position history, so "newest" here must mean exactly what those reads
meant: (block_number, block_version, processing_version, log_index), with
several rows per key inside one block being the normal case.

Every scenario seeds its own token and proxy, so the module's shared database
keeps the scenarios independent of each other and of ordering.
"""

from collections.abc import AsyncIterator

import asyncpg
import pytest
import pytest_asyncio

from tests.integration.seed import insert_allocation_position, insert_token

_BLOCK = 21_000_000
_TX = "ab" * 32


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def conn(db_url: str) -> AsyncIterator[asyncpg.Connection]:
    """One connection for the module's isolated database."""
    connection = await asyncpg.connect(db_url)
    try:
        yield connection
    finally:
        await connection.close()


@pytest_asyncio.fixture(loop_scope="module")
async def prime_id(conn: asyncpg.Connection) -> int:
    """A prime to hang the seeded proxies off."""
    return await conn.fetchval(
        "INSERT INTO prime (name, vault_address) VALUES ('alloc_current', $1) "
        "ON CONFLICT (name) DO UPDATE SET vault_address = EXCLUDED.vault_address RETURNING id",
        b"\x51" * 20,
    )


async def _cached(conn: asyncpg.Connection, proxy_hex: str, token_id: int) -> asyncpg.Record:
    return await conn.fetchrow(
        "SELECT balance, block_number, log_index, direction FROM allocation_position_current "
        "WHERE chain_id = 1 AND proxy_address = $1 AND token_id = $2",
        bytes.fromhex(proxy_hex),
        token_id,
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_newer_block_replaces_the_cached_row(conn: asyncpg.Connection, prime_id: int) -> None:
    """A position row at a higher block replaces the cached row."""
    proxy_hex = "a1" * 20
    token_id = await insert_token(conn, "APCNEWER", 18, b"\xa1" * 20)

    for block, balance in [(_BLOCK, 100), (_BLOCK + 1, 250)]:
        await insert_allocation_position(
            conn,
            token_id=token_id,
            prime_id=prime_id,
            proxy_hex=proxy_hex,
            balance=balance,
            block=block,
            tx=_TX,
            direction="in",
        )

    row = await _cached(conn, proxy_hex, token_id)
    assert row["balance"] == 250
    assert row["block_number"] == _BLOCK + 1


@pytest.mark.asyncio(loop_scope="module")
async def test_backfilled_older_block_does_not_regress_the_cached_row(conn: asyncpg.Connection, prime_id: int) -> None:
    """A backfill filling a gap below the current block leaves the cache alone."""
    proxy_hex = "a2" * 20
    token_id = await insert_token(conn, "APCOLDER", 18, b"\xa2" * 20)

    for block, balance in [(_BLOCK + 500, 5000), (_BLOCK + 100, 1000)]:
        await insert_allocation_position(
            conn,
            token_id=token_id,
            prime_id=prime_id,
            proxy_hex=proxy_hex,
            balance=balance,
            block=block,
            tx=_TX,
            direction="in",
        )

    row = await _cached(conn, proxy_hex, token_id)
    assert row["balance"] == 5000
    assert row["block_number"] == _BLOCK + 500


@pytest.mark.parametrize(
    ("label", "proxy_byte", "ascending"),
    [("asc", 0xA3, True), ("desc", 0xA7, False)],
)
@pytest.mark.asyncio(loop_scope="module")
async def test_highest_log_index_wins_within_one_block(
    conn: asyncpg.Connection, prime_id: int, label: str, proxy_byte: int, ascending: bool
) -> None:
    """Deposit then full withdrawal in one block leaves the cache on the withdrawal.

    (block_number, block_version, processing_version) is identical across the two
    rows, so only log_index can separate them; either arrival order must land on
    the same winner.
    """
    proxy_hex = f"{proxy_byte:02x}" * 20
    token_id = await insert_token(conn, f"APCTIE{label}", 18, bytes([proxy_byte]) * 20)

    rows = [(0, 300, "in"), (1, 0, "out")]
    for log_index, balance, direction in rows if ascending else reversed(rows):
        await insert_allocation_position(
            conn,
            token_id=token_id,
            prime_id=prime_id,
            proxy_hex=proxy_hex,
            balance=balance,
            block=_BLOCK,
            tx=_TX,
            direction=direction,
            log_index=log_index,
        )

    row = await _cached(conn, proxy_hex, token_id)
    assert row["balance"] == 0
    assert row["log_index"] == 1
    assert row["direction"] == "out"


@pytest.mark.asyncio(loop_scope="module")
async def test_same_token_on_two_proxies_is_cached_separately(conn: asyncpg.Connection, prime_id: int) -> None:
    """proxy_address is part of the key, so one prime's proxies do not overwrite each other."""
    token_id = await insert_token(conn, "APCPROXY", 18, b"\xa4" * 20)

    for proxy_hex, balance in [("a5" * 20, 111), ("a6" * 20, 222)]:
        await insert_allocation_position(
            conn,
            token_id=token_id,
            prime_id=prime_id,
            proxy_hex=proxy_hex,
            balance=balance,
            block=_BLOCK,
            tx=_TX,
            direction="in",
        )

    assert (await _cached(conn, "a5" * 20, token_id))["balance"] == 111
    assert (await _cached(conn, "a6" * 20, token_id))["balance"] == 222
