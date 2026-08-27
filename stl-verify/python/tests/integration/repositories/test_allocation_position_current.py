"""allocation_position_current: the trigger keeps the newest row per key.

The cache is what the allocation latest-row reads select from instead of walking
the allocation_position history, so "newest" here must mean exactly what those
reads mean: (block_number, block_version, block_timestamp, log_index, direction,
tx_hash, processing_version), with several rows per key inside one block being
the normal case. created_at is the cache row's own write time — set on the first
insert and moved by every overwrite — and ranks nowhere in that comparison.

Every scenario seeds its own token and proxy, so the module's shared database
keeps the scenarios independent of each other and of ordering.
"""

import datetime as dt
from collections.abc import AsyncIterator

import asyncpg
import pytest
import pytest_asyncio

from tests.integration.seed import insert_allocation_position, insert_token

_BLOCK = 21_000_000
_TX = "ab" * 32
_SWEEP_TX = "00" * 32
# Rows of one block share a block time, as the tracker writes them; passed
# explicitly so the seeding clock cannot stand in for the term under test.
_BLOCK_AT = dt.datetime(2026, 8, 25, 12, 0, tzinfo=dt.UTC)


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
        "SELECT balance, block_number, block_version, block_timestamp, log_index, direction, "
        "tx_hash, processing_version, created_at "
        "FROM allocation_position_current "
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

    Every term above log_index is identical across the two rows, so only
    log_index can separate them; either arrival order must land on the same
    winner.
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
            created_at=_BLOCK_AT,
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


@pytest.mark.asyncio(loop_scope="module")
async def test_reorg_replacement_wins_at_a_lower_log_index(conn: asyncpg.Connection, prime_id: int) -> None:
    """block_version ranks above log_index, so a republished block wins wherever its log lands."""
    proxy_hex = "a8" * 20
    token_id = await insert_token(conn, "APCREORG", 18, b"\xa8" * 20)

    for balance, block_version, log_index in [(44, 0, 5), (440, 1, 1)]:
        await insert_allocation_position(
            conn,
            token_id=token_id,
            prime_id=prime_id,
            proxy_hex=proxy_hex,
            balance=balance,
            block=_BLOCK,
            created_at=_BLOCK_AT,
            tx=_TX,
            direction="in",
            block_version=block_version,
            log_index=log_index,
        )

    row = await _cached(conn, proxy_hex, token_id)
    assert row["balance"] == 440
    assert (row["block_version"], row["log_index"]) == (1, 1)


@pytest.mark.asyncio(loop_scope="module")
async def test_reprocessed_row_wins_over_its_own_original(conn: asyncpg.Connection, prime_id: int) -> None:
    """A correction to one row wins it: same identity, fresh build_id, processing_version 1."""
    proxy_hex = "a9" * 20
    token_id = await insert_token(conn, "APCREPROC", 18, b"\xa9" * 20)

    for build_id, balance in [(0, 77), (1, 770)]:
        await insert_allocation_position(
            conn,
            token_id=token_id,
            prime_id=prime_id,
            proxy_hex=proxy_hex,
            balance=balance,
            block=_BLOCK,
            created_at=_BLOCK_AT,
            tx=_TX,
            direction="in",
            build_id=build_id,
        )

    row = await _cached(conn, proxy_hex, token_id)
    assert row["balance"] == 770
    assert row["processing_version"] == 1


@pytest.mark.asyncio(loop_scope="module")
async def test_reprocessed_row_does_not_outrank_a_later_log(conn: asyncpg.Connection, prime_id: int) -> None:
    """processing_version ranks last, so a correction to one log never beats a different log."""
    proxy_hex = "aa" * 20
    token_id = await insert_token(conn, "APCREPROCLOG", 18, b"\xaa" * 20)

    for build_id, balance in [(0, 77), (1, 770)]:
        await insert_allocation_position(
            conn,
            token_id=token_id,
            prime_id=prime_id,
            proxy_hex=proxy_hex,
            balance=balance,
            block=_BLOCK,
            created_at=_BLOCK_AT,
            tx=_TX,
            direction="in",
            build_id=build_id,
        )
    await insert_allocation_position(
        conn,
        token_id=token_id,
        prime_id=prime_id,
        proxy_hex=proxy_hex,
        balance=7,
        block=_BLOCK,
        created_at=_BLOCK_AT,
        tx="cd" * 32,
        direction="in",
        log_index=9,
    )

    row = await _cached(conn, proxy_hex, token_id)
    assert row["balance"] == 7
    assert (row["log_index"], row["processing_version"]) == (9, 0)


@pytest.mark.parametrize(
    ("label", "proxy_byte", "sweep_first"),
    [("event_first", 0xAB, False), ("sweep_first", 0xAC, True)],
)
@pytest.mark.asyncio(loop_scope="module")
async def test_sweep_wins_its_collision_with_an_event_row(
    conn: asyncpg.Connection, prime_id: int, label: str, proxy_byte: int, sweep_first: bool
) -> None:
    """A sweep and an event row collide at log_index 0; the sweep wins in either arrival order."""
    proxy_hex = f"{proxy_byte:02x}" * 20
    token_id = await insert_token(conn, f"APCSWEEP{label}", 18, bytes([proxy_byte]) * 20)

    rows = [(60, "in", _TX), (66, "sweep", _SWEEP_TX)]
    for balance, direction, tx in reversed(rows) if sweep_first else rows:
        await insert_allocation_position(
            conn,
            token_id=token_id,
            prime_id=prime_id,
            proxy_hex=proxy_hex,
            balance=balance,
            block=_BLOCK,
            created_at=_BLOCK_AT,
            tx=tx,
            direction=direction,
        )

    row = await _cached(conn, proxy_hex, token_id)
    assert row["balance"] == 66
    assert row["direction"] == "sweep"


@pytest.mark.asyncio(loop_scope="module")
async def test_cached_block_timestamp_is_the_winning_rows_block_time(conn: asyncpg.Connection, prime_id: int) -> None:
    """block_timestamp carries the history row's created_at; the cache's own write time is separate."""
    proxy_hex = "ad" * 20
    token_id = await insert_token(conn, "APCBLOCKTIME", 18, b"\xad" * 20)

    await insert_allocation_position(
        conn,
        token_id=token_id,
        prime_id=prime_id,
        proxy_hex=proxy_hex,
        balance=12,
        block=_BLOCK,
        created_at=_BLOCK_AT,
        tx=_TX,
        direction="in",
    )

    row = await _cached(conn, proxy_hex, token_id)
    assert row["block_timestamp"] == _BLOCK_AT
    assert row["created_at"] > _BLOCK_AT


@pytest.mark.parametrize(
    ("label", "proxy_byte", "second_block", "wins"),
    [("accepted", 0xAE, _BLOCK + 1, True), ("rejected", 0xAF, _BLOCK - 1, False)],
)
@pytest.mark.asyncio(loop_scope="module")
async def test_created_at_moves_exactly_when_the_cached_row_is_rewritten(
    conn: asyncpg.Connection, prime_id: int, label: str, proxy_byte: int, second_block: int, wins: bool
) -> None:
    """created_at is the staleness signal, so it moves on an overwrite and stays on a rejected write.

    One row per key, so an overwrite IS the creation of the current row and one
    audit column carries both its write time and the cache's staleness.
    """
    proxy_hex = f"{proxy_byte:02x}" * 20
    token_id = await insert_token(conn, f"APCTIMES{label}", 18, bytes([proxy_byte]) * 20)

    async def write(block: int, balance: int) -> None:
        await insert_allocation_position(
            conn,
            token_id=token_id,
            prime_id=prime_id,
            proxy_hex=proxy_hex,
            balance=balance,
            block=block,
            created_at=_BLOCK_AT,
            tx=_TX,
            direction="in",
        )

    await write(_BLOCK, 100)
    before = await _cached(conn, proxy_hex, token_id)
    await conn.execute("SELECT pg_sleep(0.01)")
    await write(second_block, 200)
    after = await _cached(conn, proxy_hex, token_id)

    assert after["balance"] == (200 if wins else 100)
    assert (after["created_at"] > before["created_at"]) is wins
