"""token_total_supply_current: the trigger keeps the newest supply per key, and the share lookup reads it.

"Newest" is (block_number, block_version, processing_version), the order the
share lookup requires, with block_timestamp as a final tie-break, so the cache
must equal DISTINCT ON over token_total_supply in that order.

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
from tests.integration.conftest import MIGRATIONS_DIR
from tests.integration.seed import (
    insert_allocation_position,
    insert_prime,
    insert_token,
    insert_token_total_supply,
)

_BLOCK = 22_000_000
_OBSERVED_AT = dt.datetime(2026, 8, 27, 12, 0, tzinfo=dt.UTC)
_TX = "cd" * 32

_BACKFILL_SQL = (MIGRATIONS_DIR / "20260827_120100_backfill_token_total_supply_current.sql").read_text()

# Symmetric EXCEPT over the columns the cache copies, so a missing row and a
# stale row are distinguishable (the Go suite runs the same invariant).
_CACHE_EQUALS_NEWEST_SQL = """
WITH newest AS (
    SELECT DISTINCT ON (chain_id, token_id)
           chain_id, token_id, total_supply, scaled_total_supply, block_timestamp,
           block_number, block_version, processing_version, created_at
    FROM token_total_supply
    ORDER BY chain_id, token_id,
             block_number DESC, block_version DESC, processing_version DESC,
             block_timestamp DESC)
SELECT (SELECT count(*) FROM (TABLE newest EXCEPT TABLE token_total_supply_current) a) AS history_not_in_cache,
       (SELECT count(*) FROM (TABLE token_total_supply_current EXCEPT TABLE newest) b) AS cache_not_in_history
"""


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
async def test_later_timestamp_wins_a_full_version_tuple_tie(conn: asyncpg.Connection) -> None:
    """Two rows tying on (block_number, block_version, processing_version) are split by block_timestamp.

    Legal only for hand-written rows (the PK permits them, and the assign trigger
    versions per timestamp, so both carry version 0); without the tie-break the
    winner would be arrival order and a rebuild could disagree with the trigger.
    """
    token_id = await insert_token(conn, "TSCTIE", 18, b"\xb8" * 20)

    for offset_s, supply in [(0, 100), (1, 175)]:
        await insert_token_total_supply(
            conn,
            token_id=token_id,
            total_supply=supply,
            block=_BLOCK,
            block_timestamp=_OBSERVED_AT + dt.timedelta(seconds=offset_s),
        )

    row = await _cached(conn, token_id)
    assert row["total_supply"] == 175


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
async def test_rerunning_the_backfill_repairs_an_equal_tuple_correction(conn: asyncpg.Connection) -> None:
    """The backfill file, executed verbatim, overwrites a cache row whose version tuple equals history's newest.

    The trigger's strict > can never re-land an equal-tuple correction (a hand-fix
    with triggers disabled, or a delete + re-sweep that re-assigns version 0), so
    the backfill's >= guard is the repair path — and running the real file pins
    its guard and ORDER BY against drifting from the trigger's.
    """
    token_id = await insert_token(conn, "TSCFIX", 18, b"\xb7" * 20)
    await insert_token_total_supply(conn, token_id=token_id, total_supply=300, block=_BLOCK)
    await conn.execute(
        "UPDATE token_total_supply_current SET total_supply = 999 WHERE chain_id = 1 AND token_id = $1", token_id
    )

    await conn.execute(_BACKFILL_SQL)

    row = await _cached(conn, token_id)
    assert row["total_supply"] == 300
    diverged = await conn.fetchrow(_CACHE_EQUALS_NEWEST_SQL)
    assert (diverged["history_not_in_cache"], diverged["cache_not_in_history"]) == (0, 0)


@pytest.mark.asyncio(loop_scope="module")
async def test_share_lookup_reads_the_supply_from_the_cache(conn: asyncpg.Connection, engine: AsyncEngine) -> None:
    """fetch_share divides the pinned balance by the cached supply, not by the history's.

    The cache row is overwritten after seeding so the two disagree; only the
    cache path can produce the expected ratio.
    """
    token_id = await insert_token(conn, "TSCSHARE", 18, b"\xb4" * 20)
    prime_id = await insert_prime(conn, "tts_current", b"\x52" * 20)
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
