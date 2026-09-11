"""Morpho current-state tables: trigger upkeep for the caches the backed-breakdown read joins.

morpho_vault_state_current and morpho_market_state_current are the two caches
VEC-659 adds; morpho_market_position_current (VEC-753) is the third table the
read joins and is included so every invariant the read rests on is asserted in one
place. Every scenario seeds its own protocol / market / vault, so the module's
shared database keeps the scenarios independent of each other and of ordering.
The breakdown read itself is covered by test_backed_breakdown_repository_morpho.py.
"""

import datetime as dt
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any

import asyncpg
import pytest
import pytest_asyncio

from tests.integration.seed import insert_protocol, insert_token, insert_user

_BLOCK = 21_000_000
_TIMESTAMP = dt.datetime(2026, 8, 25, 12, 0, tzinfo=dt.UTC)
# morpho_market_state.last_update, a Unix epoch inside the register's plausibility bounds.
_LAST_UPDATE = 1_800_000_000
# A build_id the assign_processing_version_* triggers have never seen for a key, so
# a second insert at the same (block, version, timestamp) takes the MAX+1 branch.
_REPROCESS_BUILD_ID = 999_999

# Per history: the cache it feeds, the cache's key columns, and the payload column
# each scenario below writes and then reads back.
_CACHES = {
    "vault_state": ("morpho_vault_state_current", ("morpho_vault_id",), "total_assets"),
    "market_state": ("morpho_market_state_current", ("morpho_market_id",), "total_supply_assets"),
    "market_position": ("morpho_market_position_current", ("user_id", "morpho_market_id"), "supply_assets"),
}

_HISTORIES = [("vault_state", 0x11), ("market_state", 0x21), ("market_position", 0x31)]


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def conn(db_url: str) -> AsyncIterator[asyncpg.Connection]:
    """One connection for the module's isolated database."""
    connection = await asyncpg.connect(db_url)
    try:
        yield connection
    finally:
        await connection.close()


async def _seed_morpho_keys(conn: asyncpg.Connection, tag: str, seed: int) -> dict[str, int]:
    """Create the protocol, market, vault and user one scenario's histories reference."""
    protocol_id = await insert_protocol(conn, f"morphoCur{tag}", bytes([seed]) * 20)
    loan_token_id = await insert_token(conn, f"MCL{tag}", 18, bytes([seed + 1]) * 20)
    collateral_token_id = await insert_token(conn, f"MCC{tag}", 18, bytes([seed + 2]) * 20)
    vault_address = bytes([seed + 3]) * 20

    market_id = await conn.fetchval(
        """
        INSERT INTO morpho_market
            (chain_id, protocol_id, market_id, loan_token_id, collateral_token_id,
             oracle_address, irm_address, lltv, created_at_block)
        VALUES (1, $1, $2, $3, $4, $5, $5, 0.86, $6)
        RETURNING id
        """,
        protocol_id,
        bytes([seed + 4]) * 32,
        loan_token_id,
        collateral_token_id,
        b"\x00" * 20,
        _BLOCK,
    )
    vault_id = await conn.fetchval(
        """
        INSERT INTO morpho_vault
            (chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
        VALUES (1, $1, $2, $3, $4, $5, 1, $6)
        RETURNING id
        """,
        protocol_id,
        vault_address,
        f"Morpho Current {tag}",
        f"mc{tag}",
        loan_token_id,
        _BLOCK,
    )
    user_id = await insert_user(conn, vault_address)
    return {"morpho_market_id": market_id, "morpho_vault_id": vault_id, "user_id": user_id}


async def _insert_history(
    conn: asyncpg.Connection, history: str, keys: dict[str, int], *, value: int, block: int, build_id: int = 0
) -> None:
    """Append one snapshot to the named history, carrying `value` in its payload column."""
    if history == "vault_state":
        await conn.execute(
            """
            INSERT INTO morpho_vault_state
                (morpho_vault_id, block_number, block_version, timestamp, total_assets, total_shares, build_id)
            VALUES ($1, $2, 0, $3, $4, $4, $5)
            """,
            keys["morpho_vault_id"],
            block,
            _TIMESTAMP,
            Decimal(value),
            build_id,
        )
    elif history == "market_state":
        await conn.execute(
            """
            INSERT INTO morpho_market_state
                (morpho_market_id, block_number, block_version, timestamp,
                 total_supply_assets, total_supply_shares, total_borrow_assets, total_borrow_shares,
                 last_update, fee, build_id)
            VALUES ($1, $2, 0, $3, $4, $4, 0, 0, $6, 0, $5)
            """,
            keys["morpho_market_id"],
            block,
            _TIMESTAMP,
            Decimal(value),
            build_id,
            _LAST_UPDATE,
        )
    else:
        await conn.execute(
            """
            INSERT INTO morpho_market_position
                (user_id, morpho_market_id, block_number, block_version, timestamp,
                 supply_shares, borrow_shares, collateral, supply_assets, borrow_assets, build_id)
            VALUES ($1, $2, $3, 0, $4, $5, 0, 0, $5, 0, $6)
            """,
            keys["user_id"],
            keys["morpho_market_id"],
            block,
            _TIMESTAMP,
            Decimal(value),
            build_id,
        )


async def _cached_row(conn: asyncpg.Connection, history: str, keys: dict[str, int]) -> Any:
    """Read back the cache row for one scenario's key."""
    table, key_columns, value_column = _CACHES[history]
    predicate = " AND ".join(f"{column} = ${index + 1}" for index, column in enumerate(key_columns))
    return await conn.fetchrow(
        f"SELECT {value_column} AS value, block_number, processing_version FROM {table} WHERE {predicate}",  # noqa: S608
        *(keys[column] for column in key_columns),
    )


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize(("history", "seed"), _HISTORIES)
async def test_newer_snapshot_replaces_the_cached_row(conn: asyncpg.Connection, history: str, seed: int) -> None:
    """A snapshot at a higher block replaces the current row."""
    keys = await _seed_morpho_keys(conn, f"New{seed:x}", seed)

    await _insert_history(conn, history, keys, value=100, block=_BLOCK)
    await _insert_history(conn, history, keys, value=250, block=_BLOCK + 1)

    row = await _cached_row(conn, history, keys)
    assert row is not None
    assert row["value"] == Decimal(250)
    assert row["block_number"] == _BLOCK + 1


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize(("history", "seed"), [(history, seed + 0x30) for history, seed in _HISTORIES])
async def test_out_of_order_snapshot_does_not_regress_the_cached_row(
    conn: asyncpg.Connection, history: str, seed: int
) -> None:
    """An older snapshot arriving late (backfill, retry) must not overwrite the current row."""
    keys = await _seed_morpho_keys(conn, f"Old{seed:x}", seed)

    await _insert_history(conn, history, keys, value=250, block=_BLOCK + 1)
    await _insert_history(conn, history, keys, value=100, block=_BLOCK)

    row = await _cached_row(conn, history, keys)
    assert row is not None
    assert row["value"] == Decimal(250)
    assert row["block_number"] == _BLOCK + 1


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize(("history", "seed"), [(history, seed + 0x60) for history, seed in _HISTORIES])
async def test_reprocess_of_the_same_block_replaces_the_cached_row(
    conn: asyncpg.Connection, history: str, seed: int
) -> None:
    """A correction at the SAME block, from a different build, takes the cache.

    The processing_version leg of the newer-wins comparison is the one that carries
    reprocessing corrections: the assign trigger gives a second insert at the same
    (block, version, timestamp) from a new build_id the next version, and if the
    comparison dropped that term the cache would serve the stale row forever.
    """
    keys = await _seed_morpho_keys(conn, f"Rep{seed:x}", seed)

    await _insert_history(conn, history, keys, value=100, block=_BLOCK)
    await _insert_history(conn, history, keys, value=250, block=_BLOCK, build_id=_REPROCESS_BUILD_ID)

    row = await _cached_row(conn, history, keys)
    assert row is not None
    assert row["value"] == Decimal(250)
    assert row["block_number"] == _BLOCK
    assert row["processing_version"] == 1


@pytest.mark.asyncio(loop_scope="module")
async def test_implausible_market_last_update_caches_as_null_without_aborting_ingest(conn: asyncpg.Connection) -> None:
    """A last_update epoch outside the register's plausibility bounds caches as NULL, and the insert lands.

    The cache carries the canonical timestamptz; an unguarded to_timestamp inside
    the trigger would raise on a corrupt epoch and abort the history insert.
    """
    keys = await _seed_morpho_keys(conn, "BadEpoch", 0xD1)

    await conn.execute(
        """
        INSERT INTO morpho_market_state
            (morpho_market_id, block_number, block_version, timestamp,
             total_supply_assets, total_supply_shares, total_borrow_assets, total_borrow_shares,
             last_update, fee)
        VALUES ($1, $2, 0, $3, 1, 1, 0, 0, -1, 0)
        """,
        keys["morpho_market_id"],
        _BLOCK,
        _TIMESTAMP,
    )

    row = await conn.fetchrow(
        "SELECT last_update_at, block_number FROM morpho_market_state_current WHERE morpho_market_id = $1",
        keys["morpho_market_id"],
    )
    assert row is not None
    assert row["block_number"] == _BLOCK
    assert row["last_update_at"] is None


# The invariant the whole design rests on, one query per cache: for a key, the cache
# row equals "newest row" over that key's history, with the newest-first order the
# trigger's comparison defines (identity terms, then processing_version). Symmetric
# EXCEPT so a missing row and a stale row are distinguishable. Scoped to the
# scenario's own key, so the assertion never reaches another scenario's rows.
_NEWEST_PER_KEY = {
    "vault_state": (
        """
        SELECT DISTINCT ON (morpho_vault_id)
               morpho_vault_id, total_assets, total_shares, "timestamp",
               block_number, block_version, processing_version
        FROM morpho_vault_state
        WHERE morpho_vault_id = $1
        ORDER BY morpho_vault_id, block_number DESC, block_version DESC, "timestamp" DESC, processing_version DESC""",
        """
        SELECT morpho_vault_id, total_assets, total_shares, block_timestamp,
               block_number, block_version, processing_version
        FROM morpho_vault_state_current
        WHERE morpho_vault_id = $1""",
        ("morpho_vault_id",),
    ),
    "market_state": (
        """
        SELECT DISTINCT ON (morpho_market_id)
               morpho_market_id, total_supply_assets, total_supply_shares, total_borrow_assets,
               total_borrow_shares,
               CASE WHEN last_update BETWEEN 1500000000 AND 4100000000 THEN to_timestamp(last_update) END,
               fee, "timestamp",
               block_number, block_version, processing_version
        FROM morpho_market_state
        WHERE morpho_market_id = $1
        ORDER BY morpho_market_id, block_number DESC, block_version DESC, "timestamp" DESC, processing_version DESC""",
        """
        SELECT morpho_market_id, total_supply_assets, total_supply_shares, total_borrow_assets,
               total_borrow_shares, last_update_at, fee, block_timestamp,
               block_number, block_version, processing_version
        FROM morpho_market_state_current
        WHERE morpho_market_id = $1""",
        ("morpho_market_id",),
    ),
    "market_position": (
        """
        SELECT DISTINCT ON (user_id, morpho_market_id)
               user_id, morpho_market_id, supply_shares, borrow_shares, collateral,
               supply_assets, borrow_assets, "timestamp",
               block_number, block_version, processing_version
        FROM morpho_market_position
        WHERE user_id = $1 AND morpho_market_id = $2
        ORDER BY user_id, morpho_market_id,
                 block_number DESC, block_version DESC, "timestamp" DESC, processing_version DESC""",
        """
        SELECT user_id, morpho_market_id, supply_shares, borrow_shares, collateral,
               supply_assets, borrow_assets, block_timestamp,
               block_number, block_version, processing_version
        FROM morpho_market_position_current
        WHERE user_id = $1 AND morpho_market_id = $2""",
        ("user_id", "morpho_market_id"),
    ),
}


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize(("history", "seed"), [(history, seed + 0x90) for history, seed in _HISTORIES])
async def test_cache_equals_newest_row_per_key_over_the_history(
    conn: asyncpg.Connection, history: str, seed: int
) -> None:
    """Each cache row equals the DISTINCT ON the breakdown query used to compute per request."""
    keys = await _seed_morpho_keys(conn, f"Inv{seed:x}", seed)
    for block, value in ((_BLOCK, 10), (_BLOCK + 2, 30), (_BLOCK + 1, 20)):
        await _insert_history(conn, history, keys, value=value, block=block)

    newest, cached, key_columns = _NEWEST_PER_KEY[history]
    missing, stale = await conn.fetchrow(
        f"""
        WITH newest AS ({newest}), cached AS ({cached})
        SELECT (SELECT count(*) FROM (TABLE newest EXCEPT TABLE cached) a),
               (SELECT count(*) FROM (TABLE cached EXCEPT TABLE newest) b)
        """,  # noqa: S608
        *(keys[column] for column in key_columns),
    )
    assert (missing, stale) == (0, 0)
