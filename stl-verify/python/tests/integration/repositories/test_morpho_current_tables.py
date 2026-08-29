"""Morpho current-state tables: trigger upkeep for the three caches VEC-659 added.

Every scenario seeds its own protocol / market / vault, so the module's shared
database keeps the scenarios independent of each other and of ordering. The
breakdown read that consumes these caches is covered by
test_backed_breakdown_repository_morpho.py.
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

# Per history: the cache it feeds, the cache's key columns, and the payload column
# each scenario below writes and then reads back.
_CACHES = {
    "vault_state": ("morpho_vault_state_current", ("morpho_vault_id",), "total_assets"),
    "market_state": ("morpho_market_state_current", ("morpho_market_id",), "total_supply_assets"),
    "market_position": ("morpho_market_position_current", ("user_id", "morpho_market_id"), "supply_assets"),
}


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
    conn: asyncpg.Connection, history: str, keys: dict[str, int], *, value: int, block: int
) -> None:
    """Append one snapshot to the named history, carrying `value` in its payload column."""
    if history == "vault_state":
        await conn.execute(
            """
            INSERT INTO morpho_vault_state
                (morpho_vault_id, block_number, block_version, timestamp, total_assets, total_shares)
            VALUES ($1, $2, 0, $3, $4, $4)
            """,
            keys["morpho_vault_id"],
            block,
            _TIMESTAMP,
            Decimal(value),
        )
    elif history == "market_state":
        await conn.execute(
            """
            INSERT INTO morpho_market_state
                (morpho_market_id, block_number, block_version, timestamp,
                 total_supply_assets, total_supply_shares, total_borrow_assets, total_borrow_shares,
                 last_update, fee)
            VALUES ($1, $2, 0, $3, $4, $4, 0, 0, $2, 0)
            """,
            keys["morpho_market_id"],
            block,
            _TIMESTAMP,
            Decimal(value),
        )
    else:
        await conn.execute(
            """
            INSERT INTO morpho_market_position
                (user_id, morpho_market_id, block_number, block_version, timestamp,
                 supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
            VALUES ($1, $2, $3, 0, $4, $5, 0, 0, $5, 0)
            """,
            keys["user_id"],
            keys["morpho_market_id"],
            block,
            _TIMESTAMP,
            Decimal(value),
        )


async def _cached_row(conn: asyncpg.Connection, history: str, keys: dict[str, int]) -> Any:
    """Read back the cache row for one scenario's key."""
    table, key_columns, value_column = _CACHES[history]
    predicate = " AND ".join(f"{column} = ${index + 1}" for index, column in enumerate(key_columns))
    return await conn.fetchrow(
        f"SELECT {value_column} AS value, block_number FROM {table} WHERE {predicate}",  # noqa: S608
        *(keys[column] for column in key_columns),
    )


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize(
    ("history", "seed"), [("vault_state", 0x11), ("market_state", 0x21), ("market_position", 0x31)]
)
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
@pytest.mark.parametrize(
    ("history", "seed"), [("vault_state", 0x41), ("market_state", 0x51), ("market_position", 0x61)]
)
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


# The invariant the whole design rests on, one query per cache: each holds exactly
# "newest row per key" over its history. Symmetric EXCEPT so a missing row and a
# stale row are distinguishable.
_NEWEST_PER_KEY = {
    "morpho_vault_state_current": """
        SELECT DISTINCT ON (morpho_vault_id)
               morpho_vault_id, total_assets, block_number, block_version, processing_version
        FROM morpho_vault_state
        ORDER BY morpho_vault_id, block_number DESC, block_version DESC, processing_version DESC""",
    "morpho_market_state_current": """
        SELECT DISTINCT ON (morpho_market_id)
               morpho_market_id, total_supply_assets, total_borrow_assets,
               block_number, block_version, processing_version
        FROM morpho_market_state
        ORDER BY morpho_market_id, block_number DESC, block_version DESC, processing_version DESC""",
    "morpho_market_position_current": """
        SELECT DISTINCT ON (user_id, morpho_market_id)
               user_id, morpho_market_id, supply_assets, block_number, block_version, processing_version
        FROM morpho_market_position
        ORDER BY user_id, morpho_market_id, block_number DESC, block_version DESC, processing_version DESC""",
}


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize(
    ("cache_table", "seed"),
    [
        ("morpho_vault_state_current", 0x71),
        ("morpho_market_state_current", 0x81),
        ("morpho_market_position_current", 0x91),
    ],
)
async def test_cache_equals_newest_row_per_key_over_the_history(
    conn: asyncpg.Connection, cache_table: str, seed: int
) -> None:
    """Each cache equals the DISTINCT ON the breakdown query used to compute per request."""
    keys = await _seed_morpho_keys(conn, f"Inv{seed:x}", seed)
    for history in _CACHES:
        for block, value in ((_BLOCK, 10), (_BLOCK + 2, 30), (_BLOCK + 1, 20)):
            await _insert_history(conn, history, keys, value=value, block=block)

    missing, stale = await conn.fetchrow(
        f"""
        WITH newest AS ({_NEWEST_PER_KEY[cache_table]})
        SELECT (SELECT count(*) FROM (TABLE newest EXCEPT TABLE {cache_table}) a),
               (SELECT count(*) FROM (TABLE {cache_table} EXCEPT TABLE newest) b)
        """  # noqa: S608
    )
    assert (missing, stale) == (0, 0)
