"""Async DB helpers shared by integration tests.

These live alongside the legacy per-module DB infrastructure in
``conftest.py`` and are imported directly by test modules. Fixtures stay in
``conftest.py``; pure utilities live here so tests don't import from a
conftest module.
"""

from typing import cast

import asyncpg


async def insert_token(conn: asyncpg.Connection, symbol: str, decimals: int, address: bytes) -> int:
    """Insert a token or return the existing ID."""
    return cast(
        int,
        await conn.fetchval(
            """
        INSERT INTO token (chain_id, address, symbol, decimals)
        VALUES (1, $1, $2, $3)
        ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol
        RETURNING id
        """,
            address,
            symbol,
            decimals,
        ),
    )


async def insert_user(conn: asyncpg.Connection, address: bytes) -> int:
    """Insert a user or return the existing ID (upsert on chain_id + address)."""
    return cast(
        int,
        await conn.fetchval(
            """
        INSERT INTO "user" (chain_id, address)
        VALUES (1, $1)
        ON CONFLICT (chain_id, address) DO UPDATE SET updated_at = NOW()
        RETURNING id
        """,
            address,
        ),
    )


async def insert_receipt_token(
    db_url: str,
    chain_id: int,
    address: bytes,
    symbol: str = "aUSDC",
) -> int:
    """Insert a receipt_token row (upserting on chain_id + address) and return its ID.

    Picks an arbitrary protocol and token from the given ``chain_id`` to
    satisfy the foreign-key constraints.
    """
    conn = await asyncpg.connect(db_url)
    try:
        protocol_id = await conn.fetchval(
            "SELECT id FROM protocol WHERE chain_id = $1 LIMIT 1",
            chain_id,
        )
        if protocol_id is None:
            raise RuntimeError(f"no protocol seed found for chain_id={chain_id}")
        token_id = await conn.fetchval(
            "SELECT id FROM token WHERE chain_id = $1 LIMIT 1",
            chain_id,
        )
        if token_id is None:
            raise RuntimeError(f"no token seed found for chain_id={chain_id}")
        return cast(
            int,
            await conn.fetchval(
                """
                INSERT INTO receipt_token
                    (protocol_id, underlying_token_id, receipt_token_address, symbol,
                     created_at_block, chain_id)
                VALUES ($1, $2, $3, $4, 1, $5)
                ON CONFLICT ON CONSTRAINT receipt_token_chain_address_unique
                    DO UPDATE SET symbol = EXCLUDED.symbol
                RETURNING id
                """,
                protocol_id,
                token_id,
                address,
                symbol,
                chain_id,
            ),
        )
    finally:
        await conn.close()


def composite_mapping_key(chain_id: int, address: bytes) -> str:
    """Build a ``chain_id:0xAddress`` composite key for the asset mapping JSON."""
    return f"{chain_id}:0x{address.hex()}"


async def store_test_ids(conn: asyncpg.Connection, ids: dict[str, int]) -> None:
    """Persist seed IDs into a helper table so test fixtures can retrieve them."""
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS _test_ids (
            key TEXT PRIMARY KEY,
            val BIGINT NOT NULL
        )
        """
    )
    for key, val in ids.items():
        await conn.execute("INSERT INTO _test_ids (key, val) VALUES ($1, $2)", key, val)
