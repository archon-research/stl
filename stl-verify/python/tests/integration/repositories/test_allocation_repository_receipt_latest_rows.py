"""Integration tests for the receipt-position latest-row selection.

``_RECEIPT_TOKEN_POSITIONS_SQL`` answers "what does this proxy hold now" out of
an append-only history, so it must pick, per receipt token, the row that wins on
``block_number, block_version, processing_version, log_index`` — and it must do
so without materialising and sorting the whole join, which is what made it the
database's largest temp-spill source.

Two guarantees are pinned here: the selected row (against the versioning cases
that a wrong rewrite gets wrong), and the absence of temp spill. The differential
test compares the live query against the pre-rewrite formulation kept verbatim in
``_PRE_REWRITE_RECEIPT_TOKEN_POSITIONS_SQL``, so any future reshaping stays
result-identical row for row.

Isolated database per module (``module_db`` from ``conftest.py``); seeded by
``seed_receipt_position_latest_rows``.
"""

import asyncio
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from app.adapters.postgres.allocation_position_repository import (
    _RECEIPT_TOKEN_POSITIONS_SQL,
    AllocationRepository,
)
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import (
    RTL_EXCLUDED_SYMBOLS,
    RTL_LATEST_BALANCES,
    RTL_PROXY_HEX,
    seed_receipt_position_latest_rows,
)

_PROXY = EthAddress(f"0x{RTL_PROXY_HEX}")

# Small enough that the pre-rewrite formulation cannot sort the seeded join in
# memory, so the spill it caused in production is reproducible in a test.
_SPILL_WORK_MEM = "64kB"

# The query exactly as it stood before the latest-row rewrite: one DISTINCT ON
# over the full four-way join. Kept verbatim as the differential reference; do
# not "fix" it to match the live query, that is what it is here to detect.
_PRE_REWRITE_RECEIPT_TOKEN_POSITIONS_SQL = text("""
    WITH latest_receipt_positions AS (
        SELECT DISTINCT ON (rt.id)
            rt.id                                    AS receipt_token_id,
            rt.symbol                                AS symbol,
            encode(rt.receipt_token_address, 'hex')  AS receipt_token_address,
            ut.id                                    AS underlying_token_id,
            ut.symbol                                AS underlying_symbol,
            encode(ut.address, 'hex')                AS underlying_token_address,
            pr.id                                    AS protocol_id,
            pr.name                                  AS protocol_name,
            ap.chain_id                              AS chain_id,
            ap.balance                               AS balance,
            ap.underlying_value                      AS underlying_value,
            ap.underlying_token_id                   AS position_underlying_token_id,
            ap.created_at                            AS latest_activity_at,
            ap.direction                             AS latest_activity_action,
            ap.tx_amount                             AS latest_activity_amount
        FROM allocation_position ap
        JOIN token t          ON t.id = ap.token_id
        JOIN receipt_token rt ON rt.receipt_token_address = t.address AND rt.chain_id = ap.chain_id
        JOIN token ut         ON ut.id = rt.underlying_token_id
        JOIN protocol pr      ON pr.id = rt.protocol_id AND pr.chain_id = ap.chain_id
        WHERE ap.proxy_address = decode(:proxy_hex, 'hex')
        ORDER BY rt.id,
                 ap.block_number DESC, ap.block_version DESC,
                 ap.processing_version DESC, ap.log_index DESC
    )
    SELECT
        p.chain_id,
        p.receipt_token_id,
        p.receipt_token_address,
        p.underlying_token_id,
        p.underlying_token_address,
        p.symbol,
        p.underlying_symbol,
        p.protocol_name,
        p.balance,
        p.underlying_value,
        CASE
            WHEN p.position_underlying_token_id IS NOT NULL
             AND p.position_underlying_token_id <> p.underlying_token_id
            THEN NULL
            ELSE COALESCE(p.underlying_value, p.balance) * lp.price_usd
        END AS amount_usd,
        p.latest_activity_at,
        p.latest_activity_action,
        p.latest_activity_amount
    FROM latest_receipt_positions p
    LEFT JOIN LATERAL (
        SELECT otp.price_usd
        FROM onchain_token_price otp
        JOIN protocol_oracle po ON po.oracle_id = otp.oracle_id
            AND po.protocol_id = p.protocol_id
        WHERE otp.token_id = p.underlying_token_id
          AND EXISTS (
              SELECT 1 FROM oracle_asset oa
              WHERE oa.oracle_id = otp.oracle_id
                AND oa.token_id = otp.token_id
                AND oa.enabled
          )
        ORDER BY otp.block_number DESC, otp.block_version DESC,
                 otp.processing_version DESC, otp.oracle_id DESC
        LIMIT 1
    ) lp ON TRUE
    WHERE p.balance > 0
    ORDER BY p.balance DESC
""")


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed the latest-row scenarios and yield the async URL."""
    asyncio.run(seed_receipt_position_latest_rows(module_db["db_url"]))
    return module_db["async_url"]


@pytest_asyncio.fixture()
async def repo(async_db_url: str):
    """Bare AllocationRepository for direct-method tests."""
    engine = create_async_engine(async_db_url)
    try:
        yield AllocationRepository(engine)
    finally:
        await engine.dispose()


@pytest_asyncio.fixture()
async def conn(async_db_url: str):
    """A connection whose work_mem is too small to sort the seeded join in memory."""
    engine = create_async_engine(async_db_url)
    try:
        async with engine.connect() as connection:
            await connection.execute(text(f"SET work_mem = '{_SPILL_WORK_MEM}'"))
            yield connection
    finally:
        await engine.dispose()


def _temp_blocks_written(node: dict[str, Any]) -> int:
    """Sum the temp blocks written by a plan node and everything below it."""
    return node.get("Temp Written Blocks", 0) + sum(_temp_blocks_written(child) for child in node.get("Plans", []))


async def _temp_blocks_for(connection: AsyncConnection, sql: Any) -> int:
    """Run *sql* under EXPLAIN ANALYZE and return the temp blocks its plan wrote."""
    explained = text("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + str(sql))
    result = await connection.execute(explained, {"proxy_hex": RTL_PROXY_HEX})
    return _temp_blocks_written(result.scalar_one()[0]["Plan"])


async def _rows_for(connection: AsyncConnection, sql: Any) -> list[tuple]:
    """Return the query's rows as plain tuples, in the order the query emitted them."""
    result = await connection.execute(sql, {"proxy_hex": RTL_PROXY_HEX})
    return [tuple(row) for row in result.fetchall()]


@pytest.mark.asyncio
@pytest.mark.parametrize(("symbol", "expected_balance"), sorted(RTL_LATEST_BALANCES.items()))
async def test_receipt_position_reports_the_latest_versioned_balance(
    repo, symbol: str, expected_balance: Decimal
) -> None:
    """Each receipt token appears once, carrying the balance of its winning row."""
    positions = [p for p in await repo.list_receipt_token_positions(_PROXY) if p.symbol == symbol]
    assert len(positions) == 1
    assert positions[0].balance == expected_balance


@pytest.mark.asyncio
@pytest.mark.parametrize("symbol", RTL_EXCLUDED_SYMBOLS)
async def test_receipt_positions_omit_tokens_outside_the_receipt_set(repo, symbol: str) -> None:
    """A swept, bare-held or foreign-protocol token is absent despite having positions."""
    positions = await repo.list_receipt_token_positions(_PROXY)
    assert symbol not in {p.symbol for p in positions}


@pytest.mark.asyncio
async def test_receipt_positions_match_the_pre_rewrite_query(conn) -> None:
    """The live query returns the pre-rewrite query's rows, in the same order."""
    assert await _rows_for(conn, _RECEIPT_TOKEN_POSITIONS_SQL) == await _rows_for(
        conn, _PRE_REWRITE_RECEIPT_TOKEN_POSITIONS_SQL
    )


@pytest.mark.asyncio
async def test_pre_rewrite_receipt_positions_query_spills_to_disk(conn) -> None:
    """The pre-rewrite formulation spills on this data, so the no-spill test cannot pass vacuously."""
    assert await _temp_blocks_for(conn, _PRE_REWRITE_RECEIPT_TOKEN_POSITIONS_SQL) > 0


@pytest.mark.asyncio
async def test_receipt_positions_query_writes_no_temp_blocks(conn) -> None:
    """The live query sorts a per-token candidate set, so it never reaches disk."""
    assert await _temp_blocks_for(conn, _RECEIPT_TOKEN_POSITIONS_SQL) == 0
