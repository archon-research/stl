"""Integration tests for the allocation latest-row selections.

Three reads answer "what does this proxy hold now" out of an append-only history
— ``_RECEIPT_TOKEN_POSITIONS_SQL``, ``_TOTAL_USD_EXPOSURE_SQL`` and
``_DIRECT_ASSET_HOLDINGS_SQL``, the first and last of which fire together on
``/v1/allocations``. Each must pick, per key, the row that wins on
``block_number, block_version, processing_version, log_index, direction,
tx_hash``, and must do so without materialising and sorting the whole history, which is what
made them the database's largest temp-spill sources.

Three guarantees are pinned here: the selected row (against the versioning cases
a wrong rewrite gets wrong), the deterministic resolution of an exact tie, and
the absence of temp spill. The differential tests compare each live query against
the same query before it was staged, kept in ``_PRE_STAGING_*`` below, so any
future reshaping stays result-identical row for row.

Isolated database per module (``module_db`` from ``conftest.py``); seeded by
``seed_receipt_position_latest_rows``.
"""

import asyncio
from decimal import Decimal
from typing import Any

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import bindparam, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from app.adapters.postgres.allocation_position_repository import (
    _DIRECT_ASSET_HOLDINGS_SQL,
    _RECEIPT_TOKEN_POSITIONS_SQL,
    _TOTAL_USD_EXPOSURE_SQL,
    _UNDERLYING_VALUE_TOKEN_ADDRS,
    AllocationRepository,
)
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import (
    RTL_DIRECT_BALANCES,
    RTL_EXCLUDED_SYMBOLS,
    RTL_LATEST_ACTIONS,
    RTL_LATEST_BALANCES,
    RTL_PROXY_HEX,
    RTL_TREASURY_BALANCE,
    RTL_UNDERLYING_PRICE,
    seed_receipt_position_latest_rows,
)

_PROXY = EthAddress(f"0x{RTL_PROXY_HEX}")

# Small enough that the pre-staging formulations cannot sort the seeded history
# in memory, so the spill they caused in production is reproducible in a test.
_SPILL_WORK_MEM = "64kB"

# Each query as it stood before the latest-row staging: one pass over the proxy's
# whole history feeding the dedup. These are semantic copies — the long rationale
# comments are dropped, and the ordering matches the live queries' (the tie-break
# is pinned by its own test, so these isolate the STAGING change). Do not restage
# their CTEs to match the live queries, that divergence is what they detect; a
# deliberate change elsewhere in a query does belong here, mirrored.

_PRE_STAGING_RECEIPT_TOKEN_POSITIONS_SQL = text("""
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
                 ap.processing_version DESC, ap.log_index DESC,
                 ap.direction DESC, ap.tx_hash DESC
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

_PRE_STAGING_TOTAL_USD_EXPOSURE_SQL = text("""
WITH latest_receipt_positions AS (
    SELECT DISTINCT ON (rt.id)
        rt.id                  AS receipt_token_id,
        rt.underlying_token_id AS underlying_token_id,
        rt.protocol_id         AS protocol_id,
        ap.balance,
        ap.underlying_value,
        ap.underlying_token_id AS position_underlying_token_id
    FROM allocation_position ap
    JOIN token t          ON t.id = ap.token_id
    JOIN receipt_token rt ON rt.receipt_token_address = t.address AND rt.chain_id = ap.chain_id
    JOIN protocol pr      ON pr.id = rt.protocol_id AND pr.chain_id = ap.chain_id
    WHERE ap.proxy_address = decode(:proxy_hex, 'hex')
    ORDER BY rt.id,
             ap.block_number DESC, ap.block_version DESC,
             ap.processing_version DESC, ap.log_index DESC,
             ap.direction DESC, ap.tx_hash DESC
)
SELECT COALESCE(SUM(
    CASE
        WHEN p.position_underlying_token_id IS NOT NULL
         AND p.position_underlying_token_id <> p.underlying_token_id
        THEN NULL
        ELSE COALESCE(p.underlying_value, p.balance) * lp.price_usd
    END
), 0) AS total_usd_exposure
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
""")

_PRE_STAGING_DIRECT_ASSET_HOLDINGS_SQL = text("""
    WITH latest_positions AS (
        SELECT DISTINCT ON (ap.token_id)
            ap.chain_id,
            ap.token_id,
            ap.balance,
            ap.underlying_value,
            ap.underlying_token_id,
            ap.created_at AS latest_activity_at,
            ap.direction AS latest_activity_action,
            ap.tx_amount AS latest_activity_amount
        FROM allocation_position ap
        WHERE ap.proxy_address = decode(:proxy_hex, 'hex')
        ORDER BY ap.token_id,
                 ap.block_number DESC, ap.block_version DESC,
                 ap.processing_version DESC, ap.log_index DESC,
                 ap.direction DESC, ap.tx_hash DESC
    )
    SELECT
        lp.chain_id,
        lp.token_id,
        encode(t.address, 'hex') AS token_address,
        t.symbol                 AS symbol,
        lp.balance,
        CASE
            WHEN t.address IN :uv_token_addrs
            THEN lp.underlying_value * up.price_usd
            ELSE lp.balance * px.price_usd
        END AS amount_usd,
        ut.id                     AS underlying_token_id,
        encode(ut.address, 'hex') AS underlying_token_address,
        ut.symbol                 AS underlying_symbol,
        lp.latest_activity_at,
        lp.latest_activity_action,
        lp.latest_activity_amount
    FROM latest_positions lp
    JOIN token t ON t.id = lp.token_id
    LEFT JOIN token ut
        ON ut.id = lp.underlying_token_id
        AND t.address IN :uv_token_addrs
        AND ut.symbol IS NOT NULL
    LEFT JOIN receipt_token rt
        ON rt.receipt_token_address = t.address AND rt.chain_id = lp.chain_id
    LEFT JOIN LATERAL (
        SELECT otp.price_usd
        FROM onchain_token_price otp
        WHERE otp.token_id = lp.token_id
          AND EXISTS (
              SELECT 1 FROM oracle_asset oa
              WHERE oa.oracle_id = otp.oracle_id
                AND oa.token_id = otp.token_id
                AND oa.enabled
          )
        ORDER BY otp.block_number DESC, otp.block_version DESC,
                 otp.processing_version DESC, otp.oracle_id DESC
        LIMIT 1
    ) px ON TRUE
    LEFT JOIN LATERAL (
        SELECT otp.price_usd
        FROM onchain_token_price otp
        WHERE otp.token_id = lp.underlying_token_id
          AND EXISTS (
              SELECT 1 FROM oracle_asset oa
              WHERE oa.oracle_id = otp.oracle_id
                AND oa.token_id = otp.token_id
                AND oa.enabled
          )
        ORDER BY otp.block_number DESC, otp.block_version DESC,
                 otp.processing_version DESC, otp.oracle_id DESC
        LIMIT 1
    ) up ON TRUE
    WHERE rt.id IS NULL AND lp.balance > 0
    ORDER BY lp.balance DESC
""").bindparams(bindparam("uv_token_addrs", expanding=True))


_PROXY_PARAMS = {"proxy_hex": RTL_PROXY_HEX}

# (live query, pre-staging reference, bind parameters).
_STAGED_QUERIES = (
    pytest.param(
        _RECEIPT_TOKEN_POSITIONS_SQL,
        _PRE_STAGING_RECEIPT_TOKEN_POSITIONS_SQL,
        _PROXY_PARAMS,
        id="receipt_token_positions",
    ),
    pytest.param(
        _TOTAL_USD_EXPOSURE_SQL,
        _PRE_STAGING_TOTAL_USD_EXPOSURE_SQL,
        _PROXY_PARAMS,
        id="total_usd_exposure",
    ),
    pytest.param(
        _DIRECT_ASSET_HOLDINGS_SQL,
        _PRE_STAGING_DIRECT_ASSET_HOLDINGS_SQL,
        _PROXY_PARAMS | {"uv_token_addrs": _UNDERLYING_VALUE_TOKEN_ADDRS},
        id="direct_asset_holdings",
    ),
)


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed the latest-row scenarios and yield the async URL."""
    asyncio.run(seed_receipt_position_latest_rows(module_db["db_url"]))
    return module_db["async_url"]


@pytest.fixture(scope="module")
def sweep_tie_receipt_token_id(async_db_url: str, db_url: str) -> int:
    """The seeded rtlSweepTie receipt_token id (async_db_url only to order seeding first)."""

    async def _fetch() -> int:
        conn = await asyncpg.connect(db_url)
        try:
            return await conn.fetchval("SELECT id FROM receipt_token WHERE symbol = 'rtlSweepTie'")
        finally:
            await conn.close()

    return asyncio.run(_fetch())


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
    """A connection whose work_mem is too small to sort the seeded history in memory."""
    engine = create_async_engine(async_db_url)
    try:
        async with engine.connect() as connection:
            await connection.execute(text(f"SET work_mem = '{_SPILL_WORK_MEM}'"))
            yield connection
    finally:
        await engine.dispose()


def _temp_blocks_written(node: dict[str, Any]) -> int:
    """Return the temp blocks written by the busiest node in a plan subtree.

    EXPLAIN's buffer counters already accumulate up the tree, so this is the root
    node's figure in practice; taking the maximum rather than the root keeps a
    subtree that fails to roll up from reading as zero.
    """
    return max([node.get("Temp Written Blocks", 0)] + [_temp_blocks_written(child) for child in node.get("Plans", [])])


async def _temp_blocks_for(connection: AsyncConnection, sql: Any, params: dict) -> int:
    """Run *sql* under EXPLAIN ANALYZE and return the temp blocks its plan wrote.

    Compiling with the values bound is what makes this work for a query carrying
    an expanding (list) parameter: ``str()`` alone leaves a postcompile marker
    that ``text()`` cannot parse back into a bind parameter.
    """
    compiled = sql.bindparams(**params).compile(
        dialect=postgresql.dialect(paramstyle="named"),
        compile_kwargs={"render_postcompile": True},
    )
    explained = text("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + str(compiled))
    result = await connection.execute(explained, dict(compiled.params))
    return _temp_blocks_written(result.scalar_one()[0]["Plan"])


async def _rows_for(connection: AsyncConnection, sql: Any, params: dict) -> list[tuple]:
    """Return the query's rows as plain tuples, in the order the query emitted them."""
    result = await connection.execute(sql, params)
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
@pytest.mark.parametrize(("symbol", "expected_action"), sorted(RTL_LATEST_ACTIONS.items()))
async def test_receipt_position_breaks_an_exact_tie_on_direction(repo, symbol: str, expected_action: str) -> None:
    """Rows tied through log_index resolve on direction, not on whatever the sort emitted."""
    positions = {p.symbol: p for p in await repo.list_receipt_token_positions(_PROXY)}
    assert positions[symbol].latest_activity_action == expected_action


@pytest.mark.asyncio
@pytest.mark.parametrize(("symbol", "expected_balance"), sorted(RTL_DIRECT_BALANCES.items()))
async def test_direct_holding_reports_the_latest_versioned_balance(
    repo, symbol: str, expected_balance: Decimal
) -> None:
    """Each bare-held token appears once, carrying the balance of its winning row."""
    holdings = [h for h in await repo.list_direct_asset_holdings(_PROXY) if h.symbol == symbol]
    assert len(holdings) == 1
    assert holdings[0].balance == expected_balance


@pytest.mark.asyncio
async def test_usd_exposure_breaks_an_exact_tie_on_direction(repo, sweep_tie_receipt_token_id: int) -> None:
    """The per-asset exposure read resolves the sweep tie the same way the positions list does."""
    exposure = await repo.get_usd_exposure(sweep_tie_receipt_token_id, _PROXY)
    assert exposure == RTL_LATEST_BALANCES["rtlSweepTie"] * RTL_UNDERLYING_PRICE


@pytest.mark.asyncio
async def test_latest_total_capital_breaks_an_exact_tie_on_direction(repo) -> None:
    """The treasury read resolves the sweep tie the same way; USDS is the USD figure."""
    assert await repo.get_latest_total_capital_usd(_PROXY) == RTL_TREASURY_BALANCE


@pytest.mark.asyncio
@pytest.mark.parametrize("symbol", RTL_EXCLUDED_SYMBOLS)
async def test_receipt_positions_omit_tokens_outside_the_receipt_set(repo, symbol: str) -> None:
    """A swept, bare-held or foreign-protocol token is absent despite having positions."""
    positions = await repo.list_receipt_token_positions(_PROXY)
    assert symbol not in {p.symbol for p in positions}


@pytest.mark.asyncio
@pytest.mark.parametrize(("live_sql", "reference_sql", "params"), _STAGED_QUERIES)
async def test_staged_query_matches_the_pre_staging_query(
    conn, live_sql: Any, reference_sql: Any, params: dict
) -> None:
    """Each live query returns its pre-staging query's rows, in the same order."""
    assert await _rows_for(conn, live_sql, params) == await _rows_for(conn, reference_sql, params)


@pytest.mark.asyncio
@pytest.mark.parametrize(("live_sql", "reference_sql", "params"), _STAGED_QUERIES)
async def test_pre_staging_query_spills_to_disk(conn, live_sql: Any, reference_sql: Any, params: dict) -> None:
    """The pre-staging formulations spill on this data, so the no-spill tests cannot pass vacuously."""
    assert await _temp_blocks_for(conn, reference_sql, params) > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(("live_sql", "reference_sql", "params"), _STAGED_QUERIES)
async def test_staged_query_writes_no_temp_blocks(conn, live_sql: Any, reference_sql: Any, params: dict) -> None:
    """Each live query sorts a per-key candidate set, so it never reaches disk."""
    assert await _temp_blocks_for(conn, live_sql, params) == 0
