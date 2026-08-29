"""Integration tests for the allocation latest-row reads, now served by the cache.

Five reads answer "what does this proxy hold now": ``_RECEIPT_TOKEN_POSITIONS_SQL``,
``_DIRECT_ASSET_HOLDINGS_SQL``, ``_TOTAL_USD_EXPOSURE_SQL``, ``_USD_EXPOSURE_SQL``
and the crypto-lending ``_WALLET_LOOKUP_SQL``. Each used to walk the
``allocation_position`` history; each now selects from
``allocation_position_current``, whose trigger keeps one row per (proxy, chain,
token).

The differential tests are the substance: every read is compared row for row
against the same selection over the history, kept in ``_HISTORY_*`` below with
the newer-wins order the cache and the migration now agree on. A cache read that
diverges from history — a duplicate, a stale row, a differently broken tie — is a
failure here rather than a silently wrong balance in production.

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
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from app.adapters.postgres.allocation_position_repository import (
    _DIRECT_ASSET_HOLDINGS_SQL,
    _RECEIPT_TOKEN_POSITIONS_SQL,
    _TOTAL_USD_EXPOSURE_SQL,
    _UNDERLYING_VALUE_TOKEN_ADDRS,
    _USD_EXPOSURE_SQL,
    AllocationRepository,
)
from app.adapters.postgres.crypto_lending_reader import _WALLET_LOOKUP_SQL
from app.adapters.postgres.reference_as_of import utc_now
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import (
    RTL_DIRECT_BALANCES,
    RTL_EXCLUDED_SYMBOLS,
    RTL_LATEST_ACTIONS,
    RTL_LATEST_BALANCES,
    RTL_PROXY_HEX,
    RTL_TREASURY_BALANCE,
    RTL_UNDERLYING_PRICE,
    RTL_WALLET_FALLBACK_RECEIPT_HEX,
    RTL_WALLET_HELD_RECEIPT_HEX,
    RTL_WALLET_RECEIPT_BALANCE,
    RTL_WALLET_RECEIPT_HOLDER_HEX,
    RTL_WALLET_UNDERLYING_BALANCE,
    RTL_WALLET_UNDERLYING_HOLDER_HEX,
    rtl_block_time,
    seed_receipt_position_latest_rows,
)

_PROXY = EthAddress(f"0x{RTL_PROXY_HEX}")

# Every receipt token the seed registers, including the ones the reads must drop.
_RECEIPT_SYMBOLS = (
    "rtlAliased",
    "rtlForeign",
    "rtlLogIndex",
    "rtlMultiChain",
    "rtlMultiChainAvax",
    "rtlReorg",
    "rtlReprocessOutranked",
    "rtlReprocessed",
    "rtlSelfTransfer",
    "rtlSharedA",
    "rtlSharedB",
    "rtlSweepFirst",
    "rtlSweepTie",
    "rtlSwept",
    "rtlVersions",
    "rtlWalletFallback",
    "rtlWalletHeld",
)

# Each read as it selected from the history, with the newer-wins order the cache
# now carries substituted in (``created_at`` is the history's name for the
# cache's ``block_timestamp``) and, in the wallet lookup, the chain predicate the
# registry join was missing. Semantic copies: the long rationale comments are
# dropped. Do not point these at the cache — that divergence is what they detect.

_HISTORY_ORDER = """ap.block_number DESC, ap.block_version DESC, ap.created_at DESC,
                 ap.log_index DESC, ap.direction DESC, ap.tx_hash DESC,
                 ap.processing_version DESC"""

_HISTORY_RECEIPT_TOKEN_POSITIONS_SQL = text(f"""
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
        ORDER BY rt.id, {_HISTORY_ORDER}
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
              SELECT 1 FROM oracle_asset_as_of(:reference_effective_at) oa
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

_HISTORY_TOTAL_USD_EXPOSURE_SQL = text(f"""
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
    ORDER BY rt.id, {_HISTORY_ORDER}
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
          SELECT 1 FROM oracle_asset_as_of(:reference_effective_at) oa
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

_HISTORY_DIRECT_ASSET_HOLDINGS_SQL = text(f"""
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
        ORDER BY ap.token_id, {_HISTORY_ORDER}
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
              SELECT 1 FROM oracle_asset_as_of(:reference_effective_at) oa
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
              SELECT 1 FROM oracle_asset_as_of(:reference_effective_at) oa
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

_HISTORY_USD_EXPOSURE_SQL = text(f"""
WITH latest_position AS (
    SELECT
        ap.balance,
        COALESCE(ap.underlying_value, ap.balance) AS valuation_units,
        ap.underlying_token_id AS position_underlying_token_id,
        rt.underlying_token_id AS registry_underlying_token_id
    FROM allocation_position ap
    JOIN receipt_token rt ON rt.id = :receipt_token_id
    JOIN token t ON t.id = ap.token_id AND t.address = rt.receipt_token_address
    JOIN protocol p ON p.id = rt.protocol_id AND p.chain_id = ap.chain_id
    WHERE ap.proxy_address = decode(:proxy_hex, 'hex')
    ORDER BY {_HISTORY_ORDER}
    LIMIT 1
),
latest_price AS (
    SELECT otp.price_usd
    FROM onchain_token_price otp
    JOIN protocol_oracle po ON po.oracle_id = otp.oracle_id
    JOIN receipt_token rt ON rt.protocol_id = po.protocol_id AND rt.id = :receipt_token_id
    WHERE otp.token_id = rt.underlying_token_id
      AND EXISTS (
          SELECT 1 FROM oracle_asset_as_of(:reference_effective_at) oa
          WHERE oa.oracle_id = otp.oracle_id
            AND oa.token_id = otp.token_id
            AND oa.enabled
      )
    ORDER BY otp.block_number DESC, otp.block_version DESC,
             otp.processing_version DESC, otp.oracle_id DESC
    LIMIT 1
)
SELECT
    CASE
        WHEN lb.position_underlying_token_id IS NOT NULL
         AND lb.position_underlying_token_id <> lb.registry_underlying_token_id
        THEN NULL
        ELSE lb.valuation_units * lp.price_usd
    END AS usd_exposure
FROM latest_position lb
CROSS JOIN latest_price lp
WHERE lb.balance > 0
""")

_HISTORY_WALLET_LOOKUP_SQL = text(f"""
WITH latest_receipt AS (
    SELECT DISTINCT ON (ap.proxy_address)
        ap.proxy_address,
        ap.balance
    FROM allocation_position ap
    JOIN token t ON t.id = ap.token_id AND t.address = :receipt_token_address
    WHERE ap.chain_id = :chain_id
    ORDER BY ap.proxy_address, {_HISTORY_ORDER}
),
latest_underlying AS (
    SELECT DISTINCT ON (ap.proxy_address)
        ap.proxy_address,
        ap.balance
    FROM allocation_position ap
    JOIN token t ON t.id = ap.token_id
    JOIN receipt_token rt ON rt.underlying_token_id = t.id
                         AND rt.receipt_token_address = :receipt_token_address
                         AND rt.chain_id = ap.chain_id
    WHERE ap.chain_id = :chain_id
    ORDER BY ap.proxy_address, {_HISTORY_ORDER}
),
candidates AS (
    SELECT proxy_address, balance, 0 AS source_rank
    FROM latest_receipt
    WHERE balance > 0

    UNION ALL

    SELECT proxy_address, balance, 1 AS source_rank
    FROM latest_underlying
    WHERE balance > 0
)
SELECT proxy_address, balance
FROM candidates
ORDER BY source_rank ASC, balance DESC
LIMIT 1
""")


_PROXY_PARAMS = {"proxy_hex": RTL_PROXY_HEX, "reference_effective_at": utc_now()}

# (cache-backed query, history reference, bind parameters).
_CACHE_QUERIES = (
    pytest.param(
        _RECEIPT_TOKEN_POSITIONS_SQL,
        _HISTORY_RECEIPT_TOKEN_POSITIONS_SQL,
        _PROXY_PARAMS,
        id="receipt_token_positions",
    ),
    pytest.param(
        _TOTAL_USD_EXPOSURE_SQL,
        _HISTORY_TOTAL_USD_EXPOSURE_SQL,
        _PROXY_PARAMS,
        id="total_usd_exposure",
    ),
    pytest.param(
        _DIRECT_ASSET_HOLDINGS_SQL,
        _HISTORY_DIRECT_ASSET_HOLDINGS_SQL,
        _PROXY_PARAMS | {"uv_token_addrs": _UNDERLYING_VALUE_TOKEN_ADDRS},
        id="direct_asset_holdings",
    ),
)

_WALLET_RECEIPT_HEXES = (RTL_WALLET_FALLBACK_RECEIPT_HEX, RTL_WALLET_HELD_RECEIPT_HEX)


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed the latest-row scenarios and yield the async URL."""
    asyncio.run(seed_receipt_position_latest_rows(module_db["db_url"]))
    return module_db["async_url"]


@pytest.fixture(scope="module")
def receipt_token_ids(async_db_url: str, db_url: str) -> dict[str, int]:
    """Seeded receipt_token ids by symbol (async_db_url only to order seeding first)."""

    async def _fetch() -> dict[str, int]:
        conn = await asyncpg.connect(db_url)
        try:
            rows = await conn.fetch("SELECT symbol, id FROM receipt_token WHERE symbol LIKE 'rtl%'")
        finally:
            await conn.close()
        return {row["symbol"]: row["id"] for row in rows}

    return asyncio.run(_fetch())


@pytest_asyncio.fixture()
async def repo(async_db_url: str):
    """Bare AllocationRepository for direct-method tests."""
    engine = create_async_engine(async_db_url)
    try:
        yield AllocationRepository(engine, utc_now)
    finally:
        await engine.dispose()


@pytest_asyncio.fixture()
async def conn(async_db_url: str):
    """A connection for running the cache and history queries side by side."""
    engine = create_async_engine(async_db_url)
    try:
        async with engine.connect() as connection:
            yield connection
    finally:
        await engine.dispose()


async def _rows_for(connection: AsyncConnection, sql: Any, params: dict) -> list[tuple]:
    """Return the query's rows as plain tuples, in the order the query emitted them."""
    result = await connection.execute(sql, params)
    return [tuple(row) for row in result.fetchall()]


@pytest.mark.asyncio
@pytest.mark.parametrize(("live_sql", "reference_sql", "params"), _CACHE_QUERIES)
async def test_cache_read_matches_the_history_read(conn, live_sql: Any, reference_sql: Any, params: dict) -> None:
    """Each cache-backed query returns the history query's rows, in the same order."""
    assert await _rows_for(conn, live_sql, params) == await _rows_for(conn, reference_sql, params)


@pytest.mark.asyncio
@pytest.mark.parametrize("symbol", _RECEIPT_SYMBOLS)
async def test_usd_exposure_matches_the_history_read(conn, receipt_token_ids: dict[str, int], symbol: str) -> None:
    """The per-asset exposure read agrees with history for every seeded receipt token."""
    params = _PROXY_PARAMS | {"receipt_token_id": receipt_token_ids[symbol]}
    assert await _rows_for(conn, _USD_EXPOSURE_SQL, params) == await _rows_for(conn, _HISTORY_USD_EXPOSURE_SQL, params)


@pytest.mark.asyncio
@pytest.mark.parametrize("receipt_hex", _WALLET_RECEIPT_HEXES)
async def test_wallet_lookup_matches_the_history_read(conn, receipt_hex: str) -> None:
    """The wallet lookup agrees with history on both its receipt and fallback branches."""
    params = {"receipt_token_address": bytes.fromhex(receipt_hex), "chain_id": 1}
    assert await _rows_for(conn, text(_WALLET_LOOKUP_SQL), params) == await _rows_for(
        conn, _HISTORY_WALLET_LOOKUP_SQL, params
    )


@pytest.mark.asyncio
async def test_receipt_positions_hold_one_row_per_receipt_token(repo) -> None:
    """Two cache rows can reach one receipt_token; only a chain-qualified token join stops that."""
    positions = await repo.list_receipt_token_positions(_PROXY)
    ids = [p.receipt_token_id for p in positions]
    assert sorted(ids) == sorted(set(ids))


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
async def test_receipt_position_reports_the_blocks_time_not_the_cache_rows(repo) -> None:
    """latest_activity_at is the winning row's block time; the cache's own write times never surface."""
    positions = {p.symbol: p for p in await repo.list_receipt_token_positions(_PROXY)}
    assert positions["rtlVersions"].latest_activity_at == rtl_block_time(9_002)


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
async def test_usd_exposure_breaks_an_exact_tie_on_direction(repo, receipt_token_ids: dict[str, int]) -> None:
    """The per-asset exposure read resolves the sweep tie the same way the positions list does."""
    exposure = await repo.get_usd_exposure(receipt_token_ids["rtlSweepTie"], _PROXY)
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
async def test_wallet_lookup_prefers_the_receipt_token_holder(conn) -> None:
    """A wallet holding the receipt token itself outranks any larger underlying holder."""
    params = {"receipt_token_address": bytes.fromhex(RTL_WALLET_HELD_RECEIPT_HEX), "chain_id": 1}
    assert await _rows_for(conn, text(_WALLET_LOOKUP_SQL), params) == [
        (bytes.fromhex(RTL_WALLET_RECEIPT_HOLDER_HEX), RTL_WALLET_RECEIPT_BALANCE)
    ]


@pytest.mark.asyncio
async def test_wallet_lookup_reads_only_the_asked_chains_registration(conn) -> None:
    """The fallback resolves the underlying through this chain's receipt_token row, not another's."""
    params = {"receipt_token_address": bytes.fromhex(RTL_WALLET_FALLBACK_RECEIPT_HEX), "chain_id": 1}
    assert await _rows_for(conn, text(_WALLET_LOOKUP_SQL), params) == [
        (bytes.fromhex(RTL_WALLET_UNDERLYING_HOLDER_HEX), RTL_WALLET_UNDERLYING_BALANCE)
    ]
