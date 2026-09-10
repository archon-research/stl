"""Current-position tables: trigger upkeep, and the backed-breakdown read that uses them.

Every scenario seeds its own protocol / oracle / tokens, so the module's shared
database keeps the scenarios independent of each other and of ordering.
"""

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.aave_like_backed_breakdown_repository import AaveLikeBackedBreakdownRepository
from app.adapters.postgres.reference_as_of import utc_now
from tests.integration.conftest import MIGRATIONS_DIR
from tests.integration.seed import (
    ORACLE_ASSET_RETIRED_FROM,
    bind_protocol_oracle,
    insert_borrower_collateral,
    insert_borrower_debt,
    insert_onchain_price,
    insert_oracle,
    insert_oracle_asset,
    insert_protocol,
    insert_reserve_data,
    insert_token,
    insert_user,
    retire_oracle_asset,
)

_BLOCK = 30_000_000


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def conn(db_url: str) -> AsyncIterator[asyncpg.Connection]:
    """One connection for the module's isolated database."""
    connection = await asyncpg.connect(db_url)
    try:
        yield connection
    finally:
        await connection.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repository(async_db_url: str) -> AsyncIterator[AaveLikeBackedBreakdownRepository]:
    """The Aave-like backed breakdown repository under test."""
    engine = create_async_engine(async_db_url)
    try:
        yield AaveLikeBackedBreakdownRepository(engine, utc_now)
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------
# Trigger upkeep
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="module")
async def test_newer_debt_insert_updates_current_row(conn: asyncpg.Connection) -> None:
    """A debt snapshot at a higher block replaces the current row."""
    protocol_id = await insert_protocol(conn, "curNewer", b"\x11" * 20)
    token_id = await insert_token(conn, "CURNEWER", 18, b"\x12" * 20)
    user_id = await insert_user(conn, b"\x13" * 20)

    await insert_borrower_debt(
        conn,
        protocol_id=protocol_id,
        user_id=user_id,
        token_id=token_id,
        amount=100,
        block=_BLOCK,
    )
    await insert_borrower_debt(
        conn,
        protocol_id=protocol_id,
        user_id=user_id,
        token_id=token_id,
        amount=250,
        block=_BLOCK + 1,
    )

    row = await conn.fetchrow(
        "SELECT amount, block_number FROM borrower_current WHERE protocol_id = $1 AND user_id = $2 AND token_id = $3",
        protocol_id,
        user_id,
        token_id,
    )
    assert row is not None
    assert row["amount"] == Decimal(250)
    assert row["block_number"] == _BLOCK + 1


@pytest.mark.asyncio(loop_scope="module")
async def test_out_of_order_debt_insert_does_not_regress_current_row(conn: asyncpg.Connection) -> None:
    """An older snapshot arriving late (backfill, retry) must not overwrite the current row."""
    protocol_id = await insert_protocol(conn, "curReorder", b"\x21" * 20)
    token_id = await insert_token(conn, "CURREORDER", 18, b"\x22" * 20)
    user_id = await insert_user(conn, b"\x23" * 20)

    await insert_borrower_debt(
        conn,
        protocol_id=protocol_id,
        user_id=user_id,
        token_id=token_id,
        amount=250,
        block=_BLOCK + 1,
    )
    await insert_borrower_debt(
        conn,
        protocol_id=protocol_id,
        user_id=user_id,
        token_id=token_id,
        amount=100,
        block=_BLOCK,
    )

    row = await conn.fetchrow(
        "SELECT amount, block_number FROM borrower_current WHERE protocol_id = $1 AND user_id = $2 AND token_id = $3",
        protocol_id,
        user_id,
        token_id,
    )
    assert row is not None
    assert row["amount"] == Decimal(250)
    assert row["block_number"] == _BLOCK + 1


@pytest.mark.asyncio(loop_scope="module")
async def test_collateral_flag_follows_the_newest_snapshot(conn: asyncpg.Connection) -> None:
    """Disabling a deposit as collateral at a higher block flips the current row's flag."""
    protocol_id = await insert_protocol(conn, "curCollateral", b"\x31" * 20)
    token_id = await insert_token(conn, "CURCOLL", 18, b"\x32" * 20)
    user_id = await insert_user(conn, b"\x33" * 20)

    await insert_borrower_collateral(
        conn,
        protocol_id=protocol_id,
        user_id=user_id,
        token_id=token_id,
        amount=5,
        block=_BLOCK,
    )
    await insert_borrower_collateral(
        conn,
        protocol_id=protocol_id,
        user_id=user_id,
        token_id=token_id,
        amount=5,
        block=_BLOCK + 1,
        collateral_enabled=False,
    )

    enabled = await conn.fetchval(
        "SELECT collateral_enabled FROM borrower_collateral_current "
        "WHERE protocol_id = $1 AND user_id = $2 AND token_id = $3",
        protocol_id,
        user_id,
        token_id,
    )
    assert enabled is False


@pytest.mark.asyncio(loop_scope="module")
async def test_null_processing_version_reserve_row_does_not_break_ingest(conn: asyncpg.Connection) -> None:
    """A residual NULL processing_version must not abort the insert that fires the trigger.

    sparklend_reserve_data is the one history table of the four whose
    processing_version is nullable (pre-convention retrofit). Its assign trigger
    copies an existing row's version verbatim, so a residual NULL row makes the
    NEXT insert for the same key carry NULL too — which, against a NOT NULL cache
    column, would abort the history insert itself and stop ingest. The cache
    COALESCEs it to -1 instead ("pre-convention, version unknown").
    """
    protocol_id = await insert_protocol(conn, "curNullPv", b"\xa1" * 20)
    token_id = await insert_token(conn, "CURNULLPV", 18, b"\xa2" * 20)

    await insert_reserve_data(conn, protocol_id=protocol_id, token_id=token_id, block=_BLOCK, collateral_enabled=True)
    # Simulate a row left behind by the retrofit, before processing_version was populated.
    await conn.execute(
        "UPDATE sparklend_reserve_data SET processing_version = NULL "
        "WHERE protocol_id = $1 AND token_id = $2 AND block_number = $3",
        protocol_id,
        token_id,
        _BLOCK,
    )

    # Same (protocol, token, block, block_version, build_id): the assign trigger
    # takes the existing NULL rather than computing a fresh version.
    await insert_reserve_data(conn, protocol_id=protocol_id, token_id=token_id, block=_BLOCK, collateral_enabled=False)

    # The insert above did not raise, which is the property under test. A cache row
    # is still present for the key — the NULL row is not newer than what the first,
    # non-NULL insert already cached, so it does not overwrite it.
    assert (
        await conn.fetchval(
            "SELECT count(*) FROM sparklend_reserve_data_current WHERE protocol_id = $1 AND token_id = $2",
            protocol_id,
            token_id,
        )
        == 1
    )


# The backfill statement for sparklend_reserve_data_current, scoped to one key.
# Kept in step with 20260820_120000_create_current_position_tables.sql so the test
# exercises the real recovery path rather than a paraphrase of it.
_RESERVE_BACKFILL = """
INSERT INTO sparklend_reserve_data_current
    (protocol_id, token_id, usage_as_collateral_enabled,
     block_number, block_version, processing_version)
SELECT DISTINCT ON (srd.protocol_id, srd.token_id)
    srd.protocol_id, srd.token_id, srd.usage_as_collateral_enabled,
    srd.block_number, srd.block_version, COALESCE(srd.processing_version, -1)
FROM sparklend_reserve_data srd
WHERE srd.protocol_id = $1 AND srd.token_id = $2
ORDER BY srd.protocol_id, srd.token_id,
         srd.block_number DESC, srd.block_version DESC, COALESCE(srd.processing_version, -1) DESC
ON CONFLICT (protocol_id, token_id) DO UPDATE SET
    usage_as_collateral_enabled = EXCLUDED.usage_as_collateral_enabled,
    block_number = EXCLUDED.block_number,
    block_version = EXCLUDED.block_version,
    processing_version = EXCLUDED.processing_version
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version)
    > (sparklend_reserve_data_current.block_number, sparklend_reserve_data_current.block_version,
       sparklend_reserve_data_current.processing_version)
"""


@pytest.mark.asyncio(loop_scope="module")
async def test_correction_lands_over_a_backfilled_null_version_row(conn: asyncpg.Connection) -> None:
    """A correction to a key whose cached row came from NULL-versioned history must land.

    This is the production shape. Pre-convention rows carry a NULL
    processing_version, and the cache for such a key is first populated by the
    migration's backfill — the assign trigger cannot produce a NULL row, it always
    assigns. A later correction to that key arrives as version **0**, because the
    trigger computes COALESCE(MAX(processing_version), -1) + 1 over rows that are
    all NULL. So the cached sentinel must sort strictly below 0, or the newer-wins
    guard drops the correction silently and the reserve flag never updates. -1 does;
    0 would not, and this test fails if the sentinel is changed to 0.
    """
    protocol_id = await insert_protocol(conn, "curNullPvFix", b"\xa5" * 20)
    token_id = await insert_token(conn, "CURNULLPVFIX", 18, b"\xa6" * 20)

    # Build a genuine pre-convention row: NULL version, with no cache row for it yet.
    await insert_reserve_data(conn, protocol_id=protocol_id, token_id=token_id, block=_BLOCK, collateral_enabled=True)
    await conn.execute(
        "UPDATE sparklend_reserve_data SET processing_version = NULL WHERE protocol_id = $1 AND token_id = $2",
        protocol_id,
        token_id,
    )
    await conn.execute(
        "DELETE FROM sparklend_reserve_data_current WHERE protocol_id = $1 AND token_id = $2",
        protocol_id,
        token_id,
    )

    await conn.execute(_RESERVE_BACKFILL, protocol_id, token_id)
    assert (
        await conn.fetchval(
            "SELECT processing_version FROM sparklend_reserve_data_current WHERE protocol_id = $1 AND token_id = $2",
            protocol_id,
            token_id,
        )
        == -1
    )

    # A distinct build_id takes the MAX(...)+1 branch, so this correction is version 0.
    await conn.execute(
        "INSERT INTO sparklend_reserve_data (protocol_id, token_id, block_number, block_version, "
        "usage_as_collateral_enabled, build_id) VALUES ($1, $2, $3, 0, false, 999999)",
        protocol_id,
        token_id,
        _BLOCK,
    )

    row = await conn.fetchrow(
        "SELECT usage_as_collateral_enabled, processing_version FROM sparklend_reserve_data_current "
        "WHERE protocol_id = $1 AND token_id = $2",
        protocol_id,
        token_id,
    )
    assert row is not None
    assert row["usage_as_collateral_enabled"] is False
    assert row["processing_version"] == 0


# A full sparklend_reserve_data payload: one distinct numeric per column, and the
# four flags as parameters, so a copy landing in the wrong column is caught. The
# widening tests below share it.
_FULL_RESERVE_ROW_SQL = """
INSERT INTO sparklend_reserve_data
    (protocol_id, token_id, block_number, block_version, usage_as_collateral_enabled,
     unbacked, accrued_to_treasury_scaled, total_a_token, total_stable_debt,
     total_variable_debt, liquidity_rate, variable_borrow_rate, stable_borrow_rate,
     average_stable_borrow_rate, liquidity_index, variable_borrow_index,
     last_update_timestamp, decimals, ltv, liquidation_threshold, liquidation_bonus,
     reserve_factor, borrowing_enabled, stable_borrow_rate_enabled, is_active, is_frozen)
VALUES ($1, $2, $3, 0, true,
        1, 2, 3, 4,
        5, 6, 7, 8,
        9, 10, 11,
        1800000000, 18, 7500, 8250, 10500,
        1000, $4, $5, $6, $7)
"""

_FLAG_COLUMNS = ("borrowing_enabled", "stable_borrow_rate_enabled", "is_active", "is_frozen")
# Two complementary flag patterns: every pairwise swap of the four flag columns
# flips at least one assertion under one of them.
_FLAG_PATTERNS: list[tuple[bool, ...]] = [(True, False, True, False), (True, True, False, False)]

# What the trigger must have written for _FULL_RESERVE_ROW_SQL: the two guarded
# canonical casts (last_update_at, decimals) beside the verbatim copies.
_FULL_RESERVE_ROW_CACHED = {
    "unbacked": 1,
    "accrued_to_treasury_scaled": 2,
    "total_a_token": 3,
    "total_stable_debt": 4,
    "total_variable_debt": 5,
    "liquidity_rate": 6,
    "variable_borrow_rate": 7,
    "stable_borrow_rate": 8,
    "average_stable_borrow_rate": 9,
    "liquidity_index": 10,
    "variable_borrow_index": 11,
    "last_update_at": datetime(2027, 1, 15, 8, 0, tzinfo=UTC),
    "decimals": 18,
    "ltv": 7500,
    "liquidation_threshold": 8250,
    "liquidation_bonus": 10500,
    "reserve_factor": 1000,
}

_PAYLOAD_BACKFILL = MIGRATIONS_DIR / "20260910_130050_backfill_sparklend_reserve_data_current_payload.sql"
# to_timestamp(1800000000), the in-bounds epoch the full row and the guard cases carry.
_PLAUSIBLE_EPOCH_AT = datetime(2027, 1, 15, 8, 0, tzinfo=UTC)


def _expected_payload(flags: tuple[bool, ...]) -> dict[str, object]:
    return {**_FULL_RESERVE_ROW_CACHED, **dict(zip(_FLAG_COLUMNS, flags))}


async def _cached_reserve(conn: asyncpg.Connection, protocol_id: int, token_id: int) -> asyncpg.Record:
    row = await conn.fetchrow(
        "SELECT * FROM sparklend_reserve_data_current WHERE protocol_id = $1 AND token_id = $2",
        protocol_id,
        token_id,
    )
    assert row is not None
    return row


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize("flags", _FLAG_PATTERNS, ids=["tftf", "ttff"])
async def test_reserve_trigger_propagates_the_whole_row(conn: asyncpg.Connection, flags: tuple[bool, ...]) -> None:
    """The cache carries the source row's full payload, not just the collateral flag.

    Every sparklend_reserve_data column is cached so a new reader needs no
    migration; last_update_at and decimals arrive as their canonical casts.
    """
    seed = 0xB3 + 2 * _FLAG_PATTERNS.index(flags)
    protocol_id = await insert_protocol(conn, f"curWide{seed:x}", bytes([seed]) * 20)
    token_id = await insert_token(conn, f"CURWIDE{seed:x}", 18, bytes([seed + 1]) * 20)

    await conn.execute(_FULL_RESERVE_ROW_SQL, protocol_id, token_id, _BLOCK, *flags)

    row = await _cached_reserve(conn, protocol_id, token_id)
    expected = _expected_payload(flags)
    assert {column: row[column] for column in expected} == expected


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize(
    ("last_update_timestamp", "decimals", "cached_last_update_at", "cached_decimals", "seed"),
    [
        pytest.param(-62135596800, 18, None, 18, 0xB7, id="epoch outside the plausibility bounds"),
        pytest.param(1800000000, 100000, _PLAUSIBLE_EPOCH_AT, None, 0xB9, id="decimals above uint8"),
        pytest.param(1800000000, Decimal("18.5"), _PLAUSIBLE_EPOCH_AT, None, 0xBB, id="fractional decimals"),
    ],
)
async def test_implausible_reserve_values_cache_as_null_without_aborting_ingest(
    conn: asyncpg.Connection,
    last_update_timestamp: int,
    decimals: object,
    cached_last_update_at: datetime | None,
    cached_decimals: int | None,
    seed: int,
) -> None:
    """A source value the canonical cast cannot represent caches as NULL, and the insert lands.

    The history column's COMMENT records that some epochs are corrupt (some
    negative), and nothing constrains decimals to a uint8; an unguarded cast inside
    the trigger would raise (or, for a fraction, round) and abort the history insert.
    """
    protocol_id = await insert_protocol(conn, f"curGuard{seed:x}", bytes([seed]) * 20)
    token_id = await insert_token(conn, f"CURGUARD{seed:x}", 18, bytes([seed + 1]) * 20)

    await conn.execute(
        """
        INSERT INTO sparklend_reserve_data
            (protocol_id, token_id, block_number, block_version, usage_as_collateral_enabled,
             last_update_timestamp, decimals)
        VALUES ($1, $2, $3, 0, true, $4, $5)
        """,
        protocol_id,
        token_id,
        _BLOCK,
        last_update_timestamp,
        decimals,
    )

    row = await _cached_reserve(conn, protocol_id, token_id)
    assert row["block_number"] == _BLOCK
    assert row["last_update_at"] == cached_last_update_at
    assert row["decimals"] == cached_decimals


@pytest.mark.asyncio(loop_scope="module")
async def test_payload_backfill_fills_a_cache_row_left_behind_by_the_widening(conn: asyncpg.Connection) -> None:
    """20260910_130050 fills the payload of a row that predates the widening.

    On a fresh database the migrator's own run of that file matches nothing (the
    cache is empty when it runs), so this is the only exercise of the statement
    that fills the 21 new columns for every production row. The pre-widening shape
    is reproduced by NULLing the payload the trigger just wrote, then the real
    migration file is executed against it.
    """
    protocol_id = await insert_protocol(conn, "curBackfill", b"\xc1" * 20)
    token_id = await insert_token(conn, "CURBACKFILL", 18, b"\xc2" * 20)
    flags = _FLAG_PATTERNS[0]
    await conn.execute(_FULL_RESERVE_ROW_SQL, protocol_id, token_id, _BLOCK, *flags)

    expected = _expected_payload(flags)
    payload = ", ".join(f"{column} = NULL" for column in expected)
    await conn.execute(
        f"UPDATE sparklend_reserve_data_current SET {payload} WHERE protocol_id = $1 AND token_id = $2",  # noqa: S608
        protocol_id,
        token_id,
    )
    before = await _cached_reserve(conn, protocol_id, token_id)
    assert all(before[column] is None for column in expected)

    await conn.execute(_PAYLOAD_BACKFILL.read_text())

    row = await _cached_reserve(conn, protocol_id, token_id)
    assert {column: row[column] for column in expected} == expected
    assert (row["block_number"], row["block_version"], row["processing_version"]) == (_BLOCK, 0, 0)


@pytest.mark.asyncio(loop_scope="module")
async def test_payload_backfill_repairs_a_key_missing_from_the_cache(conn: asyncpg.Connection) -> None:
    """The same statement is the rebuild: a key history has but the cache lacks is inserted whole."""
    protocol_id = await insert_protocol(conn, "curRepair", b"\xc3" * 20)
    token_id = await insert_token(conn, "CURREPAIR", 18, b"\xc4" * 20)
    flags = _FLAG_PATTERNS[1]
    await conn.execute(_FULL_RESERVE_ROW_SQL, protocol_id, token_id, _BLOCK, *flags)
    await conn.execute(
        "DELETE FROM sparklend_reserve_data_current WHERE protocol_id = $1 AND token_id = $2", protocol_id, token_id
    )

    await conn.execute(_PAYLOAD_BACKFILL.read_text())

    row = await _cached_reserve(conn, protocol_id, token_id)
    expected = _expected_payload(flags)
    assert {column: row[column] for column in expected} == expected
    assert (row["block_number"], row["block_version"], row["processing_version"]) == (_BLOCK, 0, 0)


@pytest.mark.asyncio(loop_scope="module")
async def test_payload_backfill_never_lowers_a_cache_row_ahead_of_history(conn: asyncpg.Connection) -> None:
    """The backfill's `>=` guard converges the equal case and refuses to lower a newer row.

    A cache row ahead of the newest readable history row (a reserve whose newest
    history row is not visible to the scan) keeps its version tuple, and its
    payload is not overwritten from the older history row either.
    """
    protocol_id = await insert_protocol(conn, "curAhead", b"\xc5" * 20)
    token_id = await insert_token(conn, "CURAHEAD", 18, b"\xc6" * 20)
    await conn.execute(_FULL_RESERVE_ROW_SQL, protocol_id, token_id, _BLOCK, *_FLAG_PATTERNS[0])

    await conn.execute(
        "UPDATE sparklend_reserve_data_current SET block_number = $3, ltv = NULL "
        "WHERE protocol_id = $1 AND token_id = $2",
        protocol_id,
        token_id,
        _BLOCK + 5,
    )

    await conn.execute(_PAYLOAD_BACKFILL.read_text())

    row = await _cached_reserve(conn, protocol_id, token_id)
    assert row["block_number"] == _BLOCK + 5
    assert row["ltv"] is None


@pytest.mark.asyncio(loop_scope="module")
async def test_out_of_order_reserve_insert_does_not_regress_current_row(conn: asyncpg.Connection) -> None:
    """A late older reserve snapshot must not resurrect the flag it already superseded."""
    protocol_id = await insert_protocol(conn, "curReserve", b"\x41" * 20)
    token_id = await insert_token(conn, "CURRESERVE", 18, b"\x42" * 20)

    await insert_reserve_data(
        conn,
        protocol_id=protocol_id,
        token_id=token_id,
        block=_BLOCK + 1,
        collateral_enabled=False,
    )
    await insert_reserve_data(conn, protocol_id=protocol_id, token_id=token_id, block=_BLOCK, collateral_enabled=True)

    row = await conn.fetchrow(
        "SELECT usage_as_collateral_enabled, block_number FROM sparklend_reserve_data_current "
        "WHERE protocol_id = $1 AND token_id = $2",
        protocol_id,
        token_id,
    )
    assert row is not None
    assert row["usage_as_collateral_enabled"] is False
    assert row["block_number"] == _BLOCK + 1


@pytest.mark.asyncio(loop_scope="module")
async def test_price_is_cached_per_oracle_and_knows_nothing_about_protocols(conn: asyncpg.Connection) -> None:
    """One price row becomes exactly one current row, keyed by (oracle, token).

    The write path carries no configuration predicate at all: bindings and enabled
    mappings are the read side's business, so the row appears whether or not any
    protocol is bound and whether or not the mapping is enabled. Keeping config out
    of the trigger is what makes a re-enable or a new binding take effect on the
    next read rather than the next price.
    """
    oracle_id = await insert_oracle(conn, "cur_percache", b"\x51" * 20)
    # Two bound protocols and one deliberately unbound: none of it changes the cache.
    first_protocol_id = await insert_protocol(conn, "curCacheA", b"\x52" * 20)
    second_protocol_id = await insert_protocol(conn, "curCacheB", b"\x53" * 20)
    await insert_protocol(conn, "curCacheUnbound", b"\x54" * 20)
    token_id = await insert_token(conn, "CURCACHED", 18, b"\x55" * 20)
    await bind_protocol_oracle(conn, first_protocol_id, oracle_id)
    await bind_protocol_oracle(conn, second_protocol_id, oracle_id)
    # Disabled on purpose: the cache must still take the row.
    await insert_oracle_asset(conn, oracle_id, token_id, enabled=False)

    await insert_onchain_price(conn, token_id=token_id, oracle_id=oracle_id, price="7.5", block=_BLOCK)

    rows = await conn.fetch(
        "SELECT oracle_id, price_usd FROM token_price_current WHERE token_id = $1",
        token_id,
    )
    assert [(row["oracle_id"], row["price_usd"]) for row in rows] == [(oracle_id, Decimal("7.5"))]


@pytest.mark.asyncio(loop_scope="module")
async def test_out_of_order_price_insert_does_not_regress_current_row(conn: asyncpg.Connection) -> None:
    """A newer price wins; an older one arriving late (backfill, retry) does not."""
    oracle_id = await insert_oracle(conn, "cur_price_order", b"\x61" * 20)
    token_id = await insert_token(conn, "CURPRICEORDER", 18, b"\x63" * 20)
    await insert_oracle_asset(conn, oracle_id, token_id)

    await insert_onchain_price(conn, token_id=token_id, oracle_id=oracle_id, price="3.0", block=_BLOCK)
    await insert_onchain_price(conn, token_id=token_id, oracle_id=oracle_id, price="9.0", block=_BLOCK + 1)
    await insert_onchain_price(conn, token_id=token_id, oracle_id=oracle_id, price="1.0", block=_BLOCK - 1)

    row = await conn.fetchrow(
        "SELECT price_usd, block_number FROM token_price_current WHERE oracle_id = $1 AND token_id = $2",
        oracle_id,
        token_id,
    )
    assert row is not None
    assert row["price_usd"] == Decimal("9.0")
    assert row["block_number"] == _BLOCK + 1


# ---------------------------------------------------------------------------
# Backed breakdown over the current tables
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def attribution_ids(conn: asyncpg.Connection) -> dict[str, int]:
    """Seed a three-borrower protocol whose history exercises every current-table rule.

    Prices: COLLA = $100, COLLB = $2. Reserves: COLLA accepted from the start,
    COLLB rejected then accepted at a higher block. Borrowers:

    * user 1 -- COLLA deposit inserted out of order (20 at block+1 written before
      10 at block, so the current row must stay 20 -> $2,000) plus 500 COLLB
      ($1,000); debt 900 DEBT. COLLA gets 2000/3000 x 900 = $600, COLLB $300.
    * user 2 -- 5 COLLA ($500) + 1,000 COLLB ($2,000); debt 250 DEBT.
      COLLA gets 500/2500 x 250 = $50, COLLB $200.
    * user 3 -- 100 COLLA, disabled as collateral at a higher block, so its
      $10,000 must not back the 700 DEBT it borrows.

    Totals: COLLA $650 (56.5217%), COLLB $500 (43.4783%).
    """
    protocol_id = await insert_protocol(conn, "curAttribution", b"\x71" * 20)
    oracle_id = await insert_oracle(conn, "cur_attribution", b"\x72" * 20)
    await bind_protocol_oracle(conn, protocol_id, oracle_id)

    coll_a_id = await insert_token(conn, "COLLA", 18, b"\x73" * 20)
    coll_b_id = await insert_token(conn, "COLLB", 6, b"\x74" * 20)
    debt_id = await insert_token(conn, "DEBT577", 18, b"\x75" * 20)
    for token_id, price in ((coll_a_id, "100.0"), (coll_b_id, "2.0")):
        await insert_oracle_asset(conn, oracle_id, token_id)
        await insert_onchain_price(conn, token_id=token_id, oracle_id=oracle_id, price=price, block=_BLOCK)

    await insert_reserve_data(conn, protocol_id=protocol_id, token_id=coll_a_id, block=_BLOCK, collateral_enabled=True)
    await insert_reserve_data(conn, protocol_id=protocol_id, token_id=coll_b_id, block=_BLOCK, collateral_enabled=False)
    await insert_reserve_data(
        conn,
        protocol_id=protocol_id,
        token_id=coll_b_id,
        block=_BLOCK + 1,
        collateral_enabled=True,
    )

    user_1 = await insert_user(conn, b"\x76" * 20)
    await insert_borrower_collateral(
        conn, protocol_id=protocol_id, user_id=user_1, token_id=coll_a_id, amount=20 * 10**18, block=_BLOCK + 1
    )
    await insert_borrower_collateral(
        conn, protocol_id=protocol_id, user_id=user_1, token_id=coll_a_id, amount=10 * 10**18, block=_BLOCK
    )
    await insert_borrower_collateral(
        conn, protocol_id=protocol_id, user_id=user_1, token_id=coll_b_id, amount=500 * 10**6, block=_BLOCK
    )
    await insert_borrower_debt(
        conn, protocol_id=protocol_id, user_id=user_1, token_id=debt_id, amount=900 * 10**18, block=_BLOCK
    )

    user_2 = await insert_user(conn, b"\x77" * 20)
    await insert_borrower_collateral(
        conn, protocol_id=protocol_id, user_id=user_2, token_id=coll_a_id, amount=5 * 10**18, block=_BLOCK
    )
    await insert_borrower_collateral(
        conn, protocol_id=protocol_id, user_id=user_2, token_id=coll_b_id, amount=1_000 * 10**6, block=_BLOCK
    )
    await insert_borrower_debt(
        conn, protocol_id=protocol_id, user_id=user_2, token_id=debt_id, amount=250 * 10**18, block=_BLOCK
    )

    user_3 = await insert_user(conn, b"\x78" * 20)
    await insert_borrower_collateral(
        conn, protocol_id=protocol_id, user_id=user_3, token_id=coll_a_id, amount=100 * 10**18, block=_BLOCK
    )
    await insert_borrower_collateral(
        conn,
        protocol_id=protocol_id,
        user_id=user_3,
        token_id=coll_a_id,
        amount=100 * 10**18,
        block=_BLOCK + 2,
        collateral_enabled=False,
    )
    await insert_borrower_debt(
        conn, protocol_id=protocol_id, user_id=user_3, token_id=debt_id, amount=700 * 10**18, block=_BLOCK
    )

    return {"protocol_id": protocol_id, "debt_id": debt_id}


@pytest.mark.asyncio(loop_scope="module")
async def test_backed_breakdown_matches_hand_computed_attribution(
    repository: AaveLikeBackedBreakdownRepository, attribution_ids: dict[str, int]
) -> None:
    """The breakdown over the current tables matches the figures derived in the seed docstring."""
    result = await repository.get_backed_breakdown(attribution_ids["protocol_id"], attribution_ids["debt_id"])

    by_symbol = {item.symbol: item for item in result.items}
    assert set(by_symbol) == {"COLLA", "COLLB"}

    assert by_symbol["COLLA"].backing_value == Decimal("650.00")
    assert by_symbol["COLLB"].backing_value == Decimal("500.00")
    assert by_symbol["COLLA"].backing_pct == Decimal("56.5217")
    assert by_symbol["COLLB"].backing_pct == Decimal("43.4783")
    assert by_symbol["COLLA"].price_usd == Decimal("100")
    assert by_symbol["COLLB"].price_usd == Decimal("2")


@pytest.mark.asyncio(loop_scope="module")
async def test_disabling_a_mapping_drops_the_price_at_read_time(
    conn: asyncpg.Connection, repository: AaveLikeBackedBreakdownRepository
) -> None:
    """Retiring a token's only price source takes effect on the next read.

    The current price row it already wrote stays behind, so only the read-time
    re-check of the enabled mapping can drop it: the collateral loses its USD
    value and all backing moves to the token that is still priced.
    """
    protocol_id = await insert_protocol(conn, "curRetired", b"\x81" * 20)
    oracle_id = await insert_oracle(conn, "cur_retired", b"\x82" * 20)
    await bind_protocol_oracle(conn, protocol_id, oracle_id)

    keep_id = await insert_token(conn, "KEEPPRICE", 18, b"\x83" * 20)
    drop_id = await insert_token(conn, "DROPPRICE", 18, b"\x84" * 20)
    debt_id = await insert_token(conn, "RETIREDDEBT", 18, b"\x85" * 20)
    for token_id, price in ((keep_id, "3.0"), (drop_id, "4.0")):
        await insert_oracle_asset(conn, oracle_id, token_id)
        await insert_onchain_price(conn, token_id=token_id, oracle_id=oracle_id, price=price, block=_BLOCK)
        await insert_reserve_data(
            conn,
            protocol_id=protocol_id,
            token_id=token_id,
            block=_BLOCK,
            collateral_enabled=True,
        )

    user_id = await insert_user(conn, b"\x86" * 20)
    await insert_borrower_collateral(
        conn, protocol_id=protocol_id, user_id=user_id, token_id=keep_id, amount=10 * 10**18, block=_BLOCK
    )
    await insert_borrower_collateral(
        conn, protocol_id=protocol_id, user_id=user_id, token_id=drop_id, amount=5 * 10**18, block=_BLOCK
    )
    await insert_borrower_debt(
        conn, protocol_id=protocol_id, user_id=user_id, token_id=debt_id, amount=100 * 10**18, block=_BLOCK
    )

    before = await repository.get_backed_breakdown(protocol_id, debt_id)
    assert {item.symbol: item.backing_value for item in before.items} == {
        "KEEPPRICE": Decimal("60.00"),
        "DROPPRICE": Decimal("40.00"),
    }

    # Retiring a source is a configuration change on the mapping, not a rewrite
    # of any price history. Append-on-change, so the retirement is a new version.
    await retire_oracle_asset(conn, oracle_id, drop_id, ORACLE_ASSET_RETIRED_FROM, "test: source retired")
    assert (
        await conn.fetchval(
            "SELECT count(*) FROM token_price_current WHERE oracle_id = $1 AND token_id = $2", oracle_id, drop_id
        )
        == 1
    )

    after = await repository.get_backed_breakdown(protocol_id, debt_id)
    assert {item.symbol: item.backing_value for item in after.items} == {"KEEPPRICE": Decimal("100.00")}
    assert {item.symbol: item.backing_pct for item in after.items} == {"KEEPPRICE": Decimal("100.0000")}


@pytest.mark.asyncio(loop_scope="module")
async def test_disabling_a_mapping_falls_back_to_the_next_enabled_oracle(
    conn: asyncpg.Connection, repository: AaveLikeBackedBreakdownRepository
) -> None:
    """With two oracles bound, retiring the winning mapping falls back — it does not unprice the token.

    This is the case a cache keyed by (protocol, token) cannot express: only one
    row per protocol survives there, so disabling its mapping can only delete the
    price, never substitute the other oracle's. The collateral would then leave
    both the numerator and the denominator of the backing ratio, silently inflating
    every other token's share. Keying by (oracle, token) keeps both prices and lets
    the read rank them, so the fallback the pre-cache query performed still happens.
    """
    protocol_id = await insert_protocol(conn, "curFallback", b"\x91" * 20)
    primary_oracle_id = await insert_oracle(conn, "cur_fallback_primary", b"\x92" * 20)
    backup_oracle_id = await insert_oracle(conn, "cur_fallback_backup", b"\x93" * 20)
    await bind_protocol_oracle(conn, protocol_id, primary_oracle_id)
    await bind_protocol_oracle(conn, protocol_id, backup_oracle_id)

    coll_id = await insert_token(conn, "FALLBACKCOLL", 18, b"\x94" * 20)
    debt_id = await insert_token(conn, "FALLBACKDEBT", 18, b"\x95" * 20)
    await insert_reserve_data(conn, protocol_id=protocol_id, token_id=coll_id, block=_BLOCK, collateral_enabled=True)

    # Both oracles price the collateral. The primary wins on block number.
    await insert_oracle_asset(conn, primary_oracle_id, coll_id)
    await insert_oracle_asset(conn, backup_oracle_id, coll_id)
    await insert_onchain_price(conn, token_id=coll_id, oracle_id=backup_oracle_id, price="2.0", block=_BLOCK)
    await insert_onchain_price(conn, token_id=coll_id, oracle_id=primary_oracle_id, price="10.0", block=_BLOCK + 1)

    user_id = await insert_user(conn, b"\x96" * 20)
    await insert_borrower_collateral(
        conn, protocol_id=protocol_id, user_id=user_id, token_id=coll_id, amount=10 * 10**18, block=_BLOCK
    )
    await insert_borrower_debt(
        conn, protocol_id=protocol_id, user_id=user_id, token_id=debt_id, amount=100 * 10**18, block=_BLOCK
    )

    before = await repository.get_backed_breakdown(protocol_id, debt_id)
    assert {item.symbol: item.price_usd for item in before.items} == {"FALLBACKCOLL": Decimal("10")}

    await retire_oracle_asset(
        conn, primary_oracle_id, coll_id, ORACLE_ASSET_RETIRED_FROM, "test: primary source retired"
    )

    # Both rows are still cached; the read now picks the backup oracle's price.
    assert await conn.fetchval("SELECT count(*) FROM token_price_current WHERE token_id = $1", coll_id) == 2
    after = await repository.get_backed_breakdown(protocol_id, debt_id)
    assert {item.symbol: item.price_usd for item in after.items} == {"FALLBACKCOLL": Decimal("2")}
    # The token keeps backing the debt in full — it did not drop out of the ratio.
    assert {item.symbol: item.backing_value for item in after.items} == {"FALLBACKCOLL": Decimal("100.00")}


@pytest.mark.asyncio(loop_scope="module")
async def test_price_cache_carries_the_winning_rows_timestamp(conn: asyncpg.Connection) -> None:
    """The cache copies the winning history row's observation time and moves it with the row.

    ``/v1/tokens/…/price`` and the CORE feed-liveness check read the timestamp from
    here instead of the history (VEC-672), so it must always be the timestamp of the
    row whose price the cache holds: it follows a newer insert and ignores an older
    one arriving late.
    """
    oracle_id = await insert_oracle(conn, "cur_price_ts", b"\xb1" * 20)
    token_id = await insert_token(conn, "CURPRICETS", 18, b"\xb2" * 20)
    await insert_oracle_asset(conn, oracle_id, token_id)

    async def history_timestamp(block: int):
        return await conn.fetchval(
            'SELECT "timestamp" FROM onchain_token_price WHERE oracle_id = $1 AND token_id = $2 AND block_number = $3',
            oracle_id,
            token_id,
            block,
        )

    async def cached():
        row = await conn.fetchrow(
            "SELECT block_timestamp, block_number, price_usd FROM token_price_current "
            "WHERE oracle_id = $1 AND token_id = $2",
            oracle_id,
            token_id,
        )
        assert row is not None
        return row

    await insert_onchain_price(
        conn, token_id=token_id, oracle_id=oracle_id, price="3.0", block=_BLOCK, time_offset="-2 hours"
    )
    first = await cached()
    assert first["block_timestamp"] == await history_timestamp(_BLOCK)

    await insert_onchain_price(
        conn, token_id=token_id, oracle_id=oracle_id, price="9.0", block=_BLOCK + 1, time_offset="-1 hours"
    )
    second = await cached()
    assert second["block_number"] == _BLOCK + 1
    assert second["block_timestamp"] == await history_timestamp(_BLOCK + 1)
    assert second["block_timestamp"] > first["block_timestamp"]

    await insert_onchain_price(
        conn, token_id=token_id, oracle_id=oracle_id, price="1.0", block=_BLOCK - 1, time_offset="-3 hours"
    )
    assert dict(await cached()) == dict(second)
