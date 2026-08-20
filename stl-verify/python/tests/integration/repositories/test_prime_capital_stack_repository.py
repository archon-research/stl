"""The reference capital series read against a real TimescaleDB.

The risk this covers is entirely in the SQL — ``time_bucket_gapfill`` with
``locf``, and the ``DISTINCT ON`` that picks a corrected row over the original
it supersedes. None of that is exercised by mocking the repository.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.prime_capital_stack_repository import PrimeCapitalStackRepository
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import insert_allocation_position

_PROXY = bytes.fromhex("1601843c5e9bc251a3272907010afa41fa18347e")
_WINDOW_START = datetime(2026, 8, 19, 0, 0, tzinfo=timezone.utc)
_FIRST_OBSERVATION = _WINDOW_START + timedelta(hours=2)


async def _insert_snapshot(
    conn: asyncpg.Connection,
    prime_id: int,
    synced_at: datetime,
    *,
    total_rc: str,
    exposure: str,
    build_id: int = 1,
) -> None:
    await conn.execute(
        """
        INSERT INTO prime_capital_stack (
            prime_id, synced_at, exposure_usd, required_risk_capital_usd, total_risk_capital_usd,
            junior_risk_capital_usd, senior_risk_capital_usd,
            internal_junior_risk_capital_usd, external_junior_risk_capital_usd,
            tokenized_junior_risk_capital_usd,
            internal_senior_risk_capital_usd, external_senior_risk_capital_usd,
            encumbrance_ratio, exposure_share, epi_utilization, spj_utilization, source, build_id
        ) VALUES ($1, $2, $3, '1', $4, '1', '0', '1', '0', '0', '0', '0', '0.37', '0.008', '0', '0',
                  'skyeco:star-monitoring:risk-capital', $5)
        """,
        prime_id,
        synced_at,
        Decimal(exposure),
        Decimal(total_rc),
        build_id,
    )


@pytest_asyncio.fixture(loop_scope="module")
async def seeded(db_url: str):
    """A prime with an allocation_position row (the proxy -> prime_id join) and no snapshots."""
    conn = await asyncpg.connect(db_url)
    try:
        prime_id = cast(int, await conn.fetchval("SELECT id FROM prime WHERE name = 'spark'"))
        token_id = cast(int, await conn.fetchval("SELECT id FROM token WHERE symbol = 'WETH' AND chain_id = 1"))
        await insert_allocation_position(
            conn,
            token_id=token_id,
            prime_id=prime_id,
            proxy_hex=_PROXY.hex(),
            balance=1,
            block=1,
            tx="11" * 32,
            direction="in",
            created_at=_FIRST_OBSERVATION,
        )
        await conn.execute("DELETE FROM prime_capital_stack WHERE prime_id = $1", prime_id)
        await conn.execute("DELETE FROM prime_reference_balance_sheet WHERE prime_id = $1", prime_id)
        yield conn, prime_id
        await conn.execute("DELETE FROM prime_capital_stack WHERE prime_id = $1", prime_id)
        await conn.execute("DELETE FROM prime_reference_balance_sheet WHERE prime_id = $1", prime_id)
        await conn.execute("DELETE FROM allocation_position WHERE proxy_address = $1", _PROXY)
    finally:
        await conn.close()


async def _buckets(async_db_url: str, *, hours: int = 6, bucket_seconds: float = 3600):
    engine = create_async_engine(async_db_url)
    try:
        repository = PrimeCapitalStackRepository(engine)
        return await repository.list_reference_capital_buckets(
            EthAddress("0x" + _PROXY.hex()),
            from_timestamp=_WINDOW_START,
            to_timestamp=_WINDOW_START + timedelta(hours=hours),
            bucket_seconds=bucket_seconds,
        )
    finally:
        await engine.dispose()


@pytest.mark.asyncio(loop_scope="module")
async def test_carries_the_last_observation_forward_into_later_buckets(seeded, async_db_url: str):
    conn, prime_id = seeded
    await _insert_snapshot(conn, prime_id, _FIRST_OBSERVATION, total_rc="48142491.08", exposure="2098090654.81")

    buckets = await _buckets(async_db_url)

    carried = [b for b in buckets if b.bucket_start > _FIRST_OBSERVATION]
    assert carried, "expected buckets after the observation"
    assert all(b.total_capital_usd == Decimal("48142491.08") for b in carried)


@pytest.mark.asyncio(loop_scope="module")
async def test_leaves_buckets_before_the_first_observation_null(seeded, async_db_url: str):
    conn, prime_id = seeded
    await _insert_snapshot(conn, prime_id, _FIRST_OBSERVATION, total_rc="48142491.08", exposure="2098090654.81")

    buckets = await _buckets(async_db_url)

    leading = [b for b in buckets if b.bucket_start < _FIRST_OBSERVATION]
    assert leading, "expected buckets before the observation"
    assert all(b.total_capital_usd is None and b.exposure_usd is None for b in leading)


@pytest.mark.asyncio(loop_scope="module")
async def test_prefers_a_correction_over_the_original_it_supersedes(seeded, async_db_url: str):
    conn, prime_id = seeded
    await _insert_snapshot(conn, prime_id, _FIRST_OBSERVATION, total_rc="1", exposure="1", build_id=1)
    await _insert_snapshot(conn, prime_id, _FIRST_OBSERVATION, total_rc="999", exposure="888", build_id=2)

    buckets = await _buckets(async_db_url)

    observed = [b for b in buckets if b.total_capital_usd is not None]
    assert observed
    assert observed[0].total_capital_usd == Decimal("999")
    assert observed[0].exposure_usd == Decimal("888")


@pytest.mark.asyncio(loop_scope="module")
async def test_returns_all_null_buckets_when_the_syncer_has_never_run(seeded, async_db_url: str):
    # An unobserved prime must read as absent, never as zero capital.
    buckets = await _buckets(async_db_url)

    assert buckets
    assert all(b.total_capital_usd is None and b.exposure_usd is None for b in buckets)


async def _insert_history(conn: asyncpg.Connection, prime_id: int, observed_at: datetime, *, treasury: str) -> None:
    await conn.execute(
        """
        INSERT INTO prime_reference_balance_sheet (
            prime_id, observed_at, treasury_balance_usd, assets_usd, allocated_assets_usd,
            idle_assets_usd, debt_usd, backstop_capital_usd, source, build_id
        ) VALUES ($1, $2, $3, '1', '1', '0', '0', '0', 'skyeco:reference', 1)
        """,
        prime_id,
        observed_at,
        Decimal(treasury),
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_serves_backfilled_history_from_before_the_syncer_first_ran(seeded, async_db_url: str):
    conn, prime_id = seeded
    await _insert_history(conn, prime_id, _WINDOW_START, treasury="111")

    buckets = await _buckets(async_db_url)

    observed = [b for b in buckets if b.total_capital_usd is not None]
    assert observed, "expected the backfilled day to populate the series"
    assert all(b.total_capital_usd == Decimal("111") for b in observed)


@pytest.mark.asyncio(loop_scope="module")
async def test_prefers_a_snapshot_over_backfilled_history_at_the_same_instant(seeded, async_db_url: str):
    # The two feeds overlap only where the syncer has taken over, and the
    # snapshot is the finer cadence, so it must win.
    conn, prime_id = seeded
    await _insert_history(conn, prime_id, _FIRST_OBSERVATION, treasury="111")
    await _insert_snapshot(conn, prime_id, _FIRST_OBSERVATION, total_rc="222", exposure="5")

    buckets = await _buckets(async_db_url)

    observed = [b for b in buckets if b.total_capital_usd is not None]
    assert observed[0].total_capital_usd == Decimal("222")


@pytest.mark.asyncio(loop_scope="module")
async def test_never_serves_backfilled_allocated_assets_as_exposure(seeded, async_db_url: str):
    # The feed's allocated_assets is a different measurement from the monitor's
    # exposure, so a history-only window must report exposure as unobserved.
    conn, prime_id = seeded
    await _insert_history(conn, prime_id, _WINDOW_START, treasury="111")

    buckets = await _buckets(async_db_url)

    assert any(b.total_capital_usd is not None for b in buckets)
    assert all(b.exposure_usd is None for b in buckets)


@pytest.mark.asyncio(loop_scope="module")
async def test_pairs_each_bucket_from_one_snapshot_row(seeded, async_db_url: str):
    conn, prime_id = seeded
    await _insert_snapshot(conn, prime_id, _FIRST_OBSERVATION, total_rc="10", exposure="20")
    await _insert_snapshot(conn, prime_id, _FIRST_OBSERVATION + timedelta(hours=1), total_rc="30", exposure="40")

    buckets = await _buckets(async_db_url)

    # Asserted by membership, not by a guarded loop: a query that dropped either
    # snapshot would satisfy a per-bucket conditional vacuously.
    pairs = {(bucket.total_capital_usd, bucket.exposure_usd) for bucket in buckets}

    assert (Decimal("10"), Decimal("20")) in pairs
    assert (Decimal("30"), Decimal("40")) in pairs
