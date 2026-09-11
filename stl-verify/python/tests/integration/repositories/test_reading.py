"""The shared read helpers against a real database.

A probe table rather than a dataset's own: the dataset SQL lands in later
tickets, and what is under test is the reading contract they will all sit on —
one snapshot behind a count and its rows, latest-version-only, and a rejection
that never turns into a truncated answer.
"""

from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres._reading import read_bounded_series, snapshot_reading
from app.domain.time_series import MaxPointsExceededError, TimeWindow

_NOW = datetime(2026, 3, 5, 13, 0, tzinfo=UTC)

# Two observations an hour apart, then a two-hour gap, then a third.
_OBSERVED_AT = [
    datetime(2026, 3, 5, 9, 0, tzinfo=UTC),
    datetime(2026, 3, 5, 10, 0, tzinfo=UTC),
    datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
]

_CREATE = """
CREATE TABLE IF NOT EXISTS series_probe (
    observed_at        TIMESTAMPTZ NOT NULL,
    processing_version INT NOT NULL,
    value              NUMERIC NOT NULL
)
"""

# The latest processing version of each observation, and nothing else, reaches
# either statement — so the count guards the series the rows carry.
_LATEST_VERSIONS = """
    SELECT DISTINCT ON (observed_at) observed_at, value
      FROM series_probe
     WHERE observed_at >= CAST(:from_timestamp AS TIMESTAMPTZ)
       AND observed_at <= CAST(:to_timestamp AS TIMESTAMPTZ)
     ORDER BY observed_at, processing_version DESC
"""
_COUNT_SQL = f"SELECT count(*) FROM ({_LATEST_VERSIONS}) AS latest"
_ROWS_SQL = f"SELECT observed_at, value FROM ({_LATEST_VERSIONS}) AS latest ORDER BY observed_at"


def _window(from_timestamp: datetime, to_timestamp: datetime = _NOW) -> TimeWindow:
    return TimeWindow(from_timestamp=from_timestamp, to_timestamp=to_timestamp)


def _params(window: TimeWindow) -> dict:
    return {"from_timestamp": window.from_timestamp, "to_timestamp": window.to_timestamp}


@pytest_asyncio.fixture(loop_scope="function")
async def engine(async_db_url):
    engine = create_async_engine(async_db_url)
    async with engine.begin() as conn:
        await conn.execute(text(_CREATE))
        await conn.execute(text("TRUNCATE series_probe"))
        for index, observed_at in enumerate(_OBSERVED_AT):
            await conn.execute(
                text("INSERT INTO series_probe (observed_at, processing_version, value) VALUES (:at, 0, :value)"),
                {"at": observed_at, "value": index},
            )
    try:
        yield engine
    finally:
        await engine.dispose()


async def _read(engine, window: TimeWindow, **kwargs):
    return await read_bounded_series(
        engine,
        what="reading the probe series",
        count_sql=_COUNT_SQL,
        rows_sql=_ROWS_SQL,
        params=_params(window),
        query=window,
        **kwargs,
    )


async def test_default_frequency_read_returns_the_observations_with_the_gap_intact(engine) -> None:
    rows = await _read(engine, _window(datetime(2026, 3, 5, 8, 0, tzinfo=UTC)))

    assert [row.observed_at for row in rows] == _OBSERVED_AT


async def test_default_frequency_read_includes_an_observation_on_the_exact_bound(engine) -> None:
    window = _window(datetime(2026, 3, 5, 10, 0, tzinfo=UTC), datetime(2026, 3, 5, 12, 0, tzinfo=UTC))

    rows = await _read(engine, window)

    assert [row.observed_at for row in rows] == _OBSERVED_AT[1:]


async def test_a_known_series_with_nothing_in_range_reads_as_empty(engine) -> None:
    rows = await _read(engine, _window(datetime(2020, 1, 1, tzinfo=UTC), datetime(2020, 1, 2, tzinfo=UTC)))

    assert rows == []


async def test_only_the_latest_processing_version_of_an_observation_is_served(engine) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO series_probe (observed_at, processing_version, value) VALUES (:at, 1, 99)"),
            {"at": _OBSERVED_AT[0]},
        )

    rows = await _read(engine, _window(datetime(2026, 3, 5, 8, 0, tzinfo=UTC)))

    assert [row.observed_at for row in rows] == _OBSERVED_AT
    assert rows[0].value == 99


async def test_an_oversized_read_is_rejected_with_the_count_it_would_have_returned(engine) -> None:
    window = _window(datetime(2026, 3, 5, 8, 0, tzinfo=UTC))

    with pytest.raises(MaxPointsExceededError) as exc_info:
        await _read(engine, window, max_points=2)

    assert exc_info.value.point_count == 3
    assert exc_info.value.max_points == 2


async def test_a_rejected_read_reaches_the_caller_as_a_rejection_not_a_database_failure(engine) -> None:
    window = _window(datetime(2026, 3, 5, 8, 0, tzinfo=UTC))

    with pytest.raises(MaxPointsExceededError) as exc_info:
        await _read(engine, window, max_points=1)

    assert "Database query failed" not in str(exc_info.value)


async def test_a_write_landing_mid_read_is_invisible_to_the_rest_of_the_snapshot(engine, async_db_url) -> None:
    writer = create_async_engine(async_db_url)
    window = _window(datetime(2026, 3, 5, 8, 0, tzinfo=UTC))
    try:
        async with snapshot_reading(engine, what="reading the probe series") as conn:
            before = (await conn.execute(text(_COUNT_SQL), _params(window))).scalar_one()
            async with writer.begin() as write_conn:
                await write_conn.execute(
                    text("INSERT INTO series_probe (observed_at, processing_version, value) VALUES (:at, 0, 7)"),
                    {"at": _OBSERVED_AT[-1] + timedelta(minutes=1)},
                )
            after = (await conn.execute(text(_COUNT_SQL), _params(window))).scalar_one()
    finally:
        await writer.dispose()

    assert before == after
