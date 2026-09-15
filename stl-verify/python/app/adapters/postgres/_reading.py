"""Lending a connection for a read, naming what failed.

Every repository here wraps its reads the same way: open a connection, run the
statement, and translate a driver failure into a ``ValueError`` naming what was
being read. This is that pattern as one helper, plus the bounded
default-frequency read built on it. The reference readers use it; the older
adapters still hand-roll it, and should adopt this rather than grow a copy.
"""

import logging
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from app.domain.time_series import MAX_POINTS, TimeWindow, enforce_max_points

logger = logging.getLogger(__name__)

# Prefixed so ``read_bounded_series`` can wrap a caller's series without shadowing
# one of its own columns or bind parameters.
_COUNT_COLUMN = "bounded_point_count"
_LIMIT_PARAM = "bounded_row_limit"


def _read_failure(exc: Exception, what: str) -> ValueError:
    """Log a driver failure and name what was being read when it happened."""
    logger.error(
        "Failed to read from database",
        extra={"error_type": type(exc).__name__, "error_message": str(exc), "reading": what},
        exc_info=True,
    )
    return ValueError(f"Database query failed while {what}: {exc}")


@asynccontextmanager
async def reading(engine: AsyncEngine, *, what: str) -> AsyncIterator[AsyncConnection]:
    """Lend one connection for the whole of ``what``, naming it on failure.

    One connection per read, so a read holds one pool slot rather than queueing
    twice. It buys no cross-statement consistency — under ``READ COMMITTED`` each
    statement gets its own snapshot — so a read whose answer spans two statements
    is written as one statement instead; see ``read_bounded_series``.

    **Map rows outside the block.** Anything raised inside is reported as a
    database failure, so row-mapping code belongs after it, where its own error
    survives unrelabelled.
    """
    try:
        async with engine.connect() as conn:
            yield conn
    except Exception as exc:
        raise _read_failure(exc, what) from exc


async def read_bounded_series(
    engine: AsyncEngine,
    *,
    what: str,
    series_sql: str,
    params: Mapping[str, Any],
    query: TimeWindow,
    max_points: int = MAX_POINTS,
) -> Sequence[Row[Any]]:
    """Read a default-frequency series, rejecting it if it is too large to serve.

    ``series_sql`` is a ``SELECT`` over the latest-version rows the response would
    carry, usable as a subquery and carrying an ``observed_at``. The ceiling is
    counted over exactly those rows, in the same statement: a window ``count(*)``
    is evaluated before ``LIMIT``, so the total is the whole series' while at most
    ``max_points + 1`` rows are fetched — one statement, one snapshot, no isolation
    level to raise and no second SQL string to keep in step.

    Raises ``MaxPointsExceededError`` — outside the block, so the rejection reaches
    the caller as itself rather than as a database failure.
    """
    bounded_sql = (
        f"SELECT count(*) OVER () AS {_COUNT_COLUMN}, bounded.* "
        f"FROM ({series_sql}) AS bounded ORDER BY observed_at LIMIT :{_LIMIT_PARAM}"
    )
    async with reading(engine, what=what) as conn:
        result = await conn.execute(text(bounded_sql), {**params, _LIMIT_PARAM: max_points + 1})
        rows = result.all()
    point_count = rows[0]._mapping[_COUNT_COLUMN] if rows else 0
    enforce_max_points(point_count, query=query, max_points=max_points)
    return rows
