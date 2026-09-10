"""Lending a connection for a read, naming what failed.

Every repository here wraps its reads the same way: open a connection, run the
statement, and translate a driver failure into a ``ValueError`` naming what was
being read. This is that pattern as one helper, in two flavours — one statement
at a time, or several pinned to one snapshot — plus the bounded default-frequency
read built on the second. The reference readers use it; the older adapters still
hand-roll it, and should adopt this rather than grow a copy.
"""

import logging
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from app.domain.time_series import MAX_POINTS, TimeWindow, enforce_max_points

logger = logging.getLogger(__name__)


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

    One connection per read, so a snapshot spanning two statements holds one
    pool slot rather than queueing twice. It buys no cross-statement
    consistency — under ``READ COMMITTED`` each statement gets its own snapshot,
    so a read that must pin two statements to one instant uses
    ``snapshot_reading`` or an explicit bind, not a shared connection.

    **Map rows outside the block.** Anything raised inside is reported as a
    database failure, so row-mapping code belongs after it, where its own error
    survives unrelabelled.
    """
    try:
        async with engine.connect() as conn:
            yield conn
    except Exception as exc:
        raise _read_failure(exc, what) from exc


@asynccontextmanager
async def snapshot_reading(engine: AsyncEngine, *, what: str) -> AsyncIterator[AsyncConnection]:
    """Lend one connection whose statements all see one snapshot.

    ``REPEATABLE READ`` for the whole block, so a count and the rows it was
    counting cannot disagree: under the default ``READ COMMITTED`` a correction
    or a backfill landing between the two statements would let a response be
    admitted on one row count and returned with another.

    **Decide inside, raise outside.** Anything raised in the block is reported as
    a database failure, and a multi-statement block is exactly where logic between
    the statements is tempting — so a rejection or a row mapping belongs after it,
    where its own error survives unrelabelled.
    """
    try:
        async with engine.connect() as conn:
            await conn.execution_options(isolation_level="REPEATABLE READ")
            async with conn.begin():
                yield conn
    except Exception as exc:
        raise _read_failure(exc, what) from exc


async def read_bounded_series(
    engine: AsyncEngine,
    *,
    what: str,
    count_sql: str,
    rows_sql: str,
    params: Mapping[str, Any],
    query: TimeWindow,
    max_points: int = MAX_POINTS,
) -> Sequence[Row[Any]]:
    """Read a default-frequency series, rejecting it if it is too large to serve.

    ``count_sql`` and ``rows_sql`` are the caller's two views of one query — the
    count must be taken over the same latest-version rows ``rows_sql`` returns, or
    the ceiling guards a different series than the one served. Both run in one
    snapshot, and the rows are never fetched for a request that is rejected.

    Raises ``MaxPointsExceededError`` — outside the block, so the rejection reaches
    the caller as itself rather than as a database failure.
    """
    async with snapshot_reading(engine, what=what) as conn:
        point_count = (await conn.execute(text(count_sql), params)).scalar_one()
        within_limit = point_count <= max_points
        rows = (await conn.execute(text(rows_sql), params)).all() if within_limit else []
    enforce_max_points(point_count, query=query, max_points=max_points)
    return rows
