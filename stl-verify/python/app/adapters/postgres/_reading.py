"""Lending a connection for a read, naming what failed.

Every repository here wraps its reads the same way: open a connection, run the
statement, and translate a driver failure into a ``ValueError`` naming what was
being read. This is that pattern as one helper. The reference readers use it;
the older adapters still hand-roll it, and should adopt this rather than grow a
copy.
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

logger = logging.getLogger(__name__)


@asynccontextmanager
async def reading(engine: AsyncEngine, *, what: str) -> AsyncIterator[AsyncConnection]:
    """Lend one connection for the whole of ``what``, naming it on failure.

    One connection per read, so a snapshot spanning two statements holds one
    pool slot rather than queueing twice. It buys no cross-statement
    consistency — under ``READ COMMITTED`` each statement gets its own snapshot,
    so a read that must pin two statements to one instant does it with an
    explicit bind, not by sharing a connection.

    **Map rows outside the block.** Anything raised inside is reported as a
    database failure, so row-mapping code belongs after it, where its own error
    survives unrelabelled.
    """
    try:
        async with engine.connect() as conn:
            yield conn
    except Exception as exc:
        logger.error(
            "Failed to read from database",
            extra={"error_type": type(exc).__name__, "error_message": str(exc), "reading": what},
            exc_info=True,
        )
        raise ValueError(f"Database query failed while {what}: {exc}") from exc
