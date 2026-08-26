"""Shared SQL and row-reading for the per-row reference snapshot tables.

``prime_capital_stack_allocation`` and ``prime_reference_position`` are written
by one indexer cycle and read the same way: pick the newest ``synced_at``, take
the highest ``processing_version`` per row identity, and resolve upstream's
``(chain_id, token_address)`` against STL's receipt-token registry. Both tables
declare identical identity columns, so the fragments live here rather than being
copied and left to drift.

Bind parameters are fixed by name: ``star`` (the prime's name, matched
case-insensitively).
"""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

logger = logging.getLogger(__name__)

# The prime these snapshots belong to, by natural key. Matched case-insensitively
# because upstream's star names reach the API through a registry that does not
# promise a case, while `prime.name` does.
PRIME_BY_STAR_CTE = """
    target AS (
        SELECT id
        FROM prime
        WHERE lower(name) = lower(:star)
        LIMIT 1
    )
"""


def token_address_bytes(column: str) -> str:
    """``column`` as registry-comparable bytes, or NULL where it is not an address.

    Guarded by a CASE rather than a WHERE, because ``decode`` raises on
    non-hex input and SQL does not promise to evaluate a conjunction left to
    right. Upstream puts a 32-byte Uniswap V4 pool id in this field, and records
    whatever else it reports verbatim, so a value that is not an address is an
    ordinary row rather than a fault.
    """
    return f"CASE WHEN {column} ~ '^0[xX][0-9a-fA-F]{{40}}$' THEN decode(substring({column} FROM 3), 'hex') END"


# Resolves upstream's claim against STL's registry in SQL, joining the selected
# rows aliased `r`. Replaces a per-row point lookup issued from the service,
# whose fan-out had to be semaphore-capped to stay inside the connection pool
# (#678, #753).
RECEIPT_TOKEN_JOIN = """
    LEFT JOIN receipt_token rt
           ON rt.chain_id = r.chain_id
          AND rt.receipt_token_address = r.token_bytes
"""


@asynccontextmanager
async def reading(engine: AsyncEngine, *, what: str) -> AsyncIterator[AsyncConnection]:
    """Lend one connection for a reference read, naming ``what`` on failure.

    One connection for the whole read, so a snapshot spanning two statements
    cannot be pinned to a cycle by one and answered from another's pool slot.
    """
    try:
        async with engine.connect() as conn:
            yield conn
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.error(
            "Failed to read a reference snapshot from database",
            extra={"error_type": type(exc).__name__, "error_message": str(exc), "reading": what},
            exc_info=True,
        )
        raise ValueError(f"Database query failed while {what}: {exc}") from exc


def required_decimal(value: Any, field_name: str) -> Decimal:
    """Read a NOT NULL numeric column, rejecting one the driver could not decode."""
    figure = optional_decimal(value, field_name)
    if figure is None:
        raise ValueError(f"Reference snapshot row has no {field_name}, which the column forbids")
    return figure


def optional_decimal(value: Any, field_name: str) -> Decimal | None:
    """Read a nullable numeric column, keeping "not reported" distinct from zero."""
    if value is None:
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"Non-numeric {field_name} in a reference snapshot row: {value!r}") from exc


def optional_text(value: Any) -> str:
    """Read a nullable identity column as the empty string the entities carry.

    The entities predate the tables and type these fields ``str``, where the
    columns allow NULL for a label upstream omitted. Empty and absent mean the
    same thing for a display label, so they are folded rather than widened.
    """
    return "" if value is None else str(value)
