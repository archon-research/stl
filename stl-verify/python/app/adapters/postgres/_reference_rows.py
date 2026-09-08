"""Shared SQL and row-reading for the per-row reference snapshot tables.

``prime_capital_stack_allocation`` and ``prime_reference_position`` are written
by one indexer cycle and read the same way: pick the newest ``synced_at``, take
the highest ``processing_version`` per row identity, and resolve upstream's
``(chain_id, token_address)`` against STL's receipt-token registry. Both tables
declare identical identity columns, so a copy of these fragments would drift
silently rather than loudly.

Bind parameters are fixed by name: ``star`` (the prime's name, matched
case-insensitively).
"""

from decimal import Decimal, InvalidOperation
from typing import Any

# The prime these snapshots belong to, by natural key. Matched case-insensitively
# because upstream's star names reach the API through a registry that does not
# promise a case, while `prime.name` does.
PRIME_BY_STAR_CTE = """
    WITH target AS (
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


def receipt_token_join(alias: str) -> str:
    """LEFT JOIN resolving ``alias``.token_bytes against the registry, keyed on chain.

    Replaces a point lookup issued per row from the service, whose fan-out had
    to be semaphore-capped to stay inside the connection pool (#678, #753).
    ``receipt_token`` is unique on (chain_id, receipt_token_address), so this
    cannot multiply a row and inflate a total.
    """
    return f"""
    LEFT JOIN receipt_token rt
           ON rt.chain_id = {alias}.chain_id
          AND rt.receipt_token_address = {alias}.token_bytes
"""


def receipt_token_underlying_join() -> str:
    """LEFT JOIN resolving the receipt token's underlying, following ``receipt_token_join``.

    Must appear after ``receipt_token_join`` in the same statement, whose ``rt``
    alias this reads. ``receipt_token.underlying_token_id`` is ``NOT NULL``, so
    a matched ``rt`` always resolves a ``ut`` row; the LEFT JOIN exists only so
    an unmatched ``rt`` (``rt.underlying_token_id`` NULL) carries an all-NULL
    ``ut`` instead of dropping the row.
    """
    return """
    LEFT JOIN token ut ON ut.id = rt.underlying_token_id
"""


def required_decimal(value: Any, field_name: str) -> Decimal:
    """Read a NOT NULL numeric column, rejecting one the driver could not decode."""
    figure = optional_decimal(value, field_name)
    if figure is None:
        raise ValueError(f"Reference snapshot row has no {field_name}, which the column forbids")
    return figure


def optional_decimal(value: Any, field_name: str) -> Decimal | None:
    """Read a nullable numeric column, keeping "not reported" distinct from zero.

    NUMERIC admits NaN and the column carries no CHECK against it, and
    ``Decimal`` takes it without complaint. Left through, it poisons every total
    it reaches and makes sorting the rows raise, so it is rejected here where the
    column can still be named rather than downstream where it cannot.
    """
    if value is None:
        return None
    try:
        figure = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"Non-numeric {field_name} in a reference snapshot row: {value!r}") from exc

    if not figure.is_finite():
        raise ValueError(f"Non-finite {field_name} in a reference snapshot row: {value!r}")
    return figure


def text_or_empty(value: Any) -> str:
    """Read a nullable identity column as the empty string the entities carry.

    The entities type these fields ``str``, where the columns allow NULL for a
    label upstream omitted. Empty and absent mean the same thing for a display
    label, so they are folded rather than widened.
    """
    return "" if value is None else str(value)
