"""Reads STL's stored reference balance-sheet positions.

The reference-capital indexer lands every position Sky's internal feed reports
for a prime in ``prime_reference_position`` each cycle. This reads the newest
cycle back, so the allocation list serves observations instead of fetching the
feed per request.

The indexer fetches positions only for the stars a cycle's risk-capital
snapshots cover, so "this prime has position rows" implies "this prime has a
coverage row". That invariant lives in the Go service and is what lets the
coverage gate below be a cheap AND rather than a decision about precedence.

The underlying-token columns' meaning is documented on
:class:`~app.domain.entities.reference_position.ReferencePosition`.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres._reading import reading
from app.adapters.postgres._reference_rows import (
    PRIME_BY_STAR_CTE,
    optional_decimal,
    receipt_token_join,
    receipt_token_underlying_join,
    required_decimal,
    text_or_empty,
    token_address_bytes,
)
from app.domain.chain_names import chain_name_for
from app.domain.entities.reference_position import ReferencePosition, ReferencePositionSnapshot

# One statement, because coverage and content are two questions of two tables and
# splitting them would let a cycle land between the answers.
#
# Both tables must have a row: the monitor must cover the prime, and this feed
# must have reported on it. Requiring only the former serves an empty balance
# sheet -- "Sky reports this prime holds nothing" -- for a prime whose positions
# have simply never landed, which is every prime in the window before the feed's
# first cycle. Requiring only the latter would let this endpoint and the
# risk-capital card disagree about whether a prime is covered at all.
_POSITIONS_SQL = text(
    PRIME_BY_STAR_CTE
    + """,
    -- Its own newest cycle, not the coverage row's: prime_capital_stack
    -- predates prime_reference_position, so every cycle written before
    -- 2026-08-26 has a coverage row and no positions. Pinning to coverage
    -- would blank the sheet for that whole window.
    cycle AS (
        SELECT p.synced_at
        FROM prime_reference_position p
        WHERE p.prime_id = (SELECT id FROM target)
        ORDER BY p.synced_at DESC, p.processing_version DESC
        LIMIT 1
    ),
    latest AS (
        SELECT DISTINCT ON (p.network, p.token_address)
            p.synced_at,
            p.network,
            p.chain_id,
            p.protocol_name,
            p.token_symbol,
            p.token_name,
            p.token_address,
            p.assets_usd,
            p.allocated_assets_usd,
            p.idle_assets_usd,
        """
    + token_address_bytes("p.token_address")
    + """ AS token_bytes
        FROM prime_reference_position p
        WHERE p.prime_id = (SELECT id FROM target)
          AND p.synced_at = (SELECT synced_at FROM cycle)
        ORDER BY p.network, p.token_address, p.processing_version DESC
    )
    SELECT
        r.synced_at,
        r.network,
        r.chain_id,
        r.protocol_name,
        r.token_symbol,
        r.token_name,
        r.token_address,
        r.assets_usd,
        r.allocated_assets_usd,
        r.idle_assets_usd,
        rt.id AS receipt_token_id,
        ut.id AS underlying_token_id,
        encode(ut.address, 'hex') AS underlying_token_address,
        ut.symbol AS underlying_symbol
    FROM latest r
    """
    + receipt_token_join("r")
    + receipt_token_underlying_join()
    + """
    WHERE EXISTS (
        SELECT 1 FROM prime_capital_stack pcs WHERE pcs.prime_id = (SELECT id FROM target)
    )
    ORDER BY r.assets_usd DESC, r.network, r.token_address
    """
)


class ReferencePositionRepository:
    """Reads the latest observed balance sheet for a prime."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def get_positions(self, star: str) -> ReferencePositionSnapshot | None:
        """Return ``star``'s newest observed positions, or ``None`` if it has none.

        ``None`` is "no reference data for this prime", which the API serves as a
        ``404``. It is never an empty snapshot: the indexer refuses to persist an
        empty balance sheet for a covered prime, so zero rows here means nothing
        has been observed rather than that the prime holds nothing.
        """
        async with reading(self._engine, what=f"reading the reference positions for '{star}'") as conn:
            rows = (await conn.execute(_POSITIONS_SQL, {"star": star})).fetchall()

        if not rows:
            return None
        return ReferencePositionSnapshot(
            synced_at=rows[0].synced_at,
            positions=tuple(_position(row) for row in rows),
        )


def _position(row) -> ReferencePosition:
    return ReferencePosition(
        protocol_name=row.protocol_name,
        network=row.network,
        symbol=row.token_symbol,
        name=text_or_empty(row.token_name),
        token_address=row.token_address,
        assets_usd=required_decimal(row.assets_usd, "assets_usd"),
        allocated_assets_usd=optional_decimal(row.allocated_assets_usd, "allocated_assets_usd"),
        idle_assets_usd=optional_decimal(row.idle_assets_usd, "idle_assets_usd"),
        receipt_token_id=row.receipt_token_id,
        underlying_token_id=row.underlying_token_id,
        underlying_token_address=("0x" + row.underlying_token_address) if row.underlying_token_id is not None else None,
        underlying_symbol=text_or_empty(row.underlying_symbol),
        chain_id=row.chain_id,
        chain=chain_name_for(row.chain_id),
    )
