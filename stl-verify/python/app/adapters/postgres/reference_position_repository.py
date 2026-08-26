"""Reads STL's stored reference balance-sheet positions.

The reference-capital indexer lands every position Sky's internal feed reports
for a prime in ``prime_reference_position`` each cycle. This reads the newest
cycle back, so the allocation list serves observations instead of fetching the
feed per request.

Coverage is still decided by the risk-capital table, not by this one — as it was
decided by the Star monitor's tracked set and not by this feed. A covered prime
holding nothing writes no position rows, which is indistinguishable here from a
prime never reported on, and the two are different answers: an empty list versus
a ``404``. Reading coverage from the table that carries a row per covered prime
per cycle keeps them apart, and keeps the allocation list and the risk-capital
card from disagreeing about whether a prime has reference data.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres._reference_rows import (
    PRIME_BY_STAR_CTE,
    RECEIPT_TOKEN_JOIN,
    optional_decimal,
    optional_text,
    reading,
    required_decimal,
    token_address_bytes,
)
from app.domain.chain_names import CHAIN_ID_TO_NAME
from app.domain.entities.reference_position import ReferencePosition, ReferencePositionSnapshot

# One statement, because coverage and content are two questions of two tables
# and splitting them would let a cycle land between the answers. The coverage row
# drives the join, so an uncovered prime yields no rows at all while a covered
# prime with no positions yields one null-padded row — the distinction the
# service turns into `404` versus an empty list.
_POSITIONS_SQL = text(
    "WITH"
    + PRIME_BY_STAR_CTE
    + """,
    covered AS (
        SELECT pcs.synced_at
        FROM prime_capital_stack pcs
        WHERE pcs.prime_id = (SELECT id FROM target)
        ORDER BY pcs.synced_at DESC, pcs.processing_version DESC
        LIMIT 1
    ),
    cycle AS (
        SELECT MAX(p.synced_at) AS synced_at
        FROM prime_reference_position p
        WHERE p.prime_id = (SELECT id FROM target)
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
        COALESCE(r.synced_at, covered.synced_at) AS synced_at,
        r.network,
        r.chain_id,
        r.protocol_name,
        r.token_symbol,
        r.token_name,
        r.token_address,
        r.assets_usd,
        r.allocated_assets_usd,
        r.idle_assets_usd,
        rt.id AS receipt_token_id
    FROM covered
    LEFT JOIN latest r ON TRUE
    """
    + RECEIPT_TOKEN_JOIN
    + """
    ORDER BY r.assets_usd DESC NULLS LAST
    """
)


class ReferencePositionRepository:
    """Reads the latest observed balance sheet for a prime."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def get_positions(self, star: str) -> ReferencePositionSnapshot | None:
        """Return ``star``'s newest observed positions, or ``None`` if it has none."""
        async with reading(self._engine, what=f"reading the reference positions for '{star}'") as conn:
            rows = (await conn.execute(_POSITIONS_SQL, {"star": star})).fetchall()

        if not rows:
            return None
        return ReferencePositionSnapshot(
            synced_at=rows[0].synced_at,
            # A single null-padded row is the coverage row alone: the prime is
            # covered and upstream reported it holding nothing.
            positions=tuple(_position(row) for row in rows if row.token_address is not None),
        )


def _position(row) -> ReferencePosition:
    chain_id: int | None = row.chain_id
    return ReferencePosition(
        protocol_name=row.protocol_name,
        network=row.network,
        symbol=row.token_symbol,
        name=optional_text(row.token_name),
        token_address=row.token_address,
        assets_usd=required_decimal(row.assets_usd, "assets_usd"),
        allocated_assets_usd=optional_decimal(row.allocated_assets_usd, "allocated_assets_usd"),
        idle_assets_usd=optional_decimal(row.idle_assets_usd, "idle_assets_usd"),
        receipt_token_id=row.receipt_token_id,
        chain_id=chain_id,
        chain=CHAIN_ID_TO_NAME.get(chain_id) if chain_id is not None else None,
    )
