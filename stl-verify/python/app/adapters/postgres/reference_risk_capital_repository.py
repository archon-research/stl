"""Reads STL's stored Star-monitor risk-capital snapshots.

The reference-capital indexer lands the monitor's per-prime totals in
``prime_capital_stack`` and the breakdown behind them in
``prime_capital_stack_allocation`` every cycle, both under one ``synced_at``.
This reads the newest cycle of the pair back, so the API serves the same
observations ``/total-capital`` already does instead of fetching the monitor
per request.
"""

from collections.abc import Sequence
from decimal import Decimal

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
from app.domain.entities.reference_risk_capital import (
    ReferenceAllocation,
    ReferencePrimeRiskCapital,
)

# The stored column is upstream's own 0-1 fraction (crr == rrc / exposure), and
# every consumer in this codebase reads a 0-100 percentage, so the rescale
# happens once, at this boundary. Documented on the column's COMMENT too.
_FRACTION_TO_PCT = Decimal("100")

# A prime the indexer has never landed a cycle for has no reference figures.
# Driven off `prime` so each candidate costs an index probe rather than a
# DISTINCT over every cycle the table holds.
_COVERED_STARS_SQL = text(
    """
    SELECT lower(p.name) AS star
    FROM prime p
    WHERE EXISTS (
        SELECT 1 FROM prime_capital_stack pcs WHERE pcs.prime_id = p.id
    )
    ORDER BY star
    """
)

_TOTALS_SQL = text(
    "WITH"
    + PRIME_BY_STAR_CTE
    + """
    SELECT
        pcs.synced_at,
        pcs.exposure_usd,
        pcs.required_risk_capital_usd,
        pcs.total_risk_capital_usd,
        pcs.encumbrance_ratio,
        pcs.exposure_share,
        pcs.junior_risk_capital_usd,
        pcs.senior_risk_capital_usd,
        pcs.internal_junior_risk_capital_usd,
        pcs.external_junior_risk_capital_usd,
        pcs.tokenized_junior_risk_capital_usd,
        pcs.internal_senior_risk_capital_usd,
        pcs.external_senior_risk_capital_usd,
        pcs.epi_utilization,
        pcs.spj_utilization
    FROM prime_capital_stack pcs
    WHERE pcs.prime_id = (SELECT id FROM target)
    ORDER BY pcs.synced_at DESC, pcs.processing_version DESC
    LIMIT 1
    """
)

# Pinned to the totals row's own cycle rather than re-deriving "latest": a cycle
# landing between the two statements would otherwise pair one instant's totals
# with another's breakdown.
_ALLOCATIONS_SQL = text(
    "WITH"
    + PRIME_BY_STAR_CTE
    + """,
    latest AS (
        SELECT DISTINCT ON (a.network, a.token_address)
            a.network,
            a.chain_id,
            a.protocol_name,
            a.symbol,
            a.name,
            a.token_address,
            a.loan_token_address,
            a.loan_token_symbol,
            a.exposure_usd,
            a.required_risk_capital_usd,
            a.crr,
        """
    + token_address_bytes("a.token_address")
    + """ AS token_bytes
        FROM prime_capital_stack_allocation a
        WHERE a.prime_id = (SELECT id FROM target)
          AND a.synced_at = CAST(:synced_at AS TIMESTAMPTZ)
        ORDER BY a.network, a.token_address, a.processing_version DESC
    )
    SELECT r.*, rt.id AS receipt_token_id
    FROM latest r
    """
    + RECEIPT_TOKEN_JOIN
    + """
    ORDER BY r.exposure_usd DESC
    """
)


class ReferenceRiskCapitalRepository:
    """Reads the latest observed risk-capital snapshot for a prime."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def covered_stars(self) -> frozenset[str]:
        """Every star reference figures have been observed for, lowercased."""
        async with reading(self._engine, what="listing reference-covered primes") as conn:
            rows = (await conn.execute(_COVERED_STARS_SQL)).fetchall()
        return frozenset(row.star for row in rows)

    async def get_prime(self, star: str) -> ReferencePrimeRiskCapital | None:
        """Return ``star``'s newest observed snapshot, or ``None`` if it has none.

        Coverage is the existence of a totals row: the indexer writes one per
        prime the monitor covers, per cycle, so a prime with none has never been
        reported on — the answer the monitor's list route used to give.
        """
        async with reading(self._engine, what=f"reading the reference risk-capital snapshot for '{star}'") as conn:
            totals = (await conn.execute(_TOTALS_SQL, {"star": star})).fetchone()
            if totals is None:
                return None
            rows = (await conn.execute(_ALLOCATIONS_SQL, {"star": star, "synced_at": totals.synced_at})).fetchall()

        return _snapshot(star, totals, rows)


def _snapshot(star: str, totals, rows: Sequence) -> ReferencePrimeRiskCapital:
    return ReferencePrimeRiskCapital(
        star=star,
        synced_at=totals.synced_at,
        exposure_usd=required_decimal(totals.exposure_usd, "exposure_usd"),
        required_risk_capital_usd=required_decimal(totals.required_risk_capital_usd, "required_risk_capital_usd"),
        total_risk_capital_usd=required_decimal(totals.total_risk_capital_usd, "total_risk_capital_usd"),
        encumbrance_ratio=optional_decimal(totals.encumbrance_ratio, "encumbrance_ratio"),
        exposure_share=required_decimal(totals.exposure_share, "exposure_share"),
        junior_risk_capital_usd=required_decimal(totals.junior_risk_capital_usd, "junior_risk_capital_usd"),
        senior_risk_capital_usd=required_decimal(totals.senior_risk_capital_usd, "senior_risk_capital_usd"),
        internal_junior_risk_capital_usd=required_decimal(
            totals.internal_junior_risk_capital_usd, "internal_junior_risk_capital_usd"
        ),
        external_junior_risk_capital_usd=required_decimal(
            totals.external_junior_risk_capital_usd, "external_junior_risk_capital_usd"
        ),
        tokenized_junior_risk_capital_usd=required_decimal(
            totals.tokenized_junior_risk_capital_usd, "tokenized_junior_risk_capital_usd"
        ),
        internal_senior_risk_capital_usd=required_decimal(
            totals.internal_senior_risk_capital_usd, "internal_senior_risk_capital_usd"
        ),
        external_senior_risk_capital_usd=required_decimal(
            totals.external_senior_risk_capital_usd, "external_senior_risk_capital_usd"
        ),
        epi_utilization=required_decimal(totals.epi_utilization, "epi_utilization"),
        spj_utilization=required_decimal(totals.spj_utilization, "spj_utilization"),
        per_allocation=tuple(_allocation(row) for row in rows),
    )


def _allocation(row) -> ReferenceAllocation:
    chain_id: int | None = row.chain_id
    return ReferenceAllocation(
        protocol_name=row.protocol_name,
        network=row.network,
        symbol=row.symbol,
        name=optional_text(row.name),
        token_address=row.token_address,
        loan_token_address=optional_text(row.loan_token_address),
        loan_token_symbol=optional_text(row.loan_token_symbol),
        exposure_usd=required_decimal(row.exposure_usd, "exposure_usd"),
        required_risk_capital_usd=required_decimal(row.required_risk_capital_usd, "required_risk_capital_usd"),
        crr_pct=required_decimal(row.crr, "crr") * _FRACTION_TO_PCT,
        receipt_token_id=row.receipt_token_id,
        chain_id=chain_id,
        chain=CHAIN_ID_TO_NAME.get(chain_id) if chain_id is not None else None,
    )
