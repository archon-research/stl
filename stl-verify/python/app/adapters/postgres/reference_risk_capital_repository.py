"""Reads STL's stored Star-monitor risk-capital snapshots.

The reference-capital indexer lands the monitor's per-prime totals in
``prime_capital_stack`` and the breakdown behind them in
``prime_capital_stack_allocation`` every cycle, both under one ``synced_at``.
This reads the newest cycle of the pair back, so the API serves the same
observations ``/total-capital`` already does instead of fetching the monitor
per request.
"""

from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres._reading import reading
from app.adapters.postgres._reference_rows import (
    PRIME_BY_STAR_CTE,
    optional_decimal,
    receipt_token_join,
    required_decimal,
    text_or_empty,
    token_address_bytes,
)
from app.domain.chain_names import chain_name_for
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
    """
)

_TOTALS_SQL = text(
    PRIME_BY_STAR_CTE
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
      -- A cycle written before prime_capital_stack_allocation existed (every
      -- cycle from 2026-08-19 to 2026-08-26) has totals with no breakdown, and
      -- a prime the monitor has since stopped covering keeps that as its
      -- newest row forever. Skipping past it to an earlier complete cycle, or
      -- to none, is what turns that into a 404 -- the endpoint's documented
      -- indexed fallback -- instead of a permanent 500.
      AND (
        pcs.exposure_usd = 0
        OR EXISTS (
            SELECT 1 FROM prime_capital_stack_allocation a
            WHERE a.prime_id = pcs.prime_id AND a.synced_at = pcs.synced_at
        )
      )
    ORDER BY pcs.synced_at DESC, pcs.processing_version DESC
    LIMIT 1
    """
)

# Pinned to the totals row's own cycle rather than re-deriving "latest": a cycle
# landing between the two statements would otherwise pair one instant's totals
# with another's breakdown. Sharing a connection does not do this for us — each
# statement gets its own READ COMMITTED snapshot — so the instant is bound
# explicitly.
_ALLOCATIONS_SQL = text(
    PRIME_BY_STAR_CTE
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
    SELECT
        r.network,
        r.chain_id,
        r.protocol_name,
        r.symbol,
        r.name,
        r.token_address,
        r.loan_token_address,
        r.loan_token_symbol,
        r.exposure_usd,
        r.required_risk_capital_usd,
        r.crr,
        rt.id AS receipt_token_id
    FROM latest r
    """
    + receipt_token_join("r")
    + """
    ORDER BY r.exposure_usd DESC, r.network, r.token_address
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
        reported on.
        """
        async with reading(self._engine, what=f"reading the reference risk-capital snapshot for '{star}'") as conn:
            totals = (await conn.execute(_TOTALS_SQL, {"star": star})).fetchone()
            if totals is None:
                return None
            rows = (await conn.execute(_ALLOCATIONS_SQL, {"star": star, "synced_at": totals.synced_at})).fetchall()

        return _snapshot(star, totals, rows)


def _snapshot(star: str, totals, rows) -> ReferencePrimeRiskCapital:
    exposure = required_decimal(totals.exposure_usd, "exposure_usd")
    _require_breakdown(star, totals.synced_at, exposure, rows)

    return ReferencePrimeRiskCapital(
        star=star,
        synced_at=totals.synced_at,
        exposure_usd=exposure,
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


def _require_breakdown(star: str, synced_at, exposure: Decimal, rows) -> None:
    """Defend the invariant ``_TOTALS_SQL``'s WHERE already enforces.

    A cycle lands its totals and its breakdown together in one transaction, and
    the totals query above only selects a ``synced_at`` that already has a
    matching allocation row or reports zero exposure — so this should be
    unreachable. It stays a hard failure rather than a silent pass in case that
    guard is ever weakened: serving zero rows against real exposure publishes
    "this prime holds nothing", which reads like a real answer rather than a bug.
    """
    if rows or exposure == 0:
        return
    raise ValueError(
        f"Reference cycle {synced_at.isoformat()} for '{star}' reports exposure {exposure} "
        "but landed no per-allocation rows; the cycle is incomplete"
    )


def _allocation(row) -> ReferenceAllocation:
    return ReferenceAllocation(
        protocol_name=row.protocol_name,
        network=row.network,
        symbol=row.symbol,
        name=text_or_empty(row.name),
        token_address=row.token_address,
        loan_token_address=text_or_empty(row.loan_token_address),
        loan_token_symbol=text_or_empty(row.loan_token_symbol),
        exposure_usd=required_decimal(row.exposure_usd, "exposure_usd"),
        required_risk_capital_usd=required_decimal(row.required_risk_capital_usd, "required_risk_capital_usd"),
        crr_pct=required_decimal(row.crr, "crr") * _FRACTION_TO_PCT,
        receipt_token_id=row.receipt_token_id,
        chain_id=row.chain_id,
        chain=chain_name_for(row.chain_id),
    )
