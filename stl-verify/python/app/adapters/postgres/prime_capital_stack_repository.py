"""Reads STL's stored reference capital series.

Two feeds back one series. The capital-stack syncer appends forward from its
first run; the balance-sheet backfill holds the year before it, which is the
only source of reference history since the Star monitor publishes none. They
are one provenance to consumers, so they are read as one series here.
"""

import asyncio
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres._time_window import clamp_limit, required_time_window_clause
from app.domain.entities.allocation import EthAddress
from app.domain.entities.reference_risk_capital import ReferenceCapitalBucket

logger = logging.getLogger(__name__)

_MAX_BUCKETS = 500

# One row per (prime, cycle): the highest processing_version wins, which is the
# correction ordering ADR-0002 mandates. Selecting without it would average an
# original against its own reprocessed correction.
_REFERENCE_CAPITAL_BUCKETS_SQL = text(
    """
    WITH target AS (
        SELECT prime_id
        FROM allocation_position
        WHERE proxy_address = decode(:address_hex, 'hex')
        LIMIT 1
    ), snapshots AS (
        SELECT DISTINCT ON (pcs.synced_at)
            pcs.synced_at AS observed_at,
            pcs.total_risk_capital_usd,
            pcs.exposure_usd
        FROM prime_capital_stack pcs
        WHERE pcs.prime_id = (SELECT prime_id FROM target)
        """
    + required_time_window_clause("pcs.synced_at")
    + """
        ORDER BY pcs.synced_at, pcs.processing_version DESC
    ), history AS (
        SELECT DISTINCT ON (pbs.observed_at)
            pbs.observed_at,
            pbs.treasury_balance_usd AS total_risk_capital_usd,
            -- Exposure is deliberately absent. This feed's allocated_assets is a
            -- different measurement from the monitor's total_exposure (+32% for
            -- spark at the same instant), so splicing it would step the series.
            NULL::NUMERIC AS exposure_usd
        FROM prime_reference_balance_sheet pbs
        WHERE pbs.prime_id = (SELECT prime_id FROM target)
        """
    + required_time_window_clause("pbs.observed_at")
    + """
        ORDER BY pbs.observed_at, pbs.processing_version DESC
    ), corrected AS (
        -- One series from two feeds. `last()` has no defined order among rows
        -- sharing a timestamp, so precedence is explicit rather than left to
        -- chance: a backfilled day and a snapshot can land on the same instant
        -- when a cycle runs at midnight, and the snapshot is the finer cadence.
        SELECT DISTINCT ON (merged.observed_at)
            merged.observed_at,
            merged.total_risk_capital_usd,
            merged.exposure_usd
        FROM (
            SELECT observed_at, total_risk_capital_usd, exposure_usd, 0 AS precedence FROM snapshots
            UNION ALL
            SELECT observed_at, total_risk_capital_usd, exposure_usd, 1 AS precedence FROM history
        ) merged
        ORDER BY merged.observed_at, merged.precedence
    )
    SELECT
        time_bucket_gapfill(
            make_interval(secs => :bucket_seconds),
            corrected.observed_at,
            CAST(:from_timestamp AS TIMESTAMPTZ),
            CAST(:to_timestamp AS TIMESTAMPTZ)
        ) AS bucket_start,
        locf(last(corrected.total_risk_capital_usd, corrected.observed_at)) AS total_capital_usd,
        locf(last(corrected.exposure_usd, corrected.observed_at)) AS exposure_usd
    FROM corrected
    GROUP BY bucket_start
    ORDER BY bucket_start DESC
    LIMIT :limit
    """
)


class PrimeCapitalStackRepository:
    """Reads the bucketed reference capital series."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def list_reference_capital_buckets(
        self,
        prime_address: EthAddress,
        *,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[ReferenceCapitalBucket]:
        """Return the last upstream observation per time bucket (LOCF gap-filled)."""
        params = {
            "address_hex": prime_address.hex,
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "bucket_seconds": bucket_seconds,
            "limit": clamp_limit(limit, _MAX_BUCKETS),
        }

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(_REFERENCE_CAPITAL_BUCKETS_SQL, params)
                rows = result.fetchall()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Failed to fetch reference capital buckets from database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "prime_address": str(prime_address),
                },
                exc_info=True,
            )
            raise ValueError(
                f"Database query failed while fetching reference capital buckets for prime {prime_address}: {exc}"
            ) from exc

        return [
            ReferenceCapitalBucket(
                bucket_start=row.bucket_start,
                total_capital_usd=_optional_decimal(row.total_capital_usd, "total_capital_usd"),
                exposure_usd=_optional_decimal(row.exposure_usd, "exposure_usd"),
            )
            for row in rows
        ]


def _optional_decimal(value: Any, field_name: str) -> Decimal | None:
    """Convert a gap-filled figure, keeping "not yet observed" distinct from zero."""
    if value is None:
        return None
    try:
        return Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"Non-numeric {field_name} in the reference capital series: {value!r}") from exc
