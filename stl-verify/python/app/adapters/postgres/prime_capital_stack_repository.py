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
        FROM prime_proxy
        WHERE proxy_address = decode(:address_hex, 'hex')
        LIMIT 1
    ), snapshots AS (
        SELECT DISTINCT ON (pcs.synced_at)
            pcs.synced_at AS observed_at,
            pcs.total_risk_capital_usd,
            pcs.exposure_usd,
            pcs.encumbrance_ratio,
            NULL::NUMERIC AS assets_usd,
            NULL::TIMESTAMPTZ AS assets_observed_at
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
            NULL::NUMERIC AS exposure_usd,
            -- The balance-sheet feed reports no encumbrance; it is the monitor's
            -- figure, so history leaves it unobserved rather than deriving one.
            NULL::NUMERIC AS encumbrance_ratio,
            pbs.assets_usd,
            pbs.observed_at AS assets_observed_at
        FROM prime_reference_balance_sheet pbs
        WHERE pbs.prime_id = (SELECT prime_id FROM target)
        """
    + required_time_window_clause("pbs.observed_at")
    + """
        ORDER BY pbs.observed_at, pbs.processing_version DESC
    ), pre AS (
        SELECT
            pcs.synced_at AS observed_at,
            pcs.total_risk_capital_usd,
            pcs.exposure_usd,
            pcs.encumbrance_ratio,
            NULL::NUMERIC AS assets_usd,
            0 AS precedence,
            pcs.processing_version
        FROM prime_capital_stack pcs
        WHERE pcs.prime_id = (SELECT prime_id FROM target)
          AND pcs.synced_at < CAST(:from_timestamp AS TIMESTAMPTZ)
          AND pcs.synced_at >= CAST(:from_timestamp AS TIMESTAMPTZ) - INTERVAL '90 days'
        UNION ALL
        SELECT
            pbs.observed_at,
            pbs.treasury_balance_usd AS total_risk_capital_usd,
            NULL::NUMERIC AS exposure_usd,
            NULL::NUMERIC AS encumbrance_ratio,
            pbs.assets_usd,
            1 AS precedence,
            pbs.processing_version
        FROM prime_reference_balance_sheet pbs
        WHERE pbs.prime_id = (SELECT prime_id FROM target)
          AND pbs.observed_at < CAST(:from_timestamp AS TIMESTAMPTZ)
          AND pbs.observed_at >= CAST(:from_timestamp AS TIMESTAMPTZ) - INTERVAL '90 days'
    ), ranked AS (
        -- Rank once, here. The six figures below then read this CTE without
        -- sorting it again, where each used to sort the whole of `pre` itself.
        SELECT
            observed_at, total_risk_capital_usd, exposure_usd, encumbrance_ratio, assets_usd,
            row_number() OVER (
                ORDER BY observed_at DESC, precedence, processing_version DESC
            ) AS rank
        FROM pre
    ), first_rank AS (
        SELECT
            min(rank) FILTER (WHERE total_risk_capital_usd IS NOT NULL) AS total_capital,
            min(rank) FILTER (WHERE exposure_usd IS NOT NULL) AS exposure,
            min(rank) FILTER (WHERE encumbrance_ratio IS NOT NULL) AS encumbrance,
            min(rank) FILTER (WHERE assets_usd IS NOT NULL) AS assets,
            min(rank) AS capital
        FROM ranked
    ), corrected AS (
        -- One series from two feeds. `last()` has no defined order among rows
        -- sharing a timestamp, so precedence is explicit rather than left to
        -- chance: a backfilled day and a snapshot can land on the same instant
        -- when a cycle runs at midnight, and the snapshot is the finer cadence.
        SELECT DISTINCT ON (merged.observed_at)
            merged.observed_at,
            merged.total_risk_capital_usd,
            merged.exposure_usd,
            merged.encumbrance_ratio,
            merged.assets_usd,
            merged.assets_observed_at
        FROM (
            SELECT observed_at, total_risk_capital_usd, exposure_usd, encumbrance_ratio, assets_usd,
                   assets_observed_at, 0 AS precedence FROM snapshots
            UNION ALL
            SELECT observed_at, total_risk_capital_usd, exposure_usd, encumbrance_ratio, assets_usd,
                   assets_observed_at, 1 AS precedence FROM history
        ) merged
        ORDER BY merged.observed_at, merged.precedence
    ), prior AS (
        -- The last observation before the window, per figure, fed to `locf` as
        -- its `prev`. Without it a figure whose newest row predates the window
        -- reads as never observed — and the balance sheet is daily, so from one
        -- minute past midnight its newest row already sits outside a 24h window
        -- and the collateral series would be empty for most of every day.
        --
        -- Bounded, because a figure this stale is not a current reading. Callers
        -- pair it with the observation time so age is visible rather than implied.
        -- Exactly one row of `ranked` matches each rank, so max() collapses it
        -- without imposing an order of its own — the rank carries the ordering.
        SELECT
            max(ranked.total_risk_capital_usd) FILTER (WHERE ranked.rank = first_rank.total_capital)
                AS total_capital_usd,
            max(ranked.exposure_usd) FILTER (WHERE ranked.rank = first_rank.exposure)
                AS exposure_usd,
            max(ranked.encumbrance_ratio) FILTER (WHERE ranked.rank = first_rank.encumbrance)
                AS encumbrance_ratio,
            max(ranked.assets_usd) FILTER (WHERE ranked.rank = first_rank.assets)
                AS assets_usd,
            max(ranked.observed_at) FILTER (WHERE ranked.rank = first_rank.assets)
                AS assets_observed_at,
            max(ranked.observed_at) FILTER (WHERE ranked.rank = first_rank.capital)
                AS capital_observed_at
        FROM ranked CROSS JOIN first_rank
    )
    SELECT
        time_bucket_gapfill(
            make_interval(secs => :bucket_seconds),
            corrected.observed_at,
            CAST(:from_timestamp AS TIMESTAMPTZ),
            CAST(:to_timestamp AS TIMESTAMPTZ)
        ) AS bucket_start,
        locf(
            last(corrected.total_risk_capital_usd, corrected.observed_at)
                FILTER (WHERE corrected.total_risk_capital_usd IS NOT NULL),
            (SELECT prior.total_capital_usd FROM prior),
            treat_null_as_missing => true
        ) AS total_capital_usd,
        -- FILTER, not just treat_null_as_missing: the two feeds NULL each other's
        -- columns, so without it last() returns the NULL of whichever feed wrote
        -- the bucket's newest row and locf then carries that NULL forever. The
        -- daily balance sheet is stamped at midnight and the monitor runs
        -- intraday, so that is every live bucket.
        locf(
            last(corrected.exposure_usd, corrected.observed_at)
                FILTER (WHERE corrected.exposure_usd IS NOT NULL),
            (SELECT prior.exposure_usd FROM prior),
            treat_null_as_missing => true
        ) AS exposure_usd,
        locf(
            last(corrected.encumbrance_ratio, corrected.observed_at)
                FILTER (WHERE corrected.encumbrance_ratio IS NOT NULL),
            (SELECT prior.encumbrance_ratio FROM prior),
            treat_null_as_missing => true
        ) AS encumbrance_ratio,
        locf(
            last(corrected.assets_usd, corrected.observed_at)
                FILTER (WHERE corrected.assets_usd IS NOT NULL),
            (SELECT prior.assets_usd FROM prior),
            treat_null_as_missing => true
        ) AS assets_usd,
        -- The instant the assets figure above was observed, not the bucket it
        -- was carried into. The feed is daily and the value is carried forward,
        -- so without this a figure up to a day old renders as a current one.
        locf(
            last(corrected.assets_observed_at, corrected.observed_at)
                FILTER (WHERE corrected.assets_observed_at IS NOT NULL),
            (SELECT prior.assets_observed_at FROM prior),
            treat_null_as_missing => true
        ) AS assets_observed_at,
        -- When total capital, exposure and encumbrance were last observed. One
        -- stamp for the three because they arrive on one row, so a stamp each
        -- would be three copies of the same instant. The prior seeding reaches
        -- up to 90 days back, so without this a figure that old serves as
        -- current with nothing to say so.
        locf(
            last(corrected.observed_at, corrected.observed_at),
            (SELECT prior.capital_observed_at FROM prior),
            treat_null_as_missing => true
        ) AS capital_observed_at
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
                encumbrance_ratio=_optional_decimal(row.encumbrance_ratio, "encumbrance_ratio"),
                assets_usd=_optional_decimal(row.assets_usd, "assets_usd"),
                assets_observed_at=row.assets_observed_at,
                capital_observed_at=row.capital_observed_at,
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
