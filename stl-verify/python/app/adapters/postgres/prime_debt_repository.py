import logging
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres._time_window import (
    clamp_limit,
    optional_time_window_clause,
    required_time_window_clause,
)
from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime_debt import PrimeDebtSnapshot
from app.domain.entities.time_series_bucket import PrimeDebtBucket

logger = logging.getLogger(__name__)

_PRIME_DEBT_LIMIT = 500

# Debt reads use the prime.id resolved up front; inline address matching cannot be decorrelated.
# An EXISTS under OR re-scans every allocation_position chunk per joined prime_debt row.
DEBT_SNAPSHOTS_SQL = f"""
    SELECT
        encode(p.vault_address, 'hex') AS prime_address,
        p.name AS prime_name,
        pd.ilk_name,
        pd.debt_wad,
        pd.block_number,
        pd.block_version,
        pd.synced_at
    FROM prime_debt pd
    JOIN prime p ON p.id = pd.prime_id
    WHERE pd.prime_id = :prime_id
    {optional_time_window_clause("pd.synced_at")}
    ORDER BY pd.synced_at DESC, pd.block_number DESC, pd.block_version DESC
    LIMIT :limit
"""

DEBT_BUCKETS_SQL = f"""
    SELECT
        time_bucket_gapfill(
            make_interval(secs => :bucket_seconds),
            pd.synced_at,
            CAST(:from_timestamp AS TIMESTAMPTZ),
            CAST(:to_timestamp AS TIMESTAMPTZ)
        ) AS bucket_start,
        locf(last(pd.debt_wad, pd.synced_at)) AS debt_wad
    FROM prime_debt pd
    WHERE pd.prime_id = :prime_id
    {required_time_window_clause("pd.synced_at")}
    GROUP BY bucket_start
    ORDER BY bucket_start DESC
    LIMIT :limit
"""


class PrimeDebtRepository:
    """PostgreSQL adapter for prime debt snapshot queries."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    @staticmethod
    def _prime_match_clause() -> str:
        # /v1/primes exposes allocation proxy_address, while prime_debt is keyed by prime.id
        # and prime.vault_address. Resolve by either identity to keep API contracts consistent.
        return """
            (
                p.vault_address = decode(:address_hex, 'hex')
                OR EXISTS (
                    SELECT 1
                    FROM prime_proxy pp
                    WHERE pp.prime_id = p.id
                      AND pp.proxy_address = decode(:address_hex, 'hex')
                )
            )
        """

    async def resolve_prime_id(self, prime_address: EthAddress) -> int | None:
        """Resolve either prime identity to its ``prime.id``, or ``None`` if unknown.

        ``prime_proxy`` holds one row per (chain, proxy) and asserts one prime per
        proxy, so the proxy arm matches at most one prime. A vault match still wins
        over a proxy match, and the ``p.id`` tiebreak keeps resolution deterministic
        if one address somehow satisfies both.
        """
        query = text(
            """
            SELECT p.id
            FROM prime p
            WHERE
            """
            + self._prime_match_clause()
            + """
            ORDER BY p.vault_address = decode(:address_hex, 'hex') DESC, p.id
            LIMIT 1
            """
        )

        try:
            async with self._engine.connect() as conn:
                row = (await conn.execute(query, {"address_hex": prime_address.hex})).fetchone()

            return row.id if row is not None else None
        except Exception as exc:
            logger.error(
                "Failed to resolve prime id in database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "prime_address": str(prime_address),
                },
                exc_info=True,
            )
            raise ValueError(f"Database query failed while resolving prime {prime_address}: {exc}") from exc

    async def list_debt_snapshots(
        self,
        prime_id: int,
        *,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
        limit: int = 100,
    ) -> list[PrimeDebtSnapshot]:
        params = {
            "prime_id": prime_id,
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "limit": clamp_limit(limit, _PRIME_DEBT_LIMIT),
        }

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(text(DEBT_SNAPSHOTS_SQL), params)
                rows = result.fetchall()

            return [
                PrimeDebtSnapshot(
                    prime_address="0x" + row.prime_address,
                    prime_name=row.prime_name,
                    ilk_name=row.ilk_name,
                    debt_wad=row.debt_wad,
                    block_number=row.block_number,
                    block_version=row.block_version,
                    synced_at=row.synced_at,
                )
                for row in rows
            ]
        except Exception as exc:
            logger.error(
                "Failed to fetch prime debt snapshots from database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "prime_id": prime_id,
                    "limit": limit,
                },
                exc_info=True,
            )
            raise ValueError(
                f"Database query failed while fetching debt snapshots for prime {prime_id}: {exc}"
            ) from exc

    async def list_debt_buckets(
        self,
        prime_id: int,
        *,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[PrimeDebtBucket]:
        """Return the last observed debt per time bucket, gap-filled (LOCF).

        Buckets with no observation carry the previous bucket's value forward;
        leading buckets before the first observation are ``None``.
        """
        params = {
            "prime_id": prime_id,
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "bucket_seconds": bucket_seconds,
            "limit": clamp_limit(limit, _PRIME_DEBT_LIMIT),
        }

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(text(DEBT_BUCKETS_SQL), params)
                rows = result.fetchall()

            return [PrimeDebtBucket(bucket_start=row.bucket_start, debt_wad=row.debt_wad) for row in rows]
        except Exception as exc:
            logger.error(
                "Failed to fetch prime debt buckets from database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "prime_id": prime_id,
                    "limit": limit,
                },
                exc_info=True,
            )
            raise ValueError(f"Database query failed while fetching debt buckets for prime {prime_id}: {exc}") from exc

    async def list_reference_debt_buckets(
        self,
        prime_id: int,
        *,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[PrimeDebtBucket]:
        """Return Sky's reported debt per time bucket, gap-filled (LOCF).

        Upstream publishes one already-normalized USD figure per prime per day,
        so it is scaled to ``wad`` here: the field means the same thing in both
        provenances, and a consumer dividing by 1e18 gets USDS units either way.
        There is no per-ilk split upstream, so this is the prime's whole debt.
        """
        query = text(
            f"""
            WITH corrected AS (
                SELECT DISTINCT ON (b.observed_at)
                    b.observed_at,
                    -- Rescaled here, not around locf(): TimescaleDB requires
                    -- locf to be the top-level call in its select expression.
                    b.debt_usd * 1e18 AS debt_wad
                FROM prime_reference_balance_sheet b
                WHERE b.prime_id = :prime_id
                {required_time_window_clause("b.observed_at")}
                ORDER BY b.observed_at, b.processing_version DESC
            ), prior AS (
                -- The last figure before the window, fed to locf as its
                -- `prev`. Upstream publishes one row per prime per day, so
                -- from a minute past midnight the newest row already sits
                -- outside a 24h window and the series would read as absent
                -- for most of every day. Bounded, because a figure this
                -- stale is not a current reading.
                SELECT b.debt_usd * 1e18 AS debt_wad
                FROM prime_reference_balance_sheet b
                WHERE b.prime_id = :prime_id
                  AND b.observed_at < CAST(:from_timestamp AS TIMESTAMPTZ)
                  AND b.observed_at >=
                      CAST(:from_timestamp AS TIMESTAMPTZ) - INTERVAL '90 days'
                ORDER BY b.observed_at DESC, b.processing_version DESC
                LIMIT 1
            )
            SELECT
                time_bucket_gapfill(
                    make_interval(secs => :bucket_seconds),
                    corrected.observed_at,
                    CAST(:from_timestamp AS TIMESTAMPTZ),
                    CAST(:to_timestamp AS TIMESTAMPTZ)
                ) AS bucket_start,
                locf(
                    last(corrected.debt_wad, corrected.observed_at),
                    (SELECT prior.debt_wad FROM prior),
                    treat_null_as_missing => true
                ) AS debt_wad
            FROM corrected
            GROUP BY bucket_start
            ORDER BY bucket_start DESC
            LIMIT :limit
            """
        )

        params = {
            "prime_id": prime_id,
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "bucket_seconds": bucket_seconds,
            "limit": clamp_limit(limit, _PRIME_DEBT_LIMIT),
        }

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(query, params)
                rows = result.fetchall()

            return [PrimeDebtBucket(bucket_start=row.bucket_start, debt_wad=row.debt_wad) for row in rows]
        except Exception as exc:
            logger.error(
                "Failed to fetch reference debt buckets from database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "prime_id": prime_id,
                    "limit": limit,
                },
                exc_info=True,
            )
            raise ValueError(
                f"Database query failed while fetching reference debt buckets for prime {prime_id}: {exc}"
            ) from exc
