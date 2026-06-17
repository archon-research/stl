import logging
from datetime import datetime
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres._time_window import (
    clamp_limit,
    optional_time_window_clause,
    required_time_window_clause,
)
from app.domain.entities.allocation import EthAddress
from app.domain.entities.capital_metrics import CapitalMetricsSnapshot
from app.domain.entities.time_series_bucket import CapitalMetricsBucket

logger = logging.getLogger(__name__)

_CAPITAL_METRICS_LIMIT = 500


def _derive_buffer(total: Decimal | None, first_loss: Decimal | None) -> Decimal | None:
    """capital_buffer = max(total_capital - first_loss_capital, 0), or None when
    either input is missing (leading gap-filled buckets)."""
    if total is None or first_loss is None:
        return None
    return max(total - first_loss, Decimal("0"))


class PostgresCapitalMetricsRepository:
    """PostgreSQL adapter for stored capital-metrics snapshot queries."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    @staticmethod
    def _prime_match_clause() -> str:
        # /v1/primes exposes allocation proxy_address, while capital_metrics_snapshot
        # is keyed by prime.id and prime.vault_address. Resolve by either identity to
        # keep API contracts consistent. Parenthesized so callers can AND a time
        # window without the trailing OR absorbing it via AND-over-OR precedence.
        return """
            (
                p.vault_address = decode(:address_hex, 'hex')
                OR EXISTS (
                    SELECT 1
                    FROM allocation_position ap
                    WHERE ap.prime_id = p.id
                      AND ap.proxy_address = decode(:address_hex, 'hex')
                )
            )
        """

    async def prime_exists(self, prime_address: EthAddress) -> bool:
        query = text(
            """
            SELECT 1
            FROM prime p
            WHERE
            """
            + self._prime_match_clause()
            + """
            LIMIT 1
            """
        )

        try:
            async with self._engine.connect() as conn:
                row = (await conn.execute(query, {"address_hex": prime_address.hex})).fetchone()

            return row is not None
        except Exception as exc:
            logger.error(
                "Failed to check prime existence in database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "prime_address": str(prime_address),
                },
                exc_info=True,
            )
            raise ValueError(f"Database query failed while checking if prime {prime_address} exists: {exc}") from exc

    async def list_snapshots(
        self,
        prime_address: EthAddress,
        *,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
        limit: int = 100,
    ) -> list[CapitalMetricsSnapshot]:
        query = text(
            """
            SELECT
                encode(p.vault_address, 'hex') AS prime_address,
                p.name AS prime_name,
                cm.risk_capital,
                cm.total_capital,
                cm.first_loss_capital,
                cm.risk_to_capital_ratio,
                cm.benchmark_source,
                cm.synced_at
            FROM capital_metrics_snapshot cm
            JOIN prime p ON p.id = cm.prime_id
            WHERE
            """
            + self._prime_match_clause()
            + f"""
            {optional_time_window_clause("cm.synced_at")}
            ORDER BY cm.synced_at DESC
            LIMIT :limit
            """
        )

        params = {
            "address_hex": prime_address.hex,
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "limit": clamp_limit(limit, _CAPITAL_METRICS_LIMIT),
        }

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(query, params)
                rows = result.fetchall()

            return [
                CapitalMetricsSnapshot(
                    prime_address="0x" + row.prime_address,
                    prime_name=row.prime_name,
                    risk_capital=row.risk_capital,
                    total_capital=row.total_capital,
                    first_loss_capital=row.first_loss_capital,
                    capital_buffer=_derive_buffer(row.total_capital, row.first_loss_capital),
                    risk_to_capital_ratio=row.risk_to_capital_ratio,
                    benchmark_source=row.benchmark_source,
                    synced_at=row.synced_at,
                )
                for row in rows
            ]
        except Exception as exc:
            logger.error(
                "Failed to fetch capital metrics snapshots from database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "prime_address": str(prime_address),
                    "limit": limit,
                },
                exc_info=True,
            )
            raise ValueError(
                f"Database query failed while fetching capital metrics for prime {prime_address}: {exc}"
            ) from exc

    async def list_buckets(
        self,
        prime_address: EthAddress,
        *,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[CapitalMetricsBucket]:
        """Return the last observed metrics per time bucket, gap-filled (LOCF).

        capital_buffer is derived per bucket from the gap-filled total and
        first-loss values, so it stays consistent with them.
        """
        query = text(
            """
            SELECT
                time_bucket_gapfill(
                    make_interval(secs => :bucket_seconds),
                    cm.synced_at,
                    CAST(:from_timestamp AS TIMESTAMPTZ),
                    CAST(:to_timestamp AS TIMESTAMPTZ)
                ) AS bucket_start,
                locf(last(cm.risk_capital, cm.synced_at)) AS risk_capital,
                locf(last(cm.total_capital, cm.synced_at)) AS total_capital,
                locf(last(cm.first_loss_capital, cm.synced_at)) AS first_loss_capital,
                locf(last(cm.risk_to_capital_ratio, cm.synced_at)) AS risk_to_capital_ratio
            FROM capital_metrics_snapshot cm
            JOIN prime p ON p.id = cm.prime_id
            WHERE
            """
            + self._prime_match_clause()
            + f"""
            {required_time_window_clause("cm.synced_at")}
            GROUP BY bucket_start
            ORDER BY bucket_start DESC
            LIMIT :limit
            """
        )

        params = {
            "address_hex": prime_address.hex,
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "bucket_seconds": bucket_seconds,
            "limit": clamp_limit(limit, _CAPITAL_METRICS_LIMIT),
        }

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(query, params)
                rows = result.fetchall()

            return [
                CapitalMetricsBucket(
                    bucket_start=row.bucket_start,
                    risk_capital=row.risk_capital,
                    total_capital=row.total_capital,
                    first_loss_capital=row.first_loss_capital,
                    capital_buffer=_derive_buffer(row.total_capital, row.first_loss_capital),
                    risk_to_capital_ratio=row.risk_to_capital_ratio,
                )
                for row in rows
            ]
        except Exception as exc:
            logger.error(
                "Failed to fetch capital metrics buckets from database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "prime_address": str(prime_address),
                    "limit": limit,
                },
                exc_info=True,
            )
            raise ValueError(
                f"Database query failed while fetching capital metrics buckets for prime {prime_address}: {exc}"
            ) from exc
