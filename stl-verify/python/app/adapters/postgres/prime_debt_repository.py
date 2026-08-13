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


class PrimeDebtRepository:
    """PostgreSQL adapter for prime debt snapshot queries."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    @staticmethod
    def _prime_match_clause() -> str:
        # /v1/primes exposes allocation proxy_address, while prime_debt is keyed by prime.id
        # and prime.vault_address. Resolve by either identity to keep API contracts consistent.
        # Parenthesized so that callers can AND additional predicates (e.g. a time window)
        # without the trailing OR absorbing them via AND-over-OR precedence.
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

    async def list_debt_snapshots(
        self,
        prime_address: EthAddress,
        *,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
        limit: int = 100,
    ) -> list[PrimeDebtSnapshot]:
        query = text(
            """
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
            WHERE
            """
            + self._prime_match_clause()
            + f"""
            {optional_time_window_clause("pd.synced_at")}
            ORDER BY pd.synced_at DESC, pd.block_number DESC, pd.block_version DESC
            LIMIT :limit
            """
        )

        params = {
            "address_hex": prime_address.hex,
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "limit": clamp_limit(limit, _PRIME_DEBT_LIMIT),
        }

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(query, params)
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
                    "prime_address": str(prime_address),
                    "limit": limit,
                },
                exc_info=True,
            )
            raise ValueError(
                f"Database query failed while fetching debt snapshots for prime {prime_address}: {exc}"
            ) from exc

    async def list_debt_buckets(
        self,
        prime_address: EthAddress,
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
        query = text(
            """
            SELECT
                time_bucket_gapfill(
                    make_interval(secs => :bucket_seconds),
                    pd.synced_at,
                    CAST(:from_timestamp AS TIMESTAMPTZ),
                    CAST(:to_timestamp AS TIMESTAMPTZ)
                ) AS bucket_start,
                locf(last(pd.debt_wad, pd.synced_at)) AS debt_wad
            FROM prime_debt pd
            JOIN prime p ON p.id = pd.prime_id
            WHERE
            """
            + self._prime_match_clause()
            + f"""
            {required_time_window_clause("pd.synced_at")}
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
            "limit": clamp_limit(limit, _PRIME_DEBT_LIMIT),
        }

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(query, params)
                rows = result.fetchall()

            return [PrimeDebtBucket(bucket_start=row.bucket_start, debt_wad=row.debt_wad) for row in rows]
        except Exception as exc:
            logger.error(
                "Failed to fetch prime debt buckets from database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "prime_address": str(prime_address),
                    "limit": limit,
                },
                exc_info=True,
            )
            raise ValueError(
                f"Database query failed while fetching debt buckets for prime {prime_address}: {exc}"
            ) from exc
