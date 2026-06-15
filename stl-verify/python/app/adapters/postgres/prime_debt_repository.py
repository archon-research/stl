import logging
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime_debt import PrimeDebtSnapshot

logger = logging.getLogger(__name__)


class PostgresPrimeDebtRepository:
    """PostgreSQL adapter for prime debt snapshot queries."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    @staticmethod
    def _prime_match_clause() -> str:
        # /v1/primes exposes allocation proxy_address, while prime_debt is keyed by prime.id
        # and prime.vault_address. Resolve by either identity to keep API contracts consistent.
        return """
            p.vault_address = decode(:address_hex, 'hex')
            OR EXISTS (
                SELECT 1
                FROM allocation_position ap
                WHERE ap.prime_id = p.id
                  AND ap.proxy_address = decode(:address_hex, 'hex')
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
            + """
            AND (CAST(:from_timestamp AS TIMESTAMP) IS NULL OR pd.synced_at >= CAST(:from_timestamp AS TIMESTAMP))
            AND (CAST(:to_timestamp AS TIMESTAMP) IS NULL OR pd.synced_at <= CAST(:to_timestamp AS TIMESTAMP))
            ORDER BY pd.synced_at DESC, pd.block_number DESC, pd.block_version DESC
            LIMIT :limit
            """
        )

        params = {
            "address_hex": prime_address.hex,
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "limit": min(max(limit, 1), 500),
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
