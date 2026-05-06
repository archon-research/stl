from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime_debt import PrimeDebtSnapshot


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

        async with self._engine.connect() as conn:
            row = (await conn.execute(query, {"address_hex": prime_address.hex})).fetchone()

        return row is not None

    async def list_debt_snapshots(self, prime_address: EthAddress, *, limit: int = 100) -> list[PrimeDebtSnapshot]:
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
            ORDER BY pd.synced_at DESC, pd.block_number DESC, pd.block_version DESC
            LIMIT :limit
            """
        )

        params = {
            "address_hex": prime_address.hex,
            "limit": min(max(limit, 1), 500),
        }

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
