"""Postgres implementation of the direct-asset-holding lookup.

Finds the most recent ``allocation_position`` row for a token address that is
NOT in the ``receipt_token`` table — i.e. a raw ERC-20 balance held directly
by some prime proxy.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.allocation import DirectAssetHolding, EthAddress
from app.logging import get_logger

logger = get_logger(__name__)

_LOOKUP_SQL = text("""
    SELECT
        ap.chain_id,
        t.id        AS token_id,
        t.address   AS token_address,
        t.symbol    AS symbol,
        ap.balance,
        tp.price_usd
    FROM allocation_position ap
    JOIN token t ON t.id = ap.token_id
    LEFT JOIN LATERAL (
        SELECT tpc.price_usd
        FROM token_price_current tpc
        WHERE tpc.token_id = t.id
        LIMIT 1
    ) tp ON true
    WHERE ap.chain_id = :chain_id
      AND t.address = :token_address
      AND NOT EXISTS (
          SELECT 1 FROM receipt_token rt
          WHERE rt.receipt_token_address = t.address AND rt.chain_id = ap.chain_id
      )
    ORDER BY ap.block_number DESC, ap.block_version DESC,
             ap.processing_version DESC, ap.log_index DESC
    LIMIT 1
""")


class PostgresDirectAssetLookupRepository:
    """Postgres-backed lookup for direct asset holdings."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def get_by_chain_and_address(self, chain_id: int, token_address: EthAddress) -> DirectAssetHolding | None:
        addr_bytes = token_address.to_bytes()
        async with self._engine.connect() as conn:
            await conn.exec_driver_sql("SET LOCAL statement_timeout = '5s'")
            result = await conn.execute(
                _LOOKUP_SQL,
                {"chain_id": chain_id, "token_address": addr_bytes},
            )
            row = result.fetchone()

        if row is None:
            return None

        balance = row.balance
        price_usd = row.price_usd
        amount_usd = (balance * price_usd) if price_usd is not None else None

        return DirectAssetHolding(
            chain_id=row.chain_id,
            token_id=row.token_id,
            token_address="0x" + bytes(row.token_address).hex(),
            symbol=row.symbol or "UNKNOWN",
            balance=balance,
            amount_usd=amount_usd,
        )
