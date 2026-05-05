import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.protocol_event import ProtocolEvent

logger = logging.getLogger(__name__)


class PostgresProtocolEventRepository:
    """PostgreSQL adapter for protocol event and related snapshot queries."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    @staticmethod
    def _normalize_tx_hash(tx_hash: str | None) -> str | None:
        if tx_hash is None:
            return None
        return tx_hash[2:] if tx_hash.startswith("0x") else tx_hash

    @staticmethod
    def _to_entity(row: object) -> ProtocolEvent:
        return ProtocolEvent(
            tx_hash="0x" + row.tx_hash,
            log_index=row.log_index,
            chain_id=row.chain_id,
            block_number=row.block_number,
            block_version=row.block_version,
            protocol_name=row.protocol_name,
            event_name=row.event_name,
            contract_address="0x" + row.contract_address,
            event_data=row.event_data,
            created_at=row.created_at,
        )

    async def list_events(
        self,
        *,
        tx_hash: str | None = None,
        protocol_name: str | None = None,
        limit: int = 100,
    ) -> list[ProtocolEvent]:
        """List protocol events with optional filters."""
        query = """
            SELECT
                encode(pe.tx_hash, 'hex') AS tx_hash,
                pe.log_index,
                pe.chain_id,
                pe.block_number,
                pe.block_version,
                p.name AS protocol_name,
                pe.event_name,
                encode(pe.contract_address, 'hex') AS contract_address,
                pe.event_data,
                pe.created_at
            FROM protocol_event pe
            JOIN protocol p ON pe.protocol_id = p.id
            WHERE (CAST(:tx_hash AS TEXT) IS NULL OR pe.tx_hash = decode(CAST(:tx_hash AS TEXT), 'hex'))
            AND (CAST(:protocol_name AS TEXT) IS NULL OR p.name = CAST(:protocol_name AS TEXT))
            ORDER BY pe.created_at DESC, pe.block_number DESC, pe.log_index DESC
            LIMIT :limit
        """

        params = {
            "tx_hash": self._normalize_tx_hash(tx_hash),
            "protocol_name": protocol_name,
            "limit": min(max(limit, 1), 500),
        }

        async with self._engine.connect() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()

        return [self._to_entity(row) for row in rows]

    async def list_events_by_tx(self, tx_hash: str) -> list[ProtocolEvent]:
        """Get all events for a transaction."""
        query = """
            SELECT
                encode(pe.tx_hash, 'hex') AS tx_hash,
                pe.log_index,
                pe.chain_id,
                pe.block_number,
                pe.block_version,
                p.name AS protocol_name,
                pe.event_name,
                encode(pe.contract_address, 'hex') AS contract_address,
                pe.event_data,
                pe.created_at
            FROM protocol_event pe
            JOIN protocol p ON pe.protocol_id = p.id
            WHERE pe.tx_hash = decode(:tx_hash, 'hex')
            ORDER BY pe.log_index ASC, pe.created_at DESC
        """

        async with self._engine.connect() as conn:
            result = await conn.execute(text(query), {"tx_hash": self._normalize_tx_hash(tx_hash)})
            rows = result.fetchall()

        return [self._to_entity(row) for row in rows]
