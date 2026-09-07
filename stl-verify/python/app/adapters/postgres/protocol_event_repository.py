import logging
from datetime import datetime
from typing import Any

from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres._time_window import (
    clamp_limit,
    optional_time_window_clause,
    required_time_window_clause,
    time_bucket_expr,
)
from app.domain.entities.protocol_event import ProtocolEvent
from app.domain.entities.time_series_bucket import ProtocolEventBucket

logger = logging.getLogger(__name__)

_PROTOCOL_EVENT_LIMIT = 500

_EVENT_SELECT_COLUMNS = """
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
"""


class ProtocolEventRepository:
    """PostgreSQL adapter for protocol event and related snapshot queries."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    @staticmethod
    def _normalize_tx_hash(tx_hash: str | None) -> str | None:
        if tx_hash is None:
            return None
        if tx_hash.startswith(("0x", "0X")):
            return tx_hash[2:]
        return tx_hash

    @staticmethod
    def _to_entity(row: Row[Any]) -> ProtocolEvent:
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
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
        limit: int = 100,
    ) -> list[ProtocolEvent]:
        """List protocol events with optional filters."""
        query = (
            _EVENT_SELECT_COLUMNS
            + f"""
            WHERE (CAST(:tx_hash AS TEXT) IS NULL OR pe.tx_hash = decode(CAST(:tx_hash AS TEXT), 'hex'))
            AND (CAST(:protocol_name AS TEXT) IS NULL OR p.name = CAST(:protocol_name AS TEXT))
            {optional_time_window_clause("pe.created_at")}
            ORDER BY pe.created_at DESC, pe.block_number DESC, pe.log_index DESC
            LIMIT :limit
        """
        )

        params = {
            "tx_hash": self._normalize_tx_hash(tx_hash),
            "protocol_name": protocol_name,
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "limit": clamp_limit(limit, _PROTOCOL_EVENT_LIMIT),
        }

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(text(query), params)
                rows = result.fetchall()

            return [self._to_entity(row) for row in rows]
        except Exception as exc:
            logger.error(
                "Failed to fetch protocol events from database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "tx_hash": tx_hash,
                    "protocol_name": protocol_name,
                    "limit": limit,
                },
                exc_info=True,
            )
            raise ValueError(f"Database query failed while fetching protocol events: {exc}") from exc

    async def list_event_buckets(
        self,
        *,
        tx_hash: str | None = None,
        protocol_name: str | None = None,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[ProtocolEventBucket]:
        """Return the count of protocol events per time bucket, newest bucket first."""
        query = text(
            f"""
            SELECT
                {time_bucket_expr("pe.created_at")} AS bucket_start,
                COUNT(*) AS event_count
            FROM protocol_event pe
            JOIN protocol p ON pe.protocol_id = p.id
            WHERE (CAST(:tx_hash AS TEXT) IS NULL OR pe.tx_hash = decode(CAST(:tx_hash AS TEXT), 'hex'))
            AND (CAST(:protocol_name AS TEXT) IS NULL OR p.name = CAST(:protocol_name AS TEXT))
            {required_time_window_clause("pe.created_at")}
            GROUP BY bucket_start
            ORDER BY bucket_start DESC
            LIMIT :limit
            """
        )

        params = {
            "tx_hash": self._normalize_tx_hash(tx_hash),
            "protocol_name": protocol_name,
            "from_timestamp": from_timestamp,
            "to_timestamp": to_timestamp,
            "bucket_seconds": bucket_seconds,
            "limit": clamp_limit(limit, _PROTOCOL_EVENT_LIMIT),
        }

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(query, params)
                rows = result.fetchall()

            return [ProtocolEventBucket(bucket_start=row.bucket_start, event_count=row.event_count) for row in rows]
        except Exception as exc:
            logger.error(
                "Failed to fetch protocol event buckets from database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "tx_hash": tx_hash,
                    "protocol_name": protocol_name,
                    "limit": limit,
                },
                exc_info=True,
            )
            raise ValueError(f"Database query failed while fetching protocol event buckets: {exc}") from exc

    async def list_events_by_tx(self, tx_hash: str) -> list[ProtocolEvent]:
        """Get all events for a transaction."""
        query = (
            _EVENT_SELECT_COLUMNS
            + """
            WHERE pe.tx_hash = decode(:tx_hash, 'hex')
            ORDER BY pe.log_index ASC, pe.created_at DESC
        """
        )

        try:
            async with self._engine.connect() as conn:
                result = await conn.execute(text(query), {"tx_hash": self._normalize_tx_hash(tx_hash)})
                rows = result.fetchall()

            return [self._to_entity(row) for row in rows]
        except Exception as exc:
            logger.error(
                "Failed to fetch events by transaction hash from database",
                extra={
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "tx_hash": tx_hash,
                },
                exc_info=True,
            )
            raise ValueError(f"Database query failed while fetching events for transaction {tx_hash}: {exc}") from exc
