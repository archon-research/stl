from datetime import datetime

from app.domain.entities.protocol_event import ProtocolEvent
from app.domain.entities.time_series_bucket import ProtocolEventBucket
from app.ports.protocol_event_repository import ProtocolEventRepository


class ProtocolEventService:
    """Service for protocol event operations."""

    def __init__(self, repository: ProtocolEventRepository) -> None:
        self._repository = repository

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
        return await self._repository.list_events(
            tx_hash=tx_hash,
            protocol_name=protocol_name,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            limit=limit,
        )

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
        """List protocol event counts aggregated into time buckets."""
        return await self._repository.list_event_buckets(
            tx_hash=tx_hash,
            protocol_name=protocol_name,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            bucket_seconds=bucket_seconds,
            limit=limit,
        )

    async def get_events_by_tx(self, tx_hash: str) -> list[ProtocolEvent]:
        """Get all events for a transaction."""
        return await self._repository.list_events_by_tx(tx_hash)
