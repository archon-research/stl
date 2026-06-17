from datetime import datetime
from typing import Protocol

from app.domain.entities.protocol_event import ProtocolEvent
from app.domain.entities.time_series_bucket import ProtocolEventBucket


class ProtocolEventRepository(Protocol):
    """Repository interface for protocol event queries."""

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
        ...

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
        """Return protocol event counts aggregated into time buckets."""
        ...

    async def list_events_by_tx(self, tx_hash: str) -> list[ProtocolEvent]:
        """Get all events for a transaction."""
        ...
