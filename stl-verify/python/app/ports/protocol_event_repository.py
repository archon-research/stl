from typing import Protocol

from app.domain.entities.protocol_event import ProtocolEvent


class ProtocolEventRepository(Protocol):
    """Repository interface for protocol event queries."""

    async def list_events(
        self,
        *,
        tx_hash: str | None = None,
        protocol_name: str | None = None,
        limit: int = 100,
    ) -> list[ProtocolEvent]:
        """List protocol events with optional filters."""
        ...

    async def list_events_by_tx(self, tx_hash: str) -> list[ProtocolEvent]:
        """Get all events for a transaction."""
        ...
