from typing import Protocol

from app.ports.backed_breakdown_repository import BackedBreakdownRepository


class BackedBreakdownRepositoryResolver(Protocol):
    """Port for resolving the protocol-scoped backed breakdown repository."""

    async def resolve(self, protocol_id: int) -> BackedBreakdownRepository:
        """Return the repository that matches the given protocol ID."""
        ...
