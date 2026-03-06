from typing import Protocol

from app.domain.entities.positions import UserLatestPositions


class PositionRepository(Protocol):
    """Repository interface for position data access.

    Concrete PostgreSQL implementation is in app.adapters.postgres.position_repository.
    """

    async def list_latest_user_positions(
        self, protocol_id: int, chain_id: int, limit: int
    ) -> list[UserLatestPositions]:
        """Retrieve latest positions for users in a protocol on a specific chain.

        Args:
            protocol_id: The protocol ID to query positions for
            chain_id: The chain ID to scope the query to
            limit: Maximum number of users to return (0 = unlimited)
        """
        ...
