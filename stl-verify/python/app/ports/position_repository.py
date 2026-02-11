from typing import Protocol

from app.domain.entities.positions import UserLatestPositions


class PositionRepository(Protocol):
    """Repository interface for position data access.

    Concrete SQLAlchemy implementation will be added in future PR.
    """

    async def list_latest_user_positions(self, protocol_id: int, limit: int) -> list[UserLatestPositions]:
        """Retrieve latest positions for users in a protocol."""
        ...
