from app.domain.entities.positions import UserLatestPositions


class PositionService:
    """Service for calculating and retrieving user positions."""

    async def list_latest_user_positions(
        self, protocol_id: int, chain_id: int, limit: int
    ) -> list[UserLatestPositions]:
        """Retrieve latest positions for users in a protocol on a specific chain."""
        return []
