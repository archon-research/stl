from app.domain.entities.backed_breakdown import BackedBreakdown
from app.ports.backed_breakdown_repository import BackedBreakdownRepository


class BackedBreakdownService:
    """Use case: compute the collateral breakdown backing a debt token."""

    def __init__(self, repository: BackedBreakdownRepository) -> None:
        self._repository = repository

    async def get_backed_breakdown(self, protocol_id: int, debt_token_id: int) -> BackedBreakdown:
        """Delegate to the repository to compute the backed breakdown."""
        return await self._repository.get_backed_breakdown(protocol_id=protocol_id, debt_token_id=debt_token_id)
