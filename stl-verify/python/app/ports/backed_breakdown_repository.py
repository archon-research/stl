from typing import Protocol

from app.domain.entities.backed_breakdown import BackedBreakdown


class BackedBreakdownRepository(Protocol):
    """Port for querying the collateral breakdown backing a debt token."""

    async def get_backed_breakdown(self, debt_token_id: int) -> BackedBreakdown:
        """Compute which collateral assets back the given debt token.

        Args:
            debt_token_id: The token ID of the debt token to analyze.

        Returns:
            BackedBreakdown with amount-first collateral contributions.
        """
        ...
