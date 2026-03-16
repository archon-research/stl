from typing import Protocol

from app.domain.entities.backed_breakdown import BackedBreakdown


class BackedBreakdownRepository(Protocol):
    """Port for querying the collateral breakdown backing a debt token."""

    async def get_backed_breakdown(self, backed_asset_id: int) -> BackedBreakdown:
        """Compute which collateral assets back the given asset.

        Args:
            backed_asset_id: The ID of the asset to analyze (e.g. debt token ID
                for lending protocols, vault ID for Morpho).

        Returns:
            BackedBreakdown with amount-first collateral contributions.
        """
        ...
