from typing import Protocol

from app.domain.entities.allocation import AllocationPosition, EthAddress, ReceiptTokenPosition, Star


class AllocationRepository(Protocol):
    async def list_stars(self) -> list[Star]:
        """Return all distinct stars."""
        ...

    async def get_star(self, address: EthAddress) -> Star | None:
        """Return the star with the given address, or None if not found."""
        ...

    async def list_receipt_token_positions(self, star_id: EthAddress) -> list[ReceiptTokenPosition]:
        """Return receipt token positions with balances for the given star."""
        ...

    async def list_allocations_by_star(
        self, star_id: EthAddress, block_number: int | None = None
    ) -> list[AllocationPosition]:
        """Return allocation positions for the given star, identified by a validated EthAddress.

        When block_number is provided, only positions at that exact block are returned.
        When omitted, positions at the latest block_number for the star are returned.
        """
        ...
