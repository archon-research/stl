from typing import Protocol

from app.domain.entities.allocation import AllocationPosition, EthAddress, Star


class AllocationRepository(Protocol):
    async def list_stars(self) -> list[Star]:
        """Return all distinct stars."""
        ...

    async def list_allocations_by_star(
        self, star_id: EthAddress, block_number: int | None = None
    ) -> list[AllocationPosition]:
        """Return allocation positions for the given star, identified by a validated EthAddress.

        When block_number is provided, only positions at that exact block are returned.
        When omitted, positions at the latest block_number for the star are returned.
        """
        ...
