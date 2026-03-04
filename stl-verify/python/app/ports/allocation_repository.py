from abc import ABC, abstractmethod

from app.domain.entities.allocation import AllocationPosition, Star


class AllocationRepository(ABC):
    @abstractmethod
    async def list_stars(self) -> list[Star]:
        """Return all distinct stars."""

    @abstractmethod
    async def list_allocations_by_star(self, star_id: str, block_number: int | None = None) -> list[AllocationPosition]:
        """Return allocation positions for the given star, identified by proxy address (hex with 0x prefix).

        When block_number is provided, only positions at that exact block are returned.
        When omitted, positions at the latest block_number for the star are returned.
        """
