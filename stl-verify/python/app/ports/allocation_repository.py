from abc import ABC, abstractmethod

from app.domain.entities.allocation import AllocationPosition, Star


class AllocationRepository(ABC):
    @abstractmethod
    async def list_stars(self) -> list[Star]:
        """Return all distinct stars."""

    @abstractmethod
    async def list_allocations_by_star(self, star: str) -> list[AllocationPosition]:
        """Return all allocation positions for the given star name."""
