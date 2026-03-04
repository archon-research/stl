from app.domain.entities.allocation import AllocationPosition, Star
from app.ports.allocation_repository import AllocationRepository


class AllocationService:
    def __init__(self, repository: AllocationRepository) -> None:
        self._repository = repository

    async def list_stars(self) -> list[Star]:
        return await self._repository.list_stars()

    async def list_allocations_by_star(self, star: str, block_number: int | None = None) -> list[AllocationPosition]:
        return await self._repository.list_allocations_by_star(star, block_number)
