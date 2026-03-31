from app.domain.entities.allocation import AllocationPosition, EthAddress, ReceiptTokenPosition, Star
from app.ports.allocation_repository import AllocationRepository


class AllocationService:
    def __init__(self, repository: AllocationRepository) -> None:
        self._repository = repository

    async def list_stars(self) -> list[Star]:
        return await self._repository.list_stars()

    async def get_star(self, address: EthAddress) -> Star | None:
        return await self._repository.get_star(address)

    async def list_receipt_token_positions(self, star_id: EthAddress) -> list[ReceiptTokenPosition]:
        return await self._repository.list_receipt_token_positions(star_id)

    async def list_allocations_by_star(
        self, star_id: EthAddress, block_number: int | None = None
    ) -> list[AllocationPosition]:
        return await self._repository.list_allocations_by_star(star_id, block_number)
