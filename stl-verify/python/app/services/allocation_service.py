from app.domain.entities.allocation import AllocationPosition, EthAddress, Prime, ReceiptTokenPosition
from app.ports.allocation_repository import AllocationRepository


class AllocationService:
    def __init__(self, repository: AllocationRepository) -> None:
        self._repository = repository

    async def list_primes(self) -> list[Prime]:
        return await self._repository.list_primes()

    async def get_prime(self, address: EthAddress) -> Prime | None:
        return await self._repository.get_prime(address)

    async def list_receipt_token_positions(self, prime_id: EthAddress) -> list[ReceiptTokenPosition]:
        return await self._repository.list_receipt_token_positions(prime_id)

    async def list_allocations_by_prime(
        self, prime_id: EthAddress, block_number: int | None = None
    ) -> list[AllocationPosition]:
        return await self._repository.list_allocations_by_prime(prime_id, block_number)
