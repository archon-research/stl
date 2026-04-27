from app.domain.entities.allocation import EthAddress, Prime, ReceiptTokenPosition
from app.ports.allocation_repository import AllocationRepository


class AllocationService:
    def __init__(self, repository: AllocationRepository) -> None:
        self._repository = repository

    async def list_primes(self) -> list[Prime]:
        return await self._repository.list_primes()

    async def list_receipt_token_positions(self, prime_id: EthAddress) -> list[ReceiptTokenPosition]:
        return await self._repository.list_receipt_token_positions(prime_id)
