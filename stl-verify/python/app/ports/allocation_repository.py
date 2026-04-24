from typing import Protocol

from app.domain.entities.allocation import AllocationPosition, EthAddress, Prime, ReceiptTokenPosition


class AllocationRepository(Protocol):
    async def list_primes(self) -> list[Prime]:
        """Return all distinct primes."""
        ...

    async def get_prime(self, address: EthAddress) -> Prime | None:
        """Return the prime with the given address, or None if not found."""
        ...

    async def list_receipt_token_positions(self, prime_id: EthAddress) -> list[ReceiptTokenPosition]:
        """Return receipt token positions with balances for the given prime."""
        ...

    async def list_allocations_by_prime(
        self, prime_id: EthAddress, block_number: int | None = None
    ) -> list[AllocationPosition]:
        """Return allocation positions for the given prime, identified by a validated EthAddress.

        When block_number is provided, only positions at that exact block are returned.
        When omitted, positions at the latest block_number for the prime are returned.
        """
        ...
