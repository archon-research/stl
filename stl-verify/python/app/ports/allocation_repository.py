from decimal import Decimal
from typing import Protocol

from app.domain.entities.allocation import EthAddress, Prime, ReceiptTokenPosition


class AllocationRepository(Protocol):
    async def list_primes(self) -> list[Prime]:
        """Return all distinct primes."""
        ...

    async def list_receipt_token_positions(self, prime_id: EthAddress) -> list[ReceiptTokenPosition]:
        """Return current receipt-token holdings for the given prime."""
        ...

    async def get_usd_exposure(self, receipt_token_id: int, prime_id: EthAddress) -> Decimal:
        """Return ``balance × price_usd`` for the prime's holding of a receipt token.

        Raises ``ValueError`` if the position or price cannot be resolved.
        """
        ...
