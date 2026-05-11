from datetime import datetime
from decimal import Decimal
from typing import Protocol

from app.domain.entities.allocation import (
    ChainMetadata,
    DirectAssetHolding,
    EthAddress,
    Prime,
    ProtocolMetadata,
    ReceiptTokenPosition,
)
from app.domain.entities.allocation_activity import AllocationActivityEvent


class AllocationRepository(Protocol):
    async def list_chains(self) -> list[ChainMetadata]:
        """Return chain metadata used by the UI."""
        ...

    async def list_protocols(self) -> list[ProtocolMetadata]:
        """Return protocol metadata used by the UI."""
        ...

    async def list_primes(self) -> list[Prime]:
        """Return all distinct primes."""
        ...

    async def prime_exists(self, prime_id: EthAddress) -> bool:
        """Return True iff any allocation_position row has ever been recorded
        for the given proxy address (registered prime, regardless of current
        non-zero balances)."""
        ...

    async def list_receipt_token_positions(self, prime_id: EthAddress) -> list[ReceiptTokenPosition]:
        """Return current receipt-token holdings for the given prime."""
        ...

    async def list_direct_asset_holdings(self, prime_id: EthAddress) -> list[DirectAssetHolding]:
        """Return tokens held directly by the prime that are not registered as receipt-token wrappers."""
        ...

    async def get_usd_exposure(self, receipt_token_id: int, prime_id: EthAddress) -> Decimal:
        """Return ``balance × price_usd`` for the prime's holding of a receipt token.

        Raises ``ValueError`` if the position or price cannot be resolved.
        """
        ...

    async def get_total_usd_exposure(self, prime_id: EthAddress) -> Decimal:
        """Return total priced USD exposure for all current positions of a prime."""
        ...

    async def list_allocation_activity(
        self,
        *,
        prime_id: EthAddress | None = None,
        chain_id: int | None = None,
        protocol_name: str | None = None,
        action_type: str | None = None,
        token_symbol: str | None = None,
        tx_hash: str | None = None,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
        limit: int = 100,
    ) -> list[AllocationActivityEvent]:
        """Return allocation activity events with optional filters."""
        ...
