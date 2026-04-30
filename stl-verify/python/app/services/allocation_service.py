from datetime import datetime
from decimal import Decimal

from app.domain.entities.allocation import EthAddress, Prime, ReceiptTokenPosition
from app.domain.entities.allocation_activity import AllocationActivityEvent
from app.ports.allocation_repository import AllocationRepository


class AllocationService:
    def __init__(self, repository: AllocationRepository) -> None:
        self._repository = repository

    async def list_primes(self) -> list[Prime]:
        return await self._repository.list_primes()

    async def list_receipt_token_positions(self, prime_id: EthAddress) -> list[ReceiptTokenPosition]:
        return await self._repository.list_receipt_token_positions(prime_id)

    async def get_total_usd_exposure(self, prime_id: EthAddress) -> Decimal:
        return await self._repository.get_total_usd_exposure(prime_id)

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
        return await self._repository.list_allocation_activity(
            prime_id=prime_id,
            chain_id=chain_id,
            protocol_name=protocol_name,
            action_type=action_type,
            token_symbol=token_symbol,
            tx_hash=tx_hash,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            limit=limit,
        )
