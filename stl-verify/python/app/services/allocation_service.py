from datetime import datetime
from decimal import Decimal

from app.domain.entities.allocation import (
    AnchorageCustodyHolding,
    ChainMetadata,
    DirectAssetHolding,
    EthAddress,
    Prime,
    ProtocolMetadata,
    ReceiptTokenPosition,
)
from app.domain.entities.allocation_activity import AllocationActivityEvent
from app.domain.entities.time_series_bucket import (
    AllocationActivityBucket,
    ExposureBucket,
    TotalCapitalBucket,
)
from app.ports.allocation_repository import AllocationRepositoryPort


class AllocationService:
    def __init__(self, repository: AllocationRepositoryPort) -> None:
        self._repository = repository

    async def list_chains(self) -> list[ChainMetadata]:
        return await self._repository.list_chains()

    async def list_protocols(self) -> list[ProtocolMetadata]:
        return await self._repository.list_protocols()

    async def list_primes(self) -> list[Prime]:
        return await self._repository.list_primes()

    async def prime_exists(self, prime_address: EthAddress) -> bool:
        return await self._repository.prime_exists(prime_address)

    async def list_receipt_token_positions(self, prime_id: EthAddress) -> list[ReceiptTokenPosition]:
        return await self._repository.list_receipt_token_positions(prime_id)

    async def list_direct_asset_holdings(self, prime_id: EthAddress) -> list[DirectAssetHolding]:
        return await self._repository.list_direct_asset_holdings(prime_id)

    async def list_anchorage_custody_holdings(self, prime_id: EthAddress) -> list[AnchorageCustodyHolding]:
        return await self._repository.list_anchorage_custody_holdings(prime_id)

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

    async def list_activity_buckets(
        self,
        *,
        prime_id: EthAddress | None = None,
        chain_id: int | None = None,
        protocol_name: str | None = None,
        action_type: str | None = None,
        token_symbol: str | None = None,
        tx_hash: str | None = None,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[AllocationActivityBucket]:
        return await self._repository.list_activity_buckets(
            prime_id=prime_id,
            chain_id=chain_id,
            protocol_name=protocol_name,
            action_type=action_type,
            token_symbol=token_symbol,
            tx_hash=tx_hash,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            bucket_seconds=bucket_seconds,
            limit=limit,
        )

    async def list_total_capital_buckets(
        self,
        prime_address: EthAddress,
        *,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[TotalCapitalBucket]:
        return await self._repository.list_total_capital_buckets(
            prime_address,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            bucket_seconds=bucket_seconds,
            limit=limit,
        )

    async def list_exposure_buckets(
        self,
        prime_address: EthAddress,
        *,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[ExposureBucket]:
        return await self._repository.list_exposure_buckets(
            prime_address,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            bucket_seconds=bucket_seconds,
            limit=limit,
        )
