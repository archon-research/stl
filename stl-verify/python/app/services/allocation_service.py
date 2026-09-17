from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from typing import Literal

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
from app.domain.entities.prime import PrimeScope
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

    async def list_primes(self, allowed_vaults: Sequence[EthAddress] | None = None) -> list[Prime]:
        return await self._repository.list_primes(allowed_vaults=allowed_vaults)

    async def list_receipt_token_positions(self, scope: PrimeScope) -> list[ReceiptTokenPosition]:
        """The prime's receipt-token positions, union across its ALM proxies.

        Allocation rows are ADDITIVE (``app.domain.prime_scope``): each proxy
        holds its own, so the prime's set is their union.
        """
        return await self._repository.list_receipt_token_positions(scope.alm_proxies)

    async def list_direct_asset_holdings(self, scope: PrimeScope) -> list[DirectAssetHolding]:
        return await self._repository.list_direct_asset_holdings(scope.alm_proxies)

    async def list_anchorage_custody_holdings(self, scope: PrimeScope) -> list[AnchorageCustodyHolding]:
        """The prime's off-chain custody, read once.

        Custody is SHARED: one figure per prime. Fanning it out over the proxies
        would triple-count $250M of BTC, which is what the retired `scope` field
        and the primary-proxy pick existed to prevent.
        """
        return await self._repository.list_anchorage_custody_holdings(scope.identity.id)

    async def get_total_usd_exposure(self, prime_id: EthAddress) -> Decimal:
        return await self._repository.get_total_usd_exposure(prime_id)

    async def list_allocation_activity(
        self,
        *,
        proxy_addresses: Sequence[EthAddress] | None = None,
        chain_id: int | None = None,
        protocol_name: str | None = None,
        action_type: str | None = None,
        token_symbol: str | None = None,
        tx_hash: str | None = None,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
        limit: int = 100,
        allowed_vaults: Sequence[EthAddress] | None = None,
    ) -> list[AllocationActivityEvent]:
        """The events of the given wallets, or every prime's when unscoped.

        Activity events are ADDITIVE, so the caller resolves a prime to its whole
        ALM proxy set: the feed is their union, never one proxy's fraction under
        a prime-wide headline. ``proxy_addresses`` follows the repository's
        contract — ``None`` is unscoped, ``()`` matches nothing.

        Authorization is part of the query semantics: allowed_vaults travels to
        the repository and lands in the SQL WHERE, before ORDER BY/LIMIT.
        """
        return await self._repository.list_allocation_activity(
            proxy_addresses=proxy_addresses,
            allowed_vaults=allowed_vaults,
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
        proxy_addresses: Sequence[EthAddress] | None = None,
        chain_id: int | None = None,
        protocol_name: str | None = None,
        action_type: str | None = None,
        token_symbol: str | None = None,
        tx_hash: str | None = None,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
        allowed_vaults: Sequence[EthAddress] | None = None,
        series: Literal["flow", "balance"] = "flow",
    ) -> list[AllocationActivityBucket]:
        # Same contract as the raw feed: the allow-list lands in the SQL WHERE,
        # so an aggregate can only ever sum rows the caller may view. That holds
        # for series="balance" too -- the allow-list is inside its window_rows.
        return await self._repository.list_activity_buckets(
            series=series,
            proxy_addresses=proxy_addresses,
            allowed_vaults=allowed_vaults,
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
        scope: PrimeScope,
        *,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[TotalCapitalBucket]:
        """The prime's treasury series, read once over its SubProxy wallets.

        Total capital is SHARED, so the wallet set scopes one read rather than
        being fanned out per wallet and summed — see ``PrimeScope``.
        """
        return await self._repository.list_total_capital_buckets(
            scope.subproxies,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            bucket_seconds=bucket_seconds,
            limit=limit,
        )

    async def list_exposure_buckets(
        self,
        scope: PrimeScope,
        *,
        from_timestamp: datetime,
        to_timestamp: datetime,
        bucket_seconds: float,
        limit: int = 100,
    ) -> list[ExposureBucket]:
        """The prime's priced exposure, summed across the wallets that hold it.

        Exposure is ADDITIVE (see ``PrimeScope``): each ALM proxy holds
        its own positions, so the whole prime's figure is their sum. The sum runs
        in SQL over ``proxy_address``, a segmentby column, rather than one query
        per wallet.
        """
        return await self._repository.list_exposure_buckets(
            scope.alm_proxies,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            bucket_seconds=bucket_seconds,
            limit=limit,
        )
