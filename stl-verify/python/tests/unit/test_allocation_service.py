from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import ChainMetadata, EthAddress, Prime, ProtocolMetadata
from app.services.allocation_service import AllocationService
from tests.factories import make_direct_asset_holding, make_prime_scope, make_receipt_token_position

_VALID_ADDR = EthAddress("0x" + "ab" * 20)
_SIBLING_ADDR = EthAddress("0x" + "cd" * 20)


@pytest.mark.asyncio
async def test_list_chains_returns_all_chains():
    repo = AsyncMock()
    repo.list_chains.return_value = [
        ChainMetadata(chain_id=1, name="Ethereum"),
        ChainMetadata(chain_id=10, name="Optimism"),
    ]
    service = AllocationService(repo)

    result = await service.list_chains()

    assert result == [
        ChainMetadata(chain_id=1, name="Ethereum"),
        ChainMetadata(chain_id=10, name="Optimism"),
    ]
    repo.list_chains.assert_awaited_once()


@pytest.mark.asyncio
async def test_list_protocols_returns_all_protocols():
    repo = AsyncMock()
    repo.list_protocols.return_value = [
        ProtocolMetadata(id=1, chain_id=1, encode="aave_v3", name="Aave V3"),
        ProtocolMetadata(id=2, chain_id=1, encode="spark", name="SparkLend"),
    ]
    service = AllocationService(repo)

    result = await service.list_protocols()

    assert result == [
        ProtocolMetadata(id=1, chain_id=1, encode="aave_v3", name="Aave V3"),
        ProtocolMetadata(id=2, chain_id=1, encode="spark", name="SparkLend"),
    ]
    repo.list_protocols.assert_awaited_once()


@pytest.mark.asyncio
async def test_list_primes_returns_all_primes():
    repo = AsyncMock()
    repo.list_primes.return_value = [
        Prime(id="0xaaa", name="grove", address="0xaaa", chain_id=1, chain=None, role="alm"),
        Prime(id="0xbbb", name="spark", address="0xbbb", chain_id=1, chain=None, role="alm"),
    ]
    service = AllocationService(repo)

    result = await service.list_primes()

    assert result == [
        Prime(id="0xaaa", name="grove", address="0xaaa", chain_id=1, chain=None, role="alm"),
        Prime(id="0xbbb", name="spark", address="0xbbb", chain_id=1, chain=None, role="alm"),
    ]
    repo.list_primes.assert_awaited_once()


@pytest.mark.asyncio
async def test_list_receipt_token_positions_delegates_to_repository():
    repo = AsyncMock()
    position = make_receipt_token_position()
    repo.list_receipt_token_positions.return_value = [position]
    service = AllocationService(repo)

    scope = make_prime_scope()
    result = await service.list_receipt_token_positions(scope)

    assert result == [position]
    repo.list_receipt_token_positions.assert_awaited_once_with(scope.alm_proxies)


@pytest.mark.asyncio
async def test_list_receipt_token_positions_returns_empty_for_unknown_prime():
    repo = AsyncMock()
    repo.list_receipt_token_positions.return_value = []
    service = AllocationService(repo)

    result = await service.list_receipt_token_positions(make_prime_scope(wallets=()))

    assert result == []


@pytest.mark.asyncio
async def test_list_direct_asset_holdings_delegates_to_repository():
    repo = AsyncMock()
    holding = make_direct_asset_holding()
    repo.list_direct_asset_holdings.return_value = [holding]
    service = AllocationService(repo)

    scope = make_prime_scope()
    result = await service.list_direct_asset_holdings(scope)

    assert result == [holding]
    repo.list_direct_asset_holdings.assert_awaited_once_with(scope.alm_proxies)


@pytest.mark.asyncio
async def test_anchorage_custody_is_read_once_on_the_resolved_prime():
    """Custody is SHARED: keyed on the prime, never fanned out over its proxies."""
    repo = AsyncMock()
    repo.list_anchorage_custody_holdings.return_value = []
    service = AllocationService(repo)

    scope = make_prime_scope()
    await service.list_anchorage_custody_holdings(scope)

    repo.list_anchorage_custody_holdings.assert_awaited_once_with(scope.identity.id)


@pytest.mark.asyncio
async def test_list_allocation_activity_delegates_filters_to_repository():
    repo = AsyncMock()
    repo.list_allocation_activity.return_value = []
    service = AllocationService(repo)
    scope = make_prime_scope()

    from_timestamp = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    to_timestamp = datetime(2026, 1, 2, 0, 0, tzinfo=UTC)

    result = await service.list_allocation_activity(
        proxy_addresses=scope.alm_proxies,
        chain_id=1,
        protocol_name="aave",
        action_type="in",
        token_symbol="USDC",
        tx_hash="0x" + "ab" * 32,
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        limit=50,
    )

    assert result == []
    # Scoped to the prime's whole ALM proxy set, resolved once at the boundary.
    repo.list_allocation_activity.assert_awaited_once_with(
        proxy_addresses=scope.alm_proxies,
        allowed_vaults=None,
        chain_id=1,
        protocol_name="aave",
        action_type="in",
        token_symbol="USDC",
        tx_hash="0x" + "ab" * 32,
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        limit=50,
    )


@pytest.mark.asyncio
async def test_list_total_capital_buckets_delegates_to_repository():
    repo = AsyncMock()
    repo.list_total_capital_buckets.return_value = []
    service = AllocationService(repo)

    from_timestamp = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    to_timestamp = datetime(2026, 1, 2, 0, 0, tzinfo=UTC)

    scope = make_prime_scope()

    result = await service.list_total_capital_buckets(
        scope,
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        bucket_seconds=3600.0,
        limit=50,
    )

    assert result == []
    repo.list_total_capital_buckets.assert_awaited_once_with(
        scope.subproxies,
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        bucket_seconds=3600.0,
        limit=50,
    )
