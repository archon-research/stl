from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from app.domain.entities.allocation import ChainMetadata, EthAddress, Prime, ProtocolMetadata
from app.domain.prime_registry import alm_proxies_for_prime
from app.services import allocation_service
from app.services.allocation_service import AllocationService
from tests.factories import make_direct_asset_holding, make_receipt_token_position

_VALID_ADDR = EthAddress("0x" + "ab" * 20)


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

    result = await service.list_receipt_token_positions(_VALID_ADDR)

    assert result == [position]
    repo.list_receipt_token_positions.assert_awaited_once_with(_VALID_ADDR)


@pytest.mark.asyncio
async def test_list_receipt_token_positions_returns_empty_for_unknown_prime():
    repo = AsyncMock()
    repo.list_receipt_token_positions.return_value = []
    service = AllocationService(repo)

    unknown_addr = EthAddress("0x" + "de" * 20)
    result = await service.list_receipt_token_positions(unknown_addr)

    assert result == []


@pytest.mark.asyncio
async def test_list_direct_asset_holdings_delegates_to_repository():
    repo = AsyncMock()
    holding = make_direct_asset_holding()
    repo.list_direct_asset_holdings.return_value = [holding]
    service = AllocationService(repo)

    result = await service.list_direct_asset_holdings(_VALID_ADDR)

    assert result == [holding]
    repo.list_direct_asset_holdings.assert_awaited_once_with(_VALID_ADDR)


@pytest.mark.asyncio
async def test_prime_exists_delegates_to_repository():
    repo = AsyncMock()
    repo.prime_exists.return_value = True
    service = AllocationService(repo)

    result = await service.prime_exists(_VALID_ADDR)

    assert result is True
    repo.prime_exists.assert_awaited_once_with(_VALID_ADDR)


@pytest.mark.asyncio
async def test_list_allocation_activity_delegates_filters_to_repository():
    repo = AsyncMock()
    repo.list_allocation_activity.return_value = []
    service = AllocationService(repo)

    from_timestamp = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    to_timestamp = datetime(2026, 1, 2, 0, 0, tzinfo=UTC)

    result = await service.list_allocation_activity(
        prime_id=_VALID_ADDR,
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
    # Not a contract proxy, so it filters to itself rather than to everything.
    repo.list_allocation_activity.assert_awaited_once_with(
        proxy_addresses=[_VALID_ADDR],
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
async def test_activity_addressed_by_one_proxy_covers_the_whole_prime():
    # A prime allocates through one proxy per chain, so a prime-wide headline
    # needs every proxy's flows, not just the one the caller happened to use.
    repo = AsyncMock()
    repo.list_activity_buckets.return_value = []
    service = AllocationService(repo)
    proxies = alm_proxies_for_prime("spark")

    await service.list_activity_buckets(
        prime_id=EthAddress(proxies[0].address),
        from_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        to_timestamp=datetime(2026, 1, 2, tzinfo=UTC),
        bucket_seconds=3600,
    )

    passed = repo.list_activity_buckets.await_args.kwargs["proxy_addresses"]
    assert len(passed) == len(proxies) > 1
    assert {str(a) for a in passed} == {entry.address for entry in proxies}


@pytest.mark.asyncio
async def test_a_prime_with_no_alm_proxies_narrows_rather_than_widening():
    # An empty filter reads downstream as no filter, which would serve every
    # prime's activity under one prime's heading.
    repo = AsyncMock()
    repo.list_allocation_activity.return_value = []
    service = AllocationService(repo)

    with (
        patch.object(allocation_service, "prime_name_for", return_value="spark"),
        patch.object(allocation_service, "alm_proxies_for_prime", return_value=()),
    ):
        await service.list_allocation_activity(prime_id=_VALID_ADDR)

    assert repo.list_allocation_activity.await_args.kwargs["proxy_addresses"] == [_VALID_ADDR]


@pytest.mark.asyncio
async def test_activity_without_a_prime_filter_stays_unscoped():
    repo = AsyncMock()
    repo.list_allocation_activity.return_value = []
    service = AllocationService(repo)

    await service.list_allocation_activity()

    assert repo.list_allocation_activity.await_args.kwargs["proxy_addresses"] is None


@pytest.mark.asyncio
async def test_list_total_capital_buckets_delegates_to_repository():
    repo = AsyncMock()
    repo.list_total_capital_buckets.return_value = []
    service = AllocationService(repo)

    from_timestamp = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    to_timestamp = datetime(2026, 1, 2, 0, 0, tzinfo=UTC)

    result = await service.list_total_capital_buckets(
        _VALID_ADDR,
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        bucket_seconds=3600.0,
        limit=50,
    )

    assert result == []
    repo.list_total_capital_buckets.assert_awaited_once_with(
        _VALID_ADDR,
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        bucket_seconds=3600.0,
        limit=50,
    )
