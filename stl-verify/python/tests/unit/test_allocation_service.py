from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.allocation import ChainMetadata, EthAddress, Prime, ProtocolMetadata
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
        Prime(id="0xaaa", name="grove", address="0xaaa"),
        Prime(id="0xbbb", name="spark", address="0xbbb"),
    ]
    service = AllocationService(repo)

    result = await service.list_primes()

    assert result == [
        Prime(id="0xaaa", name="grove", address="0xaaa"),
        Prime(id="0xbbb", name="spark", address="0xbbb"),
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
    repo.list_allocation_activity.assert_awaited_once_with(
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
