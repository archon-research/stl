from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from app.domain.entities.protocol_event import ProtocolEvent
from app.services.protocol_event_service import ProtocolEventService


def _event() -> ProtocolEvent:
    return ProtocolEvent(
        tx_hash="0x" + "ab" * 32,
        log_index=0,
        chain_id=1,
        block_number=123,
        block_version=0,
        protocol_name="spark",
        event_name="Borrow",
        contract_address="0x" + "cd" * 20,
        event_data={"amount": "100"},
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_list_events_delegates_filters_and_limit() -> None:
    repo = AsyncMock()
    event = _event()
    repo.list_events.return_value = [event]
    service = ProtocolEventService(repo)

    result = await service.list_events(tx_hash=event.tx_hash, protocol_name="spark", limit=10)

    assert result == [event]
    repo.list_events.assert_awaited_once_with(tx_hash=event.tx_hash, protocol_name="spark", limit=10)


@pytest.mark.asyncio
async def test_get_events_by_tx_delegates() -> None:
    repo = AsyncMock()
    event = _event()
    repo.list_events_by_tx.return_value = [event]
    service = ProtocolEventService(repo)

    result = await service.get_events_by_tx(event.tx_hash)

    assert result == [event]
    repo.list_events_by_tx.assert_awaited_once_with(event.tx_hash)


@pytest.mark.asyncio
async def test_list_events_returns_empty_list() -> None:
    repo = AsyncMock()
    repo.list_events.return_value = []
    service = ProtocolEventService(repo)

    result = await service.list_events(protocol_name="missing")

    assert result == []


@pytest.mark.asyncio
async def test_get_events_by_tx_propagates_repository_error() -> None:
    repo = AsyncMock()
    repo.list_events_by_tx.side_effect = ValueError("db failure")
    service = ProtocolEventService(repo)

    with pytest.raises(ValueError, match="db failure"):
        await service.get_events_by_tx("0x" + "ab" * 32)
