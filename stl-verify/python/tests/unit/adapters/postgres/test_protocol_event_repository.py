from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.adapters.postgres.protocol_event_repository import ProtocolEventRepository


def _engine_with_rows(rows):
    engine = MagicMock()
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(return_value=MagicMock(fetchall=MagicMock(return_value=rows)))
    engine.connect.return_value = conn
    return engine, conn


def _row(tx_hash: str = "ab" * 32) -> SimpleNamespace:
    return SimpleNamespace(
        tx_hash=tx_hash,
        log_index=1,
        chain_id=1,
        block_number=123,
        block_version=0,
        protocol_name="spark",
        event_name="Borrow",
        contract_address="cd" * 20,
        event_data={"amount": "1"},
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


def test_normalize_tx_hash_handles_prefix() -> None:
    assert ProtocolEventRepository._normalize_tx_hash("0x" + "ab" * 32) == "ab" * 32
    assert ProtocolEventRepository._normalize_tx_hash("ab" * 32) == "ab" * 32
    assert ProtocolEventRepository._normalize_tx_hash(None) is None


@pytest.mark.asyncio
async def test_list_events_normalizes_tx_hash_and_clamps_limit() -> None:
    engine, conn = _engine_with_rows([_row()])
    repo = ProtocolEventRepository(engine)

    result = await repo.list_events(tx_hash="0x" + "ab" * 32, protocol_name="spark", limit=9999)

    assert len(result) == 1
    assert result[0].tx_hash == "0x" + "ab" * 32
    params = conn.execute.await_args.args[1]
    assert params["tx_hash"] == "ab" * 32
    assert params["limit"] == 500


@pytest.mark.asyncio
async def test_list_events_by_tx_maps_entities() -> None:
    engine, conn = _engine_with_rows([_row("ef" * 32)])
    repo = ProtocolEventRepository(engine)

    result = await repo.list_events_by_tx("0x" + "ef" * 32)

    assert len(result) == 1
    assert result[0].tx_hash == "0x" + "ef" * 32
    params = conn.execute.await_args.args[1]
    assert params["tx_hash"] == "ef" * 32


@pytest.mark.asyncio
async def test_list_events_wraps_database_errors() -> None:
    engine = MagicMock()
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(side_effect=RuntimeError("boom"))
    engine.connect.return_value = conn
    repo = ProtocolEventRepository(engine)

    with pytest.raises(ValueError, match="fetching protocol events"):
        await repo.list_events()
