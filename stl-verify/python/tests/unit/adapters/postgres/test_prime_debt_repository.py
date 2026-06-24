from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.adapters.postgres.prime_debt_repository import PrimeDebtRepository
from app.domain.entities.allocation import EthAddress

_VALID_ADDR = EthAddress("0x" + "ab" * 20)


def _engine_with_rows(rows):
    engine = MagicMock()
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(return_value=MagicMock(fetchall=MagicMock(return_value=rows)))
    engine.connect.return_value = conn
    return engine, conn


def _engine_with_row(row):
    engine = MagicMock()
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(return_value=MagicMock(fetchone=MagicMock(return_value=row)))
    engine.connect.return_value = conn
    return engine, conn


def test_prime_match_clause_keeps_vault_or_proxy_resolution() -> None:
    clause = PrimeDebtRepository._prime_match_clause()

    assert "p.vault_address" in clause
    assert "allocation_position ap" in clause
    assert "ap.proxy_address" in clause


@pytest.mark.asyncio
async def test_prime_exists_returns_true_and_false() -> None:
    engine_true, _ = _engine_with_row(SimpleNamespace(one=1))
    repo_true = PrimeDebtRepository(engine_true)
    assert await repo_true.prime_exists(_VALID_ADDR) is True

    engine_false, _ = _engine_with_row(None)
    repo_false = PrimeDebtRepository(engine_false)
    assert await repo_false.prime_exists(_VALID_ADDR) is False


@pytest.mark.asyncio
async def test_list_debt_snapshots_maps_rows_and_clamps_limit() -> None:
    rows = [
        SimpleNamespace(
            prime_address="ab" * 20,
            prime_name="spark",
            ilk_name="ETH-A",
            debt_wad=Decimal("100.5"),
            block_number=123,
            block_version=0,
            synced_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    ]
    engine, conn = _engine_with_rows(rows)
    repo = PrimeDebtRepository(engine)

    from_ts = datetime(2026, 1, 1, tzinfo=UTC)
    to_ts = datetime(2026, 1, 2, tzinfo=UTC)

    result = await repo.list_debt_snapshots(_VALID_ADDR, from_timestamp=from_ts, to_timestamp=to_ts, limit=9999)

    assert len(result) == 1
    assert result[0].prime_address == "0x" + "ab" * 20
    call_params = conn.execute.await_args.args[1]
    assert call_params["limit"] == 500
    assert call_params["from_timestamp"] == from_ts
    assert call_params["to_timestamp"] == to_ts


@pytest.mark.asyncio
async def test_list_debt_snapshots_wraps_database_errors() -> None:
    engine = MagicMock()
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(side_effect=RuntimeError("boom"))
    engine.connect.return_value = conn

    repo = PrimeDebtRepository(engine)

    with pytest.raises(ValueError, match="fetching debt snapshots"):
        await repo.list_debt_snapshots(_VALID_ADDR)
