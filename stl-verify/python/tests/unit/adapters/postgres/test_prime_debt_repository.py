from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.adapters.postgres.prime_debt_repository import PrimeDebtRepository
from app.domain.entities.allocation import EthAddress

_VALID_ADDR = EthAddress("0x" + "ab" * 20)
_PRIME_ID = 7


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
async def test_resolve_prime_id_returns_the_matched_id_or_none() -> None:
    engine_found, _ = _engine_with_row(SimpleNamespace(id=_PRIME_ID))
    repo_found = PrimeDebtRepository(engine_found)
    assert await repo_found.resolve_prime_id(_VALID_ADDR) == _PRIME_ID

    engine_missing, _ = _engine_with_row(None)
    repo_missing = PrimeDebtRepository(engine_missing)
    assert await repo_missing.resolve_prime_id(_VALID_ADDR) is None


@pytest.mark.asyncio
async def test_list_queries_filter_by_prime_id_without_a_correlated_exists() -> None:
    # The match clause used to be inlined here, so its EXISTS re-scanned every allocation_position chunk.
    engine, conn = _engine_with_rows([])
    repo = PrimeDebtRepository(engine)

    await repo.list_debt_snapshots(_PRIME_ID)
    await repo.list_debt_buckets(
        _PRIME_ID,
        from_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        to_timestamp=datetime(2026, 1, 2, tzinfo=UTC),
        bucket_seconds=300.0,
    )

    assert len(conn.execute.await_args_list) == 2
    for call in conn.execute.await_args_list:
        statement, params = call.args
        assert "pd.prime_id = :prime_id" in str(statement)
        assert "EXISTS" not in str(statement)
        assert params["prime_id"] == _PRIME_ID


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

    result = await repo.list_debt_snapshots(_PRIME_ID, from_timestamp=from_ts, to_timestamp=to_ts, limit=9999)

    assert len(result) == 1
    assert result[0].prime_address == "0x" + "ab" * 20
    call_params = conn.execute.await_args.args[1]
    assert call_params["limit"] == 500
    assert call_params["from_timestamp"] == from_ts
    assert call_params["to_timestamp"] == to_ts


@pytest.mark.asyncio
async def test_list_reference_debt_buckets_filters_by_prime_id_without_a_correlated_exists() -> None:
    engine, conn = _engine_with_rows([])
    repo = PrimeDebtRepository(engine)

    await repo.list_reference_debt_buckets(
        _PRIME_ID,
        from_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        to_timestamp=datetime(2026, 1, 2, tzinfo=UTC),
        bucket_seconds=300.0,
    )

    statement, params = conn.execute.await_args.args
    assert "b.prime_id = :prime_id" in str(statement)
    assert "EXISTS" not in str(statement)
    assert params["prime_id"] == _PRIME_ID


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
        await repo.list_debt_snapshots(_PRIME_ID)
