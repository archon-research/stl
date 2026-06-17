from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.adapters.postgres.capital_metrics_repository import PostgresCapitalMetricsRepository
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


def test_prime_match_clause_keeps_vault_or_proxy_resolution() -> None:
    clause = PostgresCapitalMetricsRepository._prime_match_clause()

    assert "p.vault_address" in clause
    assert "allocation_position ap" in clause
    assert "ap.proxy_address" in clause


@pytest.mark.asyncio
async def test_list_snapshots_maps_rows_and_derives_buffer() -> None:
    rows = [
        SimpleNamespace(
            prime_address="ab" * 20,
            prime_name="spark",
            risk_capital=Decimal("100"),
            total_capital=Decimal("200"),
            first_loss_capital=Decimal("50"),
            risk_to_capital_ratio=Decimal("0.85"),
            benchmark_source="https://example.com/star",
            synced_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
    ]
    engine, conn = _engine_with_rows(rows)
    repo = PostgresCapitalMetricsRepository(engine)

    from_ts = datetime(2026, 1, 1, tzinfo=UTC)
    to_ts = datetime(2026, 1, 2, tzinfo=UTC)
    result = await repo.list_snapshots(_VALID_ADDR, from_timestamp=from_ts, to_timestamp=to_ts, limit=9999)

    assert len(result) == 1
    assert result[0].prime_address == "0x" + "ab" * 20
    assert result[0].capital_buffer == Decimal("150")  # max(200 - 50, 0)
    call_params = conn.execute.await_args.args[1]
    assert call_params["limit"] == 500  # clamped to max


@pytest.mark.asyncio
async def test_list_buckets_derives_buffer_and_handles_gaps() -> None:
    rows = [
        SimpleNamespace(
            bucket_start=datetime(2026, 1, 1, 12, tzinfo=UTC),
            risk_capital=Decimal("100"),
            total_capital=Decimal("200"),
            first_loss_capital=Decimal("50"),
            risk_to_capital_ratio=Decimal("0.85"),
        ),
        SimpleNamespace(
            bucket_start=datetime(2026, 1, 1, 11, tzinfo=UTC),
            risk_capital=None,
            total_capital=None,
            first_loss_capital=None,
            risk_to_capital_ratio=None,
        ),
    ]
    engine, _ = _engine_with_rows(rows)
    repo = PostgresCapitalMetricsRepository(engine)

    result = await repo.list_buckets(
        _VALID_ADDR,
        from_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        to_timestamp=datetime(2026, 1, 2, tzinfo=UTC),
        bucket_seconds=300.0,
    )

    assert len(result) == 2
    assert result[0].capital_buffer == Decimal("150")
    assert result[1].capital_buffer is None  # leading gap-filled bucket
