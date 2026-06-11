from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.adapters.postgres.allocation_position_repository import (
    _ANCHORAGE_POSITION_SQL,
    PostgresAllocationRepository,
)
from app.domain.entities.allocation import EthAddress

_PRIME = EthAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")


def _engine_with_row(row) -> MagicMock:
    engine = MagicMock()
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(return_value=MagicMock(fetchone=MagicMock(return_value=row)))
    engine.connect = MagicMock(return_value=conn)
    return engine


@pytest.mark.asyncio
async def test_get_anchorage_position_maps_row() -> None:
    ts = datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc)
    row = MagicMock()
    row.chain_id = 1
    row.receipt_token_id = 7
    row.receipt_token_address = "49506c3aa028693458d6ee816b2ec28522946872"
    row.underlying_token_id = 20
    row.underlying_token_address = "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    row.symbol = "ANCHORAGE"
    row.underlying_symbol = "USDC"
    row.protocol_name = "anchorage"
    row.balance = Decimal("250000001.323783")
    row.amount_usd = Decimal("250000001.323783")
    row.latest_activity_at = ts

    repo = PostgresAllocationRepository(_engine_with_row(row))
    pos = await repo.get_anchorage_position(_PRIME)

    assert pos is not None
    assert pos.receipt_token_address == "0x49506c3aa028693458d6ee816b2ec28522946872"
    assert pos.underlying_token_address == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    assert pos.symbol == "ANCHORAGE"
    assert pos.underlying_symbol == "USDC"
    assert pos.protocol_name == "anchorage"
    assert pos.balance == Decimal("250000001.323783")
    assert pos.amount_usd == Decimal("250000001.323783")
    assert pos.latest_activity_at == ts


@pytest.mark.asyncio
async def test_get_anchorage_position_returns_none_when_no_row() -> None:
    repo = PostgresAllocationRepository(_engine_with_row(None))
    assert await repo.get_anchorage_position(_PRIME) is None


@pytest.mark.asyncio
async def test_get_anchorage_position_returns_none_when_balance_null() -> None:
    row = MagicMock()
    row.balance = None
    repo = PostgresAllocationRepository(_engine_with_row(row))
    assert await repo.get_anchorage_position(_PRIME) is None


def test_anchorage_sql_uses_latest_active_snapshot_aggregation() -> None:
    """Regression guard for the documented aggregation choices."""
    sql = str(_ANCHORAGE_POSITION_SQL)
    # Latest snapshot per (package, asset, custody), newest processing_version wins.
    assert "DISTINCT ON (s.package_id, s.asset_type, s.custody_type)" in sql
    assert "s.snapshot_time DESC, s.processing_version DESC" in sql
    # Active packages only.
    assert "s.active = TRUE" in sql
    # balance = SUM(asset_quantity); amount_usd = SUM(asset_quantity * asset_price),
    # NOT asset_weighted_value (which is an LTV-haircut figure).
    assert "SUM(asset_quantity)" in sql
    assert "SUM(asset_quantity * asset_price)" in sql
    assert "asset_weighted_value" not in sql
    # Prime resolved from proxy address via allocation_position.
    assert "FROM allocation_position" in sql
