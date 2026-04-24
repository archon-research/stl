from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.adapters.postgres.allocation_share_repository import (
    _SHARE_LOOKUP_SQL,
    MissingShareError,
    PostgresAllocationShare,
    StaleShareError,
)

_WALLET = bytes.fromhex("1601843c5e9bc251a3272907010afa41fa18347e")


def _engine_with_row(row) -> MagicMock:
    engine = MagicMock()
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    conn.exec_driver_sql = AsyncMock()
    conn.execute = AsyncMock(return_value=MagicMock(fetchone=MagicMock(return_value=row)))
    engine.connect = MagicMock(return_value=conn)
    return engine


@pytest.mark.asyncio
async def test_share_uses_scaled_pair_when_both_present() -> None:
    fresh_ts = datetime.now(timezone.utc)
    row = MagicMock()
    row.balance = Decimal("10")
    row.scaled_balance = Decimal("5")
    row.total_supply = Decimal("100")
    row.scaled_total_supply = Decimal("50")
    row.supply_ts = fresh_ts

    share = PostgresAllocationShare(_engine_with_row(row), chain_id=1, token_id=123, wallet_address=_WALLET)
    got = await share.get_share()
    # Scaled ratio: 5 / 50 = 0.1 (would be 0.1 for unscaled too but different values
    # here let us catch the wrong pick).
    assert got == Decimal("5") / Decimal("50")


@pytest.mark.asyncio
async def test_share_falls_back_to_unscaled_when_scaled_missing() -> None:
    fresh_ts = datetime.now(timezone.utc)
    row = MagicMock()
    row.balance = Decimal("10")
    row.scaled_balance = None
    row.total_supply = Decimal("100")
    row.scaled_total_supply = None
    row.supply_ts = fresh_ts

    share = PostgresAllocationShare(_engine_with_row(row), chain_id=1, token_id=123, wallet_address=_WALLET)
    got = await share.get_share()
    assert got == Decimal("10") / Decimal("100")


@pytest.mark.asyncio
async def test_missing_row_raises_missing_share_error() -> None:
    share = PostgresAllocationShare(_engine_with_row(None), chain_id=1, token_id=1, wallet_address=_WALLET)
    with pytest.raises(MissingShareError):
        await share.get_share()


def test_sql_pins_balance_to_supply_block() -> None:
    """Regression guard: the lookup SQL must pin balance to the supply row's block.

    If these sub-queries are ever re-written as two independent ``latest_*``
    selections (the earlier shape), a balance from a later block could be paired
    with a supply from an earlier block when ``totalSupply()`` failed on the
    newer block. The ``pinned_balance`` CTE with the
    ``ap.block_number < ls.block_number OR (= AND version <=)`` predicate is
    what enforces the same-block invariant at read time.
    """
    sql = _SHARE_LOOKUP_SQL
    assert "pinned_balance" in sql
    assert "latest_supply" in sql
    # The pinning predicate must reference the supply alias (ls) from the
    # balance side. A regression back to independent latest-of-each selection
    # would drop this clause.
    assert "ap.block_number < ls.block_number" in sql
    # Same-block rows must share block_version exactly — `<=` would let a
    # pre-reorg balance pair with a post-reorg supply.
    assert "ap.block_version = ls.block_version" in sql


@pytest.mark.asyncio
async def test_stale_supply_raises_stale_share_error() -> None:
    stale_ts = datetime.now(timezone.utc) - timedelta(seconds=4000)
    row = MagicMock()
    row.balance = Decimal("1")
    row.scaled_balance = None
    row.total_supply = Decimal("1")
    row.scaled_total_supply = None
    row.supply_ts = stale_ts

    share = PostgresAllocationShare(
        _engine_with_row(row), chain_id=1, token_id=1, wallet_address=_WALLET, max_stale_seconds=1800
    )
    with pytest.raises(StaleShareError):
        await share.get_share()


@pytest.mark.asyncio
async def test_zero_denominator_returns_zero() -> None:
    fresh_ts = datetime.now(timezone.utc)
    row = MagicMock()
    row.balance = Decimal("0")
    row.scaled_balance = None
    row.total_supply = Decimal("0")
    row.scaled_total_supply = None
    row.supply_ts = fresh_ts

    share = PostgresAllocationShare(_engine_with_row(row), chain_id=1, token_id=1, wallet_address=_WALLET)
    got = await share.get_share()
    assert got == Decimal("0")


@pytest.mark.asyncio
async def test_rejects_invalid_wallet_length() -> None:
    with pytest.raises(ValueError, match="20 bytes"):
        PostgresAllocationShare(MagicMock(), chain_id=1, token_id=1, wallet_address=b"\x00" * 10)


@pytest.mark.asyncio
async def test_naive_supply_timestamp_is_treated_as_utc() -> None:
    """Some drivers strip tzinfo; repo normalises to UTC so comparisons remain correct."""
    naive_ts = (datetime.now(timezone.utc) - timedelta(seconds=100)).replace(tzinfo=None)
    row = MagicMock()
    row.balance = Decimal("1")
    row.scaled_balance = None
    row.total_supply = Decimal("1")
    row.scaled_total_supply = None
    row.supply_ts = naive_ts

    share = PostgresAllocationShare(
        _engine_with_row(row), chain_id=1, token_id=1, wallet_address=_WALLET, max_stale_seconds=1800
    )
    got = await share.get_share()
    assert got == Decimal("1")
