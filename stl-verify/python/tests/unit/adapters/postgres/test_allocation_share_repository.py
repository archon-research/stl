from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.adapters.postgres.allocation_share_repository import (
    _BATCH_SHARE_LOOKUP_SQL,
    _ShareRequest,
    batch_fetch_shares,
    fetch_share,
)
from app.domain.exceptions import MissingShareError, StaleShareError

_WALLET = bytes.fromhex("1601843c5e9bc251a3272907010afa41fa18347e")


def _engine_with_rows(rows: list) -> tuple[MagicMock, AsyncMock]:
    """Build an engine whose connection returns ``rows`` from ``fetchall``."""
    engine = MagicMock()
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    conn.exec_driver_sql = AsyncMock()
    conn.execute = AsyncMock(return_value=MagicMock(fetchall=MagicMock(return_value=rows)))
    engine.connect = MagicMock(return_value=conn)
    return engine, conn


def _row(
    *,
    chain_id: int,
    token_id: int,
    balance,
    scaled_balance,
    total_supply,
    scaled_total_supply,
    supply_ts,
) -> MagicMock:
    row = MagicMock()
    row.chain_id = chain_id
    row.token_id = token_id
    row.balance = balance
    row.scaled_balance = scaled_balance
    row.total_supply = total_supply
    row.scaled_total_supply = scaled_total_supply
    row.supply_ts = supply_ts
    return row


# ----------------------------------------------------------------------
# batch_fetch_shares
# ----------------------------------------------------------------------


def test_batch_sql_pins_balance_to_supply_block() -> None:
    """Regression guard: the lookup SQL must pin balance to the supply row's block.

    If the lateral sub-queries are ever re-written as two independent
    ``latest_*`` selections (an older shape), a balance from a later block
    could be paired with a supply from an earlier block when
    ``totalSupply()`` failed on the newer block. The
    ``ap.block_number < ls.block_number OR (= AND version =)`` predicate is
    what enforces the same-block invariant at read time. The 14d
    ``created_at`` bound is what keeps TimescaleDB chunk-pruning at plan time
    — dropping it re-introduces the full-hypertable scan that motivated this
    PR.
    """
    sql = _BATCH_SHARE_LOOKUP_SQL
    assert "LATERAL" in sql
    assert "unnest(CAST(:chain_ids AS int[]))" in sql
    assert "unnest(CAST(:token_ids AS bigint[]))" in sql
    assert "ap.block_number < ls.block_number" in sql
    # Same-block rows must share block_version exactly — `<=` would let a
    # pre-reorg balance pair with a post-reorg supply.
    assert "ap.block_version = ls.block_version" in sql
    assert "ap.created_at >= NOW() - INTERVAL '14 days'" in sql
    # Wallet bind must stay scalar (one prime per request).
    assert ":wallet" in sql


@pytest.mark.asyncio
async def test_batch_uses_scaled_pair_when_both_present() -> None:
    """Per-pair: prefer the scaled balance / scaled supply (rebase-immune)."""
    rows = [
        _row(
            chain_id=1,
            token_id=10,
            balance=Decimal("10"),
            scaled_balance=Decimal("5"),
            total_supply=Decimal("100"),
            scaled_total_supply=Decimal("50"),
            supply_ts=datetime.now(timezone.utc),
        ),
    ]
    engine, _ = _engine_with_rows(rows)

    out = await batch_fetch_shares(engine=engine, requests=[_ShareRequest(1, 10)], wallet_address=_WALLET)

    # 5 / 50 (scaled), not 10 / 100 (unscaled) — the values differ so the
    # wrong pick would be detected.
    assert out[(1, 10)] == Decimal("5") / Decimal("50")


@pytest.mark.asyncio
async def test_batch_zero_denominator_returns_zero() -> None:
    """A zero ``total_supply`` is logged and silently returns ``share=0``."""
    rows = [
        _row(
            chain_id=1,
            token_id=10,
            balance=Decimal("0"),
            scaled_balance=None,
            total_supply=Decimal("0"),
            scaled_total_supply=None,
            supply_ts=datetime.now(timezone.utc),
        ),
    ]
    engine, _ = _engine_with_rows(rows)

    out = await batch_fetch_shares(engine=engine, requests=[_ShareRequest(1, 10)], wallet_address=_WALLET)
    assert out[(1, 10)] == Decimal("0")


@pytest.mark.asyncio
async def test_batch_naive_supply_timestamp_is_treated_as_utc() -> None:
    """Some drivers strip tzinfo; the resolver normalises so age comparisons stay well-defined."""
    naive = (datetime.now(timezone.utc) - timedelta(seconds=100)).replace(tzinfo=None)
    rows = [
        _row(
            chain_id=1,
            token_id=10,
            balance=Decimal("1"),
            scaled_balance=None,
            total_supply=Decimal("1"),
            scaled_total_supply=None,
            supply_ts=naive,
        ),
    ]
    engine, _ = _engine_with_rows(rows)

    out = await batch_fetch_shares(
        engine=engine, requests=[_ShareRequest(1, 10)], wallet_address=_WALLET, max_stale_seconds=1800
    )
    assert out[(1, 10)] == Decimal("1")


@pytest.mark.asyncio
async def test_batch_returns_share_per_pair_and_uses_one_round_trip() -> None:
    fresh = datetime.now(timezone.utc)
    rows = [
        _row(
            chain_id=1,
            token_id=10,
            balance=Decimal("5"),
            scaled_balance=None,
            total_supply=Decimal("100"),
            scaled_total_supply=None,
            supply_ts=fresh,
        ),
        _row(
            chain_id=1,
            token_id=20,
            balance=Decimal("1"),
            scaled_balance=Decimal("3"),
            total_supply=Decimal("4"),
            scaled_total_supply=Decimal("12"),
            supply_ts=fresh,
        ),
    ]
    engine, conn = _engine_with_rows(rows)

    out = await batch_fetch_shares(
        engine=engine,
        requests=[_ShareRequest(1, 10), _ShareRequest(1, 20)],
        wallet_address=_WALLET,
    )

    # Single DB round-trip for the batch.
    assert conn.execute.await_count == 1

    assert out[(1, 10)] == Decimal("5") / Decimal("100")
    # Prefers scaled pair when both sides have scaled values.
    assert out[(1, 20)] == Decimal("3") / Decimal("12")


@pytest.mark.asyncio
async def test_batch_returns_missing_share_error_value_for_unmatched_pair() -> None:
    """Per-pair failure must not poison sibling results.

    The LEFT JOIN returns NULLs for the balance/supply columns when there's
    no matching row; the resolver maps that to ``MissingShareError`` as a
    *value*, so a single missing pair never raises and aborts the batch.
    """
    fresh = datetime.now(timezone.utc)
    rows = [
        _row(
            chain_id=1,
            token_id=10,
            balance=Decimal("5"),
            scaled_balance=None,
            total_supply=Decimal("100"),
            scaled_total_supply=None,
            supply_ts=fresh,
        ),
        _row(
            chain_id=1,
            token_id=20,
            balance=None,
            scaled_balance=None,
            total_supply=None,
            scaled_total_supply=None,
            supply_ts=None,
        ),
    ]
    engine, _ = _engine_with_rows(rows)

    out = await batch_fetch_shares(
        engine=engine,
        requests=[_ShareRequest(1, 10), _ShareRequest(1, 20)],
        wallet_address=_WALLET,
    )

    assert out[(1, 10)] == Decimal("5") / Decimal("100")
    assert isinstance(out[(1, 20)], MissingShareError)


@pytest.mark.asyncio
async def test_batch_returns_stale_share_error_value_for_stale_supply() -> None:
    stale = datetime.now(timezone.utc) - timedelta(seconds=4000)
    rows = [
        _row(
            chain_id=1,
            token_id=10,
            balance=Decimal("1"),
            scaled_balance=None,
            total_supply=Decimal("1"),
            scaled_total_supply=None,
            supply_ts=stale,
        )
    ]
    engine, _ = _engine_with_rows(rows)

    out = await batch_fetch_shares(
        engine=engine,
        requests=[_ShareRequest(1, 10)],
        wallet_address=_WALLET,
        max_stale_seconds=1800,
    )
    assert isinstance(out[(1, 10)], StaleShareError)


@pytest.mark.asyncio
async def test_batch_short_circuits_on_empty_input() -> None:
    """An empty request list must not even open a connection.

    Guards against accidentally paying for the round-trip when a caller has
    nothing to ask for (e.g. a prime with no crypto-lending positions).
    """
    engine, conn = _engine_with_rows([])
    out = await batch_fetch_shares(engine=engine, requests=[], wallet_address=_WALLET)
    assert out == {}
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_batch_deduplicates_requested_pairs() -> None:
    fresh = datetime.now(timezone.utc)
    rows = [
        _row(
            chain_id=1,
            token_id=10,
            balance=Decimal("1"),
            scaled_balance=None,
            total_supply=Decimal("2"),
            scaled_total_supply=None,
            supply_ts=fresh,
        )
    ]
    engine, conn = _engine_with_rows(rows)

    # Duplicate request for the same (chain_id, token_id) must not cause the
    # DB to be asked twice for the same answer.
    await batch_fetch_shares(
        engine=engine,
        requests=[_ShareRequest(1, 10), _ShareRequest(1, 10)],
        wallet_address=_WALLET,
    )
    bound = conn.execute.await_args.args[1]
    assert bound["chain_ids"] == [1]
    assert bound["token_ids"] == [10]


@pytest.mark.asyncio
async def test_batch_rejects_invalid_wallet_length() -> None:
    engine, _ = _engine_with_rows([])
    with pytest.raises(ValueError, match="20 bytes"):
        await batch_fetch_shares(
            engine=engine,
            requests=[_ShareRequest(1, 1)],
            wallet_address=b"\x00" * 10,
        )


# ----------------------------------------------------------------------
# fetch_share
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_share_returns_decimal_on_hit() -> None:
    """The single-pair wrapper rides on top of ``batch_fetch_shares`` — proves
    the legacy ``get_share`` path still resolves through the same SQL.
    """
    rows = [
        _row(
            chain_id=1,
            token_id=10,
            balance=Decimal("5"),
            scaled_balance=None,
            total_supply=Decimal("100"),
            scaled_total_supply=None,
            supply_ts=datetime.now(timezone.utc),
        )
    ]
    engine, _ = _engine_with_rows(rows)

    got = await fetch_share(engine=engine, chain_id=1, token_id=10, wallet_address=_WALLET)
    assert got == Decimal("5") / Decimal("100")


@pytest.mark.asyncio
async def test_fetch_share_reraises_missing_share_error_as_exception() -> None:
    """Per-pair MissingShareError must surface as a raise, matching the
    legacy ``PostgresAllocationShare.get_share`` contract that the only
    production caller (``crypto_lending_reader._load_share``) still relies on.
    """
    engine, _ = _engine_with_rows([])
    with pytest.raises(MissingShareError):
        await fetch_share(engine=engine, chain_id=1, token_id=10, wallet_address=_WALLET)


@pytest.mark.asyncio
async def test_fetch_share_reraises_stale_share_error_as_exception() -> None:
    rows = [
        _row(
            chain_id=1,
            token_id=10,
            balance=Decimal("1"),
            scaled_balance=None,
            total_supply=Decimal("1"),
            scaled_total_supply=None,
            supply_ts=datetime.now(timezone.utc) - timedelta(seconds=4000),
        )
    ]
    engine, _ = _engine_with_rows(rows)
    with pytest.raises(StaleShareError):
        await fetch_share(engine=engine, chain_id=1, token_id=10, wallet_address=_WALLET, max_stale_seconds=1800)
