"""SQL shape and row mapping for the stored balance-sheet reader.

The behaviour against a real database — the coverage join, the DISTINCT ON, the
registry join — is covered by the integration suite. These cover the mapping and
the invariants the SQL text must keep.
"""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.adapters.postgres.reference_position_repository import (
    _POSITIONS_SQL,
    ReferencePositionRepository,
)

_SYNCED_AT = datetime(2026, 8, 26, 9, 15, tzinfo=UTC)


def _engine_with_rows(rows):
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(return_value=MagicMock(fetchall=MagicMock(return_value=rows)))
    engine = MagicMock()
    engine.connect.return_value = conn
    return engine, conn


def _row(**overrides) -> SimpleNamespace:
    return SimpleNamespace(
        **{
            "synced_at": _SYNCED_AT,
            "network": "ethereum",
            "chain_id": 1,
            "protocol_name": "sparklend",
            "token_symbol": "spUSDS",
            "token_name": "Spark USDS",
            "token_address": "0x" + "cd" * 20,
            "assets_usd": Decimal("787379142.91"),
            "allocated_assets_usd": Decimal("787000000.00"),
            "idle_assets_usd": Decimal("379142.91"),
            "receipt_token_id": 41,
            **overrides,
        }
    )


def _coverage_only_row() -> SimpleNamespace:
    """The null-padded row a covered prime with no positions produces."""
    return _row(
        network=None,
        chain_id=None,
        protocol_name=None,
        token_symbol=None,
        token_name=None,
        token_address=None,
        assets_usd=None,
        allocated_assets_usd=None,
        idle_assets_usd=None,
        receipt_token_id=None,
    )


@pytest.mark.asyncio
async def test_returns_none_when_the_prime_is_not_covered() -> None:
    # No coverage row means no rows at all, which the API serves as a 404.
    engine, _ = _engine_with_rows([])

    assert await ReferencePositionRepository(engine).get_positions("obex") is None


@pytest.mark.asyncio
async def test_a_covered_prime_holding_nothing_is_an_empty_snapshot_not_none() -> None:
    # An empty balance sheet is a claim; `None` means "no reference data at
    # all". Collapsing them would turn the former into a 404.
    engine, _ = _engine_with_rows([_coverage_only_row()])

    snapshot = await ReferencePositionRepository(engine).get_positions("spark")

    assert snapshot is not None
    assert snapshot.positions == ()
    assert snapshot.synced_at == _SYNCED_AT


@pytest.mark.asyncio
async def test_maps_a_position_with_its_resolved_registry_id() -> None:
    engine, _ = _engine_with_rows([_row()])

    snapshot = await ReferencePositionRepository(engine).get_positions("spark")

    assert snapshot is not None
    (position,) = snapshot.positions
    assert position.symbol == "spUSDS"
    assert position.assets_usd == Decimal("787379142.91")
    assert position.receipt_token_id == 41
    assert position.chain == "mainnet"


@pytest.mark.asyncio
async def test_keeps_a_position_the_registry_join_missed() -> None:
    # Most of this feed is positions STL has no registry entry for; an
    # unresolved id must not drop the row.
    engine, _ = _engine_with_rows([_row(receipt_token_id=None)])

    snapshot = await ReferencePositionRepository(engine).get_positions("spark")

    assert snapshot is not None
    assert snapshot.positions[0].receipt_token_id is None


@pytest.mark.asyncio
async def test_leaves_an_unmapped_network_without_a_chain() -> None:
    engine, _ = _engine_with_rows([_row(network="plume", chain_id=None, receipt_token_id=None)])

    snapshot = await ReferencePositionRepository(engine).get_positions("spark")

    assert snapshot is not None
    assert snapshot.positions[0].chain is None
    assert snapshot.positions[0].network == "plume"


@pytest.mark.asyncio
async def test_keeps_an_omitted_decomposition_distinct_from_zero() -> None:
    engine, _ = _engine_with_rows([_row(allocated_assets_usd=None, idle_assets_usd=None)])

    snapshot = await ReferencePositionRepository(engine).get_positions("spark")

    assert snapshot is not None
    assert snapshot.positions[0].allocated_assets_usd is None
    assert snapshot.positions[0].idle_assets_usd is None


@pytest.mark.asyncio
async def test_wraps_a_database_failure_rather_than_reporting_no_coverage() -> None:
    # A read that failed says nothing about coverage, so it must not surface as
    # `None`, which the API serves as a 404 "not covered".
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(side_effect=RuntimeError("boom"))
    engine = MagicMock()
    engine.connect.return_value = conn

    with pytest.raises(ValueError, match="reading the reference positions"):
        await ReferencePositionRepository(engine).get_positions("spark")


def test_coverage_is_read_from_the_risk_capital_table_not_this_one() -> None:
    # Keeping one answer to coverage stops the allocation list and the
    # risk-capital card disagreeing about whether a prime has reference data.
    statement = str(_POSITIONS_SQL)
    assert "FROM prime_capital_stack pcs" in statement
    assert "FROM covered" in statement
    assert "LEFT JOIN latest r ON TRUE" in statement


def test_the_snapshot_ordering_never_selects_on_build_id() -> None:
    statement = str(_POSITIONS_SQL)
    assert "build_id" not in statement
    assert "p.processing_version DESC" in statement


def test_the_registry_join_is_guarded_against_a_non_address() -> None:
    assert "CASE WHEN p.token_address ~ '^0[xX][0-9a-fA-F]{40}$'" in str(_POSITIONS_SQL)
