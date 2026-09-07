"""Row mapping for the stored balance-sheet reader.

The SQL's behaviour against a real database — the coverage gate, latest-cycle
selection, the registry join — is covered by the integration suite.
"""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.adapters.postgres.reference_position_repository import (
    _POSITIONS_SQL,
    ReferencePositionRepository,
)

_SYNCED_AT = datetime(2026, 8, 26, 9, 15, tzinfo=UTC)


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
            "wallet_address": "0x" + "ef" * 20,
            "assets_usd": Decimal("787379142.91"),
            "allocated_assets_usd": Decimal("787000000.00"),
            "idle_assets_usd": Decimal("379142.91"),
            "receipt_token_id": 41,
            "underlying_token_id": 7,
            "underlying_token_address": "cd" * 20,
            "underlying_symbol": "USDS",
            **overrides,
        }
    )


def _reader(stub_engine, *rows, error: Exception | None = None):
    engine, _ = stub_engine({"fetchall.return_value": list(rows)}, error=error)
    return ReferencePositionRepository(engine)


@pytest.mark.asyncio
async def test_returns_none_when_nothing_has_been_observed(stub_engine) -> None:
    # Zero rows means the prime is uncovered or its positions have never
    # landed. Neither may be served as a prime holding nothing.
    assert await _reader(stub_engine).get_positions("obex") is None


@pytest.mark.asyncio
async def test_maps_a_position_with_its_resolved_registry_id(stub_engine) -> None:
    snapshot = await _reader(stub_engine, _row()).get_positions("spark")

    assert snapshot is not None
    (position,) = snapshot.positions
    assert position.symbol == "spUSDS"
    assert position.assets_usd == Decimal("787379142.91")
    assert position.receipt_token_id == 41
    assert position.chain == "mainnet"
    assert position.wallet_address == "0x" + "ef" * 20
    assert snapshot.synced_at == _SYNCED_AT


@pytest.mark.asyncio
async def test_a_resolved_position_carries_the_registrys_underlying(stub_engine) -> None:
    # Sky's feed names no underlying itself; a resolved receipt token
    # contributes one from STL's registry, the same join that resolves
    # `receipt_token_id`.
    snapshot = await _reader(stub_engine, _row()).get_positions("spark")

    assert snapshot is not None
    (position,) = snapshot.positions
    assert position.underlying_token_id == 7
    assert position.underlying_token_address == "0x" + "cd" * 20
    assert position.underlying_symbol == "USDS"


@pytest.mark.asyncio
async def test_keeps_a_position_the_registry_join_missed(stub_engine) -> None:
    # Most of this feed is positions STL has no registry entry for; an
    # unresolved id must not drop the row.
    snapshot = await _reader(
        stub_engine,
        _row(receipt_token_id=None, underlying_token_id=None, underlying_token_address=None, underlying_symbol=None),
    ).get_positions("spark")

    assert snapshot is not None
    (position,) = snapshot.positions
    assert position.receipt_token_id is None
    assert position.underlying_token_id is None
    assert position.underlying_token_address is None
    assert position.underlying_symbol == ""


@pytest.mark.asyncio
async def test_leaves_an_unmapped_network_without_a_chain(stub_engine) -> None:
    snapshot = await _reader(
        stub_engine,
        _row(
            network="plume",
            chain_id=None,
            receipt_token_id=None,
            underlying_token_id=None,
            underlying_token_address=None,
            underlying_symbol=None,
        ),
    ).get_positions("spark")

    assert snapshot is not None
    assert snapshot.positions[0].chain is None
    assert snapshot.positions[0].network == "plume"


@pytest.mark.asyncio
async def test_keeps_an_omitted_decomposition_distinct_from_zero(stub_engine) -> None:
    snapshot = await _reader(stub_engine, _row(allocated_assets_usd=None, idle_assets_usd=None)).get_positions("spark")

    assert snapshot is not None
    assert snapshot.positions[0].allocated_assets_usd is None
    assert snapshot.positions[0].idle_assets_usd is None


@pytest.mark.asyncio
async def test_reads_a_label_upstream_omitted_as_empty(stub_engine) -> None:
    snapshot = await _reader(stub_engine, _row(token_name=None)).get_positions("spark")

    assert snapshot is not None
    assert snapshot.positions[0].name == ""


@pytest.mark.asyncio
async def test_wraps_a_database_failure_rather_than_reporting_no_coverage(stub_engine) -> None:
    # A read that failed says nothing about coverage, so it must not surface as
    # `None`, which the API serves as a 404 "not covered".
    with pytest.raises(ValueError, match="reading the reference positions"):
        await _reader(stub_engine, error=RuntimeError("boom")).get_positions("spark")


def test_coverage_requires_the_risk_capital_table_too() -> None:
    # Reading coverage from this table alone would let the allocation list and
    # the risk-capital card disagree about whether a prime is covered.
    assert "prime_capital_stack" in str(_POSITIONS_SQL)


def test_the_snapshot_ordering_never_selects_on_build_id() -> None:
    # build_id spans many cycles and appears in no unique constraint, so
    # ordering by it picks an arbitrary row. A behavioural test cannot catch
    # this: seeded corrections happen to agree with build_id order.
    assert "build_id" not in str(_POSITIONS_SQL)


def test_the_latest_cycle_selection_keys_on_wallet_address_too() -> None:
    # Grove legitimately reports the same (network, token_address) under two
    # proxy wallets. A DISTINCT ON that dropped wallet_address would collapse
    # them into one row, silently discarding a real position (integration
    # coverage: test_positions_serve_both_wallets_for_the_same_token).
    assert "p.wallet_address" in str(_POSITIONS_SQL)
