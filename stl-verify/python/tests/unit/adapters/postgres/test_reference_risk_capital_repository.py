"""SQL shape and row mapping for the stored risk-capital snapshot reader.

The behaviour against a real database — the DISTINCT ON, the registry join, the
latest-cycle selection — is covered by the integration suite. These cover the
mapping and the invariants the SQL text must keep.
"""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.adapters.postgres.reference_risk_capital_repository import (
    _ALLOCATIONS_SQL,
    _TOTALS_SQL,
    ReferenceRiskCapitalRepository,
)

_SYNCED_AT = datetime(2026, 8, 26, 9, 15, tzinfo=UTC)


def _engine(*results):
    """An engine whose connection answers each execute with the next result."""
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(side_effect=[MagicMock(**result) for result in results])
    engine = MagicMock()
    engine.connect.return_value = conn
    return engine, conn


def _totals_row(**overrides) -> SimpleNamespace:
    figures = dict.fromkeys(
        (
            "exposure_share",
            "junior_risk_capital_usd",
            "senior_risk_capital_usd",
            "internal_junior_risk_capital_usd",
            "external_junior_risk_capital_usd",
            "tokenized_junior_risk_capital_usd",
            "internal_senior_risk_capital_usd",
            "external_senior_risk_capital_usd",
            "epi_utilization",
            "spj_utilization",
        ),
        Decimal("0"),
    )
    return SimpleNamespace(
        **{
            "synced_at": _SYNCED_AT,
            "exposure_usd": Decimal("2098090654.81"),
            "required_risk_capital_usd": Decimal("17837860.43"),
            "total_risk_capital_usd": Decimal("48142491.08"),
            "encumbrance_ratio": Decimal("0.3705"),
            **figures,
            **overrides,
        }
    )


def _allocation_row(**overrides) -> SimpleNamespace:
    return SimpleNamespace(
        **{
            "network": "ethereum",
            "chain_id": 1,
            "protocol_name": "sparklend",
            "symbol": "spUSDT",
            "name": "Spark USDT",
            "token_address": "0x" + "cd" * 20,
            "loan_token_address": "0x" + "12" * 20,
            "loan_token_symbol": "USDT",
            "exposure_usd": Decimal("344187505.66"),
            "required_risk_capital_usd": Decimal("990048.94"),
            "crr": Decimal("0.0028764051"),
            "receipt_token_id": 41,
            **overrides,
        }
    )


@pytest.mark.asyncio
async def test_get_prime_returns_none_when_no_cycle_has_reported_on_the_prime() -> None:
    engine, conn = _engine({"fetchone.return_value": None})
    repository = ReferenceRiskCapitalRepository(engine)

    assert await repository.get_prime("obex") is None
    # The breakdown is not asked for: without a totals row there is no cycle to
    # pin it to, so a second query could only pick an unrelated one.
    assert len(conn.execute.await_args_list) == 1


@pytest.mark.asyncio
async def test_get_prime_pins_the_breakdown_to_the_totals_row_own_cycle() -> None:
    # Re-deriving "latest" for the breakdown would pair one instant's totals
    # with another's rows whenever a cycle lands between the two statements.
    engine, conn = _engine(
        {"fetchone.return_value": _totals_row()},
        {"fetchall.return_value": [_allocation_row()]},
    )
    repository = ReferenceRiskCapitalRepository(engine)

    await repository.get_prime("spark")

    _, params = conn.execute.await_args_list[1].args
    assert params == {"star": "spark", "synced_at": _SYNCED_AT}


@pytest.mark.asyncio
async def test_get_prime_rescales_the_stored_fraction_into_a_percentage() -> None:
    # The column is upstream's own 0-1 crr; every consumer reads 0-100.
    engine, _ = _engine(
        {"fetchone.return_value": _totals_row()},
        {"fetchall.return_value": [_allocation_row(crr=Decimal("0.0028764051"))]},
    )
    repository = ReferenceRiskCapitalRepository(engine)

    snapshot = await repository.get_prime("spark")

    assert snapshot is not None
    assert snapshot.per_allocation[0].crr_pct == Decimal("0.28764051")


@pytest.mark.asyncio
async def test_get_prime_names_the_chain_for_a_mapped_network() -> None:
    engine, _ = _engine(
        {"fetchone.return_value": _totals_row()},
        {"fetchall.return_value": [_allocation_row()]},
    )
    repository = ReferenceRiskCapitalRepository(engine)

    snapshot = await repository.get_prime("spark")

    assert snapshot is not None
    assert snapshot.per_allocation[0].chain == "mainnet"


@pytest.mark.asyncio
async def test_get_prime_leaves_an_unmapped_network_without_a_chain() -> None:
    # Upstream adds chains before STL has an id for them; 0 is unavailable
    # because it already means off-chain custody.
    engine, _ = _engine(
        {"fetchone.return_value": _totals_row()},
        {"fetchall.return_value": [_allocation_row(network="plume", chain_id=None, receipt_token_id=None)]},
    )
    repository = ReferenceRiskCapitalRepository(engine)

    snapshot = await repository.get_prime("spark")

    assert snapshot is not None
    assert snapshot.per_allocation[0].chain is None
    assert snapshot.per_allocation[0].chain_id is None


@pytest.mark.asyncio
async def test_get_prime_reads_a_label_upstream_omitted_as_empty() -> None:
    # The columns are nullable where upstream omits a display label, and the
    # entity types them `str`; empty and absent mean the same thing for a label.
    engine, _ = _engine(
        {"fetchone.return_value": _totals_row()},
        {"fetchall.return_value": [_allocation_row(name=None, loan_token_address=None, loan_token_symbol=None)]},
    )
    repository = ReferenceRiskCapitalRepository(engine)

    snapshot = await repository.get_prime("spark")

    assert snapshot is not None
    row = snapshot.per_allocation[0]
    assert (row.name, row.loan_token_address, row.loan_token_symbol) == ("", "", "")


@pytest.mark.asyncio
async def test_get_prime_wraps_a_database_failure_rather_than_reporting_no_coverage() -> None:
    # A read that failed says nothing about coverage, so it must not surface as
    # `None`, which the API serves as a 404 "not covered".
    conn = AsyncMock()
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    conn.execute = AsyncMock(side_effect=RuntimeError("boom"))
    engine = MagicMock()
    engine.connect.return_value = conn

    with pytest.raises(ValueError, match="reading the reference risk-capital snapshot"):
        await ReferenceRiskCapitalRepository(engine).get_prime("spark")


@pytest.mark.asyncio
async def test_covered_stars_lowercases_what_it_reports() -> None:
    engine, _ = _engine({"fetchall.return_value": [SimpleNamespace(star="spark")]})

    assert await ReferenceRiskCapitalRepository(engine).covered_stars() == frozenset({"spark"})


def test_the_snapshot_ordering_never_selects_on_build_id() -> None:
    # build_id spans many cycles and appears in no unique constraint, so
    # ordering by it picks an arbitrary one and mixes values across cycles.
    for statement in (_TOTALS_SQL, _ALLOCATIONS_SQL):
        assert "build_id" not in str(statement)
    assert "ORDER BY pcs.synced_at DESC, pcs.processing_version DESC" in str(_TOTALS_SQL)
    assert "a.processing_version DESC" in str(_ALLOCATIONS_SQL)


def test_the_breakdown_guards_the_registry_join_against_a_non_address() -> None:
    # A Uniswap V4 row carries a 32-byte pool id where an address is expected,
    # and `decode` raises on anything that is not hex.
    assert "CASE WHEN a.token_address ~ '^0[xX][0-9a-fA-F]{40}$'" in str(_ALLOCATIONS_SQL)
