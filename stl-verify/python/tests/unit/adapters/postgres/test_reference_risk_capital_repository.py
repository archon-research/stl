"""Row mapping for the stored risk-capital snapshot reader.

The SQL's behaviour against a real database — latest-cycle selection, the
DISTINCT ON, the registry join — is covered by the integration suite. These
cover the mapping and the invariants that hold above the SQL.
"""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app.adapters.postgres.reference_risk_capital_repository import (
    _ALLOCATIONS_SQL,
    _TOTALS_SQL,
    ReferenceRiskCapitalRepository,
)

_SYNCED_AT = datetime(2026, 8, 26, 9, 15, tzinfo=UTC)


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


def _reader(stub_engine, *results, error: Exception | None = None):
    engine, conn = stub_engine(*results, error=error)
    return ReferenceRiskCapitalRepository(engine), conn


def _with_snapshot(stub_engine, *rows, totals: SimpleNamespace | None = None):
    return _reader(
        stub_engine,
        {"fetchone.return_value": totals or _totals_row()},
        {"fetchall.return_value": list(rows) or [_allocation_row()]},
    )


@pytest.mark.asyncio
async def test_returns_none_when_no_cycle_has_reported_on_the_prime(stub_engine) -> None:
    repository, conn = _reader(stub_engine, {"fetchone.return_value": None})

    assert await repository.get_prime("obex") is None
    # The breakdown is not asked for: without a totals row there is no cycle to
    # pin it to, so a second query could only pick an unrelated one.
    assert len(conn.execute.await_args_list) == 1


@pytest.mark.asyncio
async def test_pins_the_breakdown_to_the_totals_row_own_cycle(stub_engine) -> None:
    # Re-deriving "latest" for the breakdown would pair one instant's totals
    # with another's rows whenever a cycle lands between the two statements.
    repository, conn = _with_snapshot(stub_engine)

    await repository.get_prime("spark")

    _, params = conn.execute.await_args_list[1].args
    assert params == {"star": "spark", "synced_at": _SYNCED_AT}


@pytest.mark.asyncio
async def test_rescales_the_stored_fraction_into_a_percentage(stub_engine) -> None:
    # The column is upstream's own 0-1 crr; every consumer reads 0-100.
    repository, _ = _with_snapshot(stub_engine, _allocation_row(crr=Decimal("0.0028764051")))

    snapshot = await repository.get_prime("spark")

    assert snapshot is not None
    assert snapshot.per_allocation[0].crr_pct == Decimal("0.28764051")


@pytest.mark.asyncio
async def test_names_the_chain_for_a_mapped_network(stub_engine) -> None:
    repository, _ = _with_snapshot(stub_engine)

    snapshot = await repository.get_prime("spark")

    assert snapshot is not None
    assert snapshot.per_allocation[0].chain == "mainnet"


@pytest.mark.asyncio
async def test_leaves_an_unmapped_network_without_a_chain(stub_engine) -> None:
    # Upstream adds chains before STL has an id for them; 0 is unavailable
    # because it already means off-chain custody.
    repository, _ = _with_snapshot(stub_engine, _allocation_row(network="plume", chain_id=None, receipt_token_id=None))

    snapshot = await repository.get_prime("spark")

    assert snapshot is not None
    assert snapshot.per_allocation[0].chain is None
    assert snapshot.per_allocation[0].chain_id is None


@pytest.mark.asyncio
async def test_reads_a_label_upstream_omitted_as_empty(stub_engine) -> None:
    # The columns are nullable where upstream omits a display label, and the
    # entity types them `str`; empty and absent mean the same for a label.
    repository, _ = _with_snapshot(
        stub_engine, _allocation_row(name=None, loan_token_address=None, loan_token_symbol=None)
    )

    snapshot = await repository.get_prime("spark")

    assert snapshot is not None
    row = snapshot.per_allocation[0]
    assert (row.name, row.loan_token_address, row.loan_token_symbol) == ("", "", "")


@pytest.mark.asyncio
async def test_keeps_an_unobserved_encumbrance_ratio_distinct_from_zero(stub_engine) -> None:
    # The only nullable column among the totals, so the only one that can be
    # served as a real zero by a missing guard.
    repository, _ = _with_snapshot(stub_engine, totals=_totals_row(encumbrance_ratio=None))

    snapshot = await repository.get_prime("spark")

    assert snapshot is not None
    assert snapshot.encumbrance_ratio is None


@pytest.mark.asyncio
async def test_refuses_a_cycle_reporting_exposure_with_no_breakdown_rows(stub_engine) -> None:
    # The indexer writes the totals before the breakdown, in separate
    # transactions, and the totals table predates the breakdown table -- so a
    # readable cycle can have no rows. Serving it publishes "this prime holds
    # nothing" against real exposure, which reads like a real answer.
    repository, _ = _reader(
        stub_engine,
        {"fetchone.return_value": _totals_row(exposure_usd=Decimal("2098090654.81"))},
        {"fetchall.return_value": []},
    )

    with pytest.raises(ValueError, match="landed no per-allocation rows"):
        await repository.get_prime("spark")


@pytest.mark.asyncio
async def test_serves_a_prime_with_no_exposure_and_no_breakdown(stub_engine) -> None:
    # The permitted half of the same shape: at zero exposure there is nothing
    # for a row to account for, and the writer allows it.
    repository, _ = _reader(
        stub_engine,
        {"fetchone.return_value": _totals_row(exposure_usd=Decimal("0"))},
        {"fetchall.return_value": []},
    )

    snapshot = await repository.get_prime("spark")

    assert snapshot is not None
    assert snapshot.per_allocation == ()


@pytest.mark.asyncio
async def test_wraps_a_database_failure_rather_than_reporting_no_coverage(stub_engine) -> None:
    # A read that failed says nothing about coverage, so it must not surface as
    # `None`, which the API serves as a 404 "not covered".
    repository, _ = _reader(stub_engine, error=RuntimeError("boom"))

    with pytest.raises(ValueError, match="reading the reference risk-capital snapshot"):
        await repository.get_prime("spark")


@pytest.mark.asyncio
async def test_covered_stars_lowercases_what_it_reports(stub_engine) -> None:
    repository, _ = _reader(stub_engine, {"fetchall.return_value": [SimpleNamespace(star="spark")]})

    assert await repository.covered_stars() == frozenset({"spark"})


@pytest.mark.parametrize("statement", [_TOTALS_SQL, _ALLOCATIONS_SQL])
def test_the_snapshot_ordering_never_selects_on_build_id(statement) -> None:
    # build_id spans many cycles and appears in no unique constraint, so
    # ordering by it picks an arbitrary row and mixes values across cycles.
    # A behavioural test cannot catch this: seeded corrections happen to agree
    # with build_id order.
    assert "build_id" not in str(statement)
