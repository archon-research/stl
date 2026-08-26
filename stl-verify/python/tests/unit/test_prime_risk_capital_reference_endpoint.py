"""The reference branch of ``/v1/primes/{id}/risk-capital``.

Kept apart from the self-mode suite because the two share only the route: this
branch runs no model and touches no allocation data, so its fixtures and its
failure modes have nothing in common with those.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from app.api.deps import get_reference_risk_capital_service_factory
from app.api.v1 import prime_risk_capital
from app.domain.entities.reference_risk_capital import ReferenceAllocation, ReferencePrimeRiskCapital
from app.main import app
from app.services.prime_risk_capital_service import PrimeRiskCapitalService

_VALID_ADDR = "0x" + "ab" * 20
_SYNCED_AT = datetime(2026, 8, 26, 9, 15, tzinfo=UTC)


def _reference_allocation(
    *,
    receipt_token_id: int | None = 41,
    token_address: str = "0x" + "cd" * 20,
    required_risk_capital_usd: Decimal = Decimal("990048.94"),
) -> ReferenceAllocation:
    return ReferenceAllocation(
        protocol_name="sparklend",
        network="ethereum",
        symbol="spUSDT",
        name="Spark USDT",
        token_address=token_address,
        loan_token_address="0x" + "12" * 20,
        loan_token_symbol="USDT",
        exposure_usd=Decimal("344187505.66"),
        required_risk_capital_usd=required_risk_capital_usd,
        crr_pct=Decimal("0.28764051"),
        receipt_token_id=receipt_token_id,
        chain="mainnet",
    )


def _snapshot(*, per_allocation: tuple[ReferenceAllocation, ...] | None = None) -> ReferencePrimeRiskCapital:
    zero = Decimal("0")
    return ReferencePrimeRiskCapital(
        star="spark",
        synced_at=_SYNCED_AT,
        exposure_usd=Decimal("2098090654.81"),
        required_risk_capital_usd=Decimal("17837860.43"),
        total_risk_capital_usd=Decimal("48142491.08"),
        encumbrance_ratio=Decimal("0.3705"),
        exposure_share=Decimal("0.0084"),
        junior_risk_capital_usd=Decimal("48142491.08"),
        senior_risk_capital_usd=zero,
        internal_junior_risk_capital_usd=Decimal("48142491.08"),
        external_junior_risk_capital_usd=zero,
        tokenized_junior_risk_capital_usd=zero,
        internal_senior_risk_capital_usd=zero,
        external_senior_risk_capital_usd=zero,
        epi_utilization=zero,
        spj_utilization=zero,
        per_allocation=(_reference_allocation(),) if per_allocation is None else per_allocation,
    )


@pytest.fixture
def reference_client(request):
    """A TestClient whose reference reader returns ``request.param``.

    ``param`` is a snapshot, or ``None`` for a prime no cycle has reported on.
    """
    reference_service = AsyncMock()
    reference_service.get.return_value = request.param

    self_service = AsyncMock(spec=PrimeRiskCapitalService)
    self_service.prime_exists.return_value = True

    async def _self_dep():
        yield self_service

    app.dependency_overrides[prime_risk_capital._get_service] = _self_dep
    app.dependency_overrides[get_reference_risk_capital_service_factory] = lambda: lambda: reference_service
    try:
        yield TestClient(app), self_service
    finally:
        app.dependency_overrides.clear()


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_reference_mode_reports_its_provenance(reference_client):
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?reference=true").json()

    assert body["source"] == "reference"


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_reference_mode_serves_the_upstream_totals_in_the_existing_fields(reference_client):
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?reference=true").json()

    assert body["exposure_usd"] == "2098090654.81"
    assert body["required_risk_capital_usd"] == "17837860.43"
    assert body["total_risk_capital_usd"] == "48142491.08"


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_reference_mode_stamps_the_cycle_the_figures_were_observed_at(reference_client):
    # The figures are STL's record of the monitor rather than a live read, so
    # serving them without a stamp would imply they are current.
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?reference=true").json()

    assert body["reference_synced_at"] == "2026-08-26T09:15:00Z"


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_reference_mode_populates_the_junior_senior_split_self_mode_cannot(reference_client):
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?reference=true").json()

    assert body["junior_risk_capital_usd"] == "48142491.08"
    assert body["senior_risk_capital_usd"] == "0"
    assert body["exposure_share"] == "0.0084"


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_reference_mode_serves_the_upstream_breakdown_as_per_allocation(reference_client):
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?reference=true").json()

    (row,) = body["per_allocation"]
    assert row["symbol"] == "spUSDT"
    assert row["exposure_usd"] == "344187505.66"
    assert row["crr_pct"] == "0.28764051"
    assert row["applied"] is True
    assert row["model"] is None


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_reference_mode_stamps_reference_provenance_on_each_row(reference_client):
    # The row's own source, not just the response-level one: a consumer reading
    # per_allocation in isolation must still see who reported the position.
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?reference=true").json()

    (row,) = body["per_allocation"]
    assert row["source"] == "reference"


@pytest.mark.parametrize(
    "reference_client",
    [_snapshot(per_allocation=(_reference_allocation(receipt_token_id=None, token_address="0x" + "ef" * 32),))],
    indirect=True,
)
def test_reference_mode_serves_an_unresolvable_position_with_a_null_receipt_token_id(reference_client):
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?reference=true").json()

    (row,) = body["per_allocation"]
    assert row["receipt_token_id"] is None
    assert row["token_address"] == "0x" + "ef" * 32


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_reference_mode_leaves_the_per_proxy_audit_trail_empty(reference_client):
    # Upstream publishes no proxy topology, so there is no split to audit the
    # total against, and no chain may be reported as unserved either: the
    # upstream totals are not bounded by what STL indexes.
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?reference=true").json()

    assert body["prime_per_chain"] == []
    assert body["prime_unserved_chains"] == []


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_reference_mode_never_runs_the_self_model(reference_client):
    client, self_service = reference_client

    client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?reference=true")

    self_service.compute.assert_not_awaited()


@pytest.mark.parametrize("reference_client", [None], indirect=True)
def test_reference_mode_returns_404_when_no_cycle_has_reported_on_the_prime(reference_client):
    client, _ = reference_client

    response = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?reference=true")

    assert response.status_code == 404
    assert "No reference risk capital" in response.json()["detail"]


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_reference_mode_is_off_by_default(reference_client):
    client, self_service = reference_client
    self_service.compute.return_value = _self_result()

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital").json()

    assert body["source"] == "indexed"
    assert body["junior_risk_capital_usd"] is None
    assert body["per_allocation"] == []


def _indexed_allocation(*, receipt_token_id: int, exposure_usd: Decimal):
    from app.domain.entities.prime_risk_capital import AllocationRiskCapital

    return AllocationRiskCapital(
        receipt_token_id=receipt_token_id,
        symbol="spUSDT",
        protocol_name="sparklend",
        exposure_usd=exposure_usd,
        applied=True,
        required_risk_capital_usd=Decimal("30"),
        crr_pct=Decimal("28.76"),
        model="gap_sweep",
    )


def _self_result(total_risk_capital_usd: Decimal = Decimal("100"), per_allocation=None):
    from app.domain.entities.prime_risk_capital import PrimeRiskCapital

    return PrimeRiskCapital(
        proxy_address=_VALID_ADDR,
        model="gap_sweep",
        exposure_usd=Decimal("1000"),
        total_risk_capital_usd=total_risk_capital_usd,
        required_risk_capital_usd=Decimal("30"),
        encumbrance_ratio=Decimal("0.3"),
        modeled_exposure_usd=Decimal("600"),
        modeled_pct=Decimal("0.6"),
        per_allocation=per_allocation if per_allocation is not None else [],
    )


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_source_reference_answers_from_the_monitor(reference_client):
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?source=reference").json()

    assert body["source"] == "reference"


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_source_indexed_answers_from_stl_own_model(reference_client):
    client, self_service = reference_client
    self_service.compute.return_value = _self_result()

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?source=indexed").json()

    assert body["source"] == "indexed"
    assert body["junior_risk_capital_usd"] is None


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_both_keeps_each_provenance_in_its_own_fields(reference_client):
    # They populate disjoint sets and disagree on what they share, so nothing is
    # merged into one number.
    client, self_service = reference_client
    self_service.compute.return_value = _self_result()

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?source=both").json()

    assert body["source"] == "both"
    assert body["prime_exposure_usd"] != body["reference_prime_exposure_usd"]
    assert body["reference_total_risk_capital_usd"] == "48142491.08"
    # Sky reports these and STL models none of them.
    assert body["junior_risk_capital_usd"] is not None
    assert body["reference_synced_at"] == "2026-08-26T09:15:00Z"


@pytest.mark.parametrize(
    "reference_client",
    [
        _snapshot(
            per_allocation=(
                _reference_allocation(receipt_token_id=None, token_address="0x" + "ee" * 20),
                _reference_allocation(receipt_token_id=41),
            )
        )
    ],
    indirect=True,
)
def test_both_orders_the_merged_breakdown_by_exposure(reference_client):
    # Each half arrives ordered by its own exposure, so concatenating them yields
    # neither order and a consumer truncating the published list reads the wrong
    # rows.
    client, self_service = reference_client
    self_service.compute.return_value = _self_result(
        per_allocation=[
            _indexed_allocation(receipt_token_id=41, exposure_usd=Decimal("900000000")),
            _indexed_allocation(receipt_token_id=77, exposure_usd=Decimal("1")),
        ]
    )

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?source=both").json()

    exposures = [Decimal(row["exposure_usd"]) for row in body["per_allocation"]]
    assert exposures == sorted(exposures, reverse=True)


@pytest.mark.parametrize(
    "reference_client",
    [_snapshot(per_allocation=(_reference_allocation(receipt_token_id=41),))],
    indirect=True,
)
def test_both_carries_skys_own_ratio_rather_than_deriving_one(reference_client):
    # Upstream's `crr` is its own ratio; dividing its two figures would publish a
    # number Sky does not.
    client, self_service = reference_client
    self_service.compute.return_value = _self_result(
        per_allocation=[_indexed_allocation(receipt_token_id=41, exposure_usd=Decimal("900000000"))]
    )

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?source=both").json()

    (row,) = [row for row in body["per_allocation"] if row["source"] == "both"]
    assert row["crr_pct"] == "28.76"
    assert row["reference_crr_pct"] == "0.28764051"


@pytest.mark.parametrize("reference_client", [None], indirect=True)
def test_both_serves_stl_own_model_for_a_prime_with_no_reference_data(reference_client):
    client, self_service = reference_client
    self_service.compute.return_value = _self_result()

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?source=both").json()

    assert body["source"] == "indexed"
    assert body["reference_prime_exposure_usd"] is None
    assert body["reference_synced_at"] is None


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_a_database_failure_is_not_rewritten_into_an_absence_of_reference_data(reference_client):
    # A read that failed says nothing about coverage, so it must not be served
    # as "this prime has none" -- which reads identically to a real answer.
    client, _ = reference_client
    app.dependency_overrides[get_reference_risk_capital_service_factory] = lambda: lambda: _failing_reader()

    with pytest.raises(ValueError, match="boom"):
        client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?source=reference")


def _failing_reader():
    reader = AsyncMock()
    reader.get.side_effect = ValueError("Database query failed: boom")
    return reader


@pytest.mark.parametrize(
    "reference_client",
    # Two positions whose requirements sum to the prime's, as upstream's do:
    # measured against live data they reconcile to -0.0002%.
    [
        _snapshot(
            per_allocation=(
                _reference_allocation(required_risk_capital_usd=Decimal("10000000.43")),
                _reference_allocation(
                    token_address="0x" + "dd" * 20,
                    required_risk_capital_usd=Decimal("7837860.00"),
                ),
            )
        )
    ],
    indirect=True,
)
def test_encumbrance_contributions_decompose_the_prime_ratio(reference_client):
    # The denominator is the prime's whole risk capital, the same for every row,
    # so the column decomposes the published ratio rather than being a set of
    # unrelated fractions.
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?source=reference").json()

    contributions = [Decimal(row["encumbrance_contribution"]) for row in body["per_allocation"]]
    assert len(contributions) == 2
    expected = Decimal(body["prime_required_risk_capital_usd"]) / Decimal(body["total_risk_capital_usd"])
    assert sum(contributions) == expected


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_no_contribution_is_attributed_without_a_total_to_divide_by(reference_client):
    client, self_service = reference_client
    self_service.compute.return_value = _self_result(total_risk_capital_usd=Decimal(0))

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?source=indexed").json()

    assert all(row["encumbrance_contribution"] is None for row in body["per_allocation"])


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_both_attributes_no_contribution_to_a_sky_only_row(reference_client):
    # Under `both` the denominator is STL's own total; a row Sky alone reports
    # is not comparable to it, so it stays excluded even though its own
    # `source` is `reference` — unlike a pure `source=reference` response,
    # where that same field value means every row is fair game.
    client, self_service = reference_client
    self_service.compute.return_value = _self_result(total_risk_capital_usd=Decimal("100"))

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?source=both").json()

    (row,) = [row for row in body["per_allocation"] if row["source"] == "reference"]
    assert row["encumbrance_contribution"] is None
