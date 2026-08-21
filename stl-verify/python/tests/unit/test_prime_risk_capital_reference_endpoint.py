"""The ``reference=true`` branch of ``/v1/primes/{id}/risk-capital``.

Kept apart from the self-mode suite because the two share only the route: this
branch runs no model and touches no allocation data, so its fixtures and its
failure modes have nothing in common with those.
"""

from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from app.api.deps import get_reference_risk_capital_service_factory
from app.api.v1 import prime_risk_capital
from app.domain.entities.reference_risk_capital import ReferenceAllocation, ReferencePrimeRiskCapital
from app.domain.exceptions import ReferenceDataUnavailableError
from app.main import app
from app.services.prime_risk_capital_service import PrimeRiskCapitalService

_VALID_ADDR = "0x" + "ab" * 20


def _reference_allocation(
    *, receipt_token_id: int | None = 41, token_address: str = "0x" + "cd" * 20
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
        required_risk_capital_usd=Decimal("990048.94"),
        crr_pct=Decimal("0.28764051"),
        receipt_token_id=receipt_token_id,
        chain="mainnet",
    )


def _snapshot(*, per_allocation: tuple[ReferenceAllocation, ...] | None = None) -> ReferencePrimeRiskCapital:
    zero = Decimal("0")
    return ReferencePrimeRiskCapital(
        star="spark",
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
    """A TestClient whose reference service returns ``request.param``.

    ``param`` is either a snapshot (or ``None``) to return, or an exception
    instance to raise.
    """
    outcome = request.param
    reference_service = AsyncMock()
    if isinstance(outcome, Exception):
        reference_service.get.side_effect = outcome
    else:
        reference_service.get.return_value = outcome

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
def test_reference_mode_returns_404_when_the_monitor_does_not_track_the_prime(reference_client):
    client, _ = reference_client

    response = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?reference=true")

    assert response.status_code == 404
    assert "does not track" in response.json()["detail"]


@pytest.mark.parametrize("reference_client", [ReferenceDataUnavailableError("boom")], indirect=True)
def test_reference_mode_returns_502_when_the_monitor_cannot_be_read(reference_client):
    # Held apart from the 404 above so an upstream outage is never served as an
    # absence of exposure, which reads as a real answer.
    client, _ = reference_client

    response = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital?reference=true")

    assert response.status_code == 502


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_reference_mode_is_off_by_default(reference_client):
    client, self_service = reference_client
    self_service.compute.return_value = _self_result()

    body = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital").json()

    assert body["source"] == "self"
    assert body["junior_risk_capital_usd"] is None
    assert body["per_allocation"] == []


def _self_result():
    from app.domain.entities.prime_risk_capital import PrimeRiskCapital

    return PrimeRiskCapital(
        proxy_address=_VALID_ADDR,
        model="gap_sweep",
        exposure_usd=Decimal("1000"),
        total_risk_capital_usd=Decimal("100"),
        required_risk_capital_usd=Decimal("30"),
        encumbrance_ratio=Decimal("0.3"),
        modeled_exposure_usd=Decimal("600"),
        modeled_pct=Decimal("0.6"),
        per_allocation=[],
    )
