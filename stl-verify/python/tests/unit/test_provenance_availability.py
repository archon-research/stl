from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from app.api.deps import get_reference_risk_capital_service_factory
from app.api.v1 import provenance_availability
from app.domain.entities.allocation import Prime
from app.domain.exceptions import ReferenceDataUnavailableError
from app.main import app
from app.services.allocation_service import AllocationService
from app.services.reference_risk_capital_service import ReferenceRiskCapitalService


def _prime(name: str) -> Prime:
    return Prime(
        id="0x" + "ab" * 20,
        name=name,
        address="0x" + "ab" * 20,
        chain_id=1,
        chain="mainnet",
        role="alm",
        prime_vault_address=None,
    )


@pytest.fixture
def client(request):
    """A client whose monitor returns ``request.param``, or raises it."""
    outcome = request.param

    service = AsyncMock(spec=AllocationService)
    service.list_primes.return_value = [_prime("spark"), _prime("grove"), _prime("spark")]

    reference = AsyncMock(spec=ReferenceRiskCapitalService)
    if isinstance(outcome, Exception):
        reference.tracked_stars.side_effect = outcome
    else:
        reference.tracked_stars.return_value = outcome

    async def _service_dep():
        yield service

    app.dependency_overrides[provenance_availability._get_service] = _service_dep
    app.dependency_overrides[get_reference_risk_capital_service_factory] = lambda: lambda: reference
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


@pytest.mark.parametrize("client", [frozenset({"spark"})], indirect=True)
def test_a_tracked_prime_can_be_served_from_either_provenance_or_both(client: TestClient):
    body = client.get("/v1/provenance/available").json()

    spark = next(row for row in body["primes"] if row["name"] == "spark")
    assert spark["available"] == ["indexed", "reference", "both"]


@pytest.mark.parametrize("client", [frozenset({"spark"})], indirect=True)
def test_an_untracked_prime_offers_only_stl_own_figures(client: TestClient):
    body = client.get("/v1/provenance/available").json()

    grove = next(row for row in body["primes"] if row["name"] == "grove")
    assert grove["available"] == ["indexed"]


@pytest.mark.parametrize("client", [frozenset({"spark"})], indirect=True)
def test_each_prime_is_listed_once_however_many_proxies_it_has(client: TestClient):
    body = client.get("/v1/provenance/available").json()

    names = [row["name"] for row in body["primes"]]
    assert names == sorted(set(names))


@pytest.mark.parametrize("client", [ReferenceDataUnavailableError("boom")], indirect=True)
def test_unknown_coverage_is_reported_as_no_coverage(client: TestClient):
    # Never a 502: STL's own figures are unaffected. Claiming a provenance is
    # available and then failing every request for it is the worse answer.
    response = client.get("/v1/provenance/available")

    assert response.status_code == 200
    body = response.json()
    assert body["reference_upstream_reachable"] is False
    assert all(row["available"] == ["indexed"] for row in body["primes"])


@pytest.mark.parametrize("client", [frozenset({"SPARK"})], indirect=True)
def test_coverage_matches_regardless_of_case(client: TestClient):
    body = client.get("/v1/provenance/available").json()

    spark = next(row for row in body["primes"] if row["name"] == "spark")
    assert "reference" in spark["available"]
