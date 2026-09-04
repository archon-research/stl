"""Every route that ENUMERATES primes filters by the caller's allow-list.

``/v1/provenance/available`` shipped without the filter its sibling
``/v1/primes`` had, so an org:viewer authorized for one prime was handed every
prime name STL indexes. Two levels are pinned here: the route asks for the
allow-list at all, and the value it asks for reaches the query unchanged.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import AsyncMock

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from app.api import deps
from app.api.v1 import allocations, provenance_availability
from app.domain.entities.allocation import EthAddress, Prime
from app.main import app
from app.services.allocation_service import AllocationService
from app.services.reference_risk_capital_service import ReferenceRiskCapitalService

VAULT = "0x" + "a" * 40

# Every handler that enumerates primes. Each must push the allow-list into its
# query or it discloses primes the caller may not view. Paths are unprefixed:
# these are the routers as declared, before main.py mounts them under /v1.
PRIME_ENUMERATING_ROUTES = [
    (allocations.router, "GET", "/primes"),
    (allocations.router, "GET", "/allocations/activity"),
    (provenance_availability.router, "GET", "/provenance/available"),
]


def _prime(name: str, address: str) -> Prime:
    return Prime(
        id=address,
        name=name,
        address=address,
        chain_id=1,
        chain="mainnet",
        role="alm",
        prime_vault_address=address,
    )


def _dependency_calls(dependant) -> set:
    """Every callable in a route's dependency tree, however deeply nested."""
    calls = {dependant.call} if dependant.call is not None else set()
    for sub in dependant.dependencies:
        calls |= _dependency_calls(sub)
    return calls


@pytest.mark.parametrize("router,method,path", PRIME_ENUMERATING_ROUTES)
def test_the_route_declares_the_allow_list_dependency(router, method: str, path: str) -> None:
    """Structural: a dropped ``Depends`` fails here rather than in production.

    The single-element unpack is load-bearing — a renamed path must fail this
    test, not quietly make it assert nothing.
    """
    (route,) = [r for r in router.routes if isinstance(r, APIRoute) and r.path == path and method in r.methods]

    assert deps.allowed_prime_vaults in _dependency_calls(route.dependant)


@pytest.fixture
def allocation_service() -> AsyncMock:
    service = AsyncMock(spec=AllocationService)
    service.list_primes.return_value = [_prime("spark", VAULT)]
    service.list_allocation_activity.return_value = []
    service.list_activity_buckets.return_value = []
    return service


@pytest.fixture
def client(request, allocation_service: AsyncMock) -> Iterator[TestClient]:
    """A client whose caller is authorized for ``request.param``."""
    reference = AsyncMock(spec=ReferenceRiskCapitalService)
    reference.covered_stars.return_value = frozenset({"spark"})

    async def _service_dep():
        yield allocation_service

    app.dependency_overrides[allocations._get_service] = _service_dep
    app.dependency_overrides[provenance_availability._get_service] = _service_dep
    app.dependency_overrides[deps.get_reference_risk_capital_service_factory] = lambda: lambda: reference
    app.dependency_overrides[deps.allowed_prime_vaults] = lambda: request.param
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


@pytest.mark.parametrize("client", [frozenset({VAULT})], indirect=True)
@pytest.mark.parametrize("path", ["/v1/primes", "/v1/provenance/available"])
def test_the_allow_list_reaches_the_query(client: TestClient, allocation_service: AsyncMock, path: str) -> None:
    assert client.get(path).status_code == 200
    assert allocation_service.list_primes.await_args.kwargs["allowed_vaults"] == [EthAddress(VAULT)]


@pytest.mark.parametrize("client", [frozenset()], indirect=True)
@pytest.mark.parametrize("path", ["/v1/primes", "/v1/provenance/available"])
def test_an_empty_allow_list_is_passed_through_not_dropped(
    client: TestClient, allocation_service: AsyncMock, path: str
) -> None:
    """``[]`` means "may view none". Collapsing it to None means "auth off"."""
    assert client.get(path).status_code == 200
    assert allocation_service.list_primes.await_args.kwargs["allowed_vaults"] == []


@pytest.mark.parametrize("client", [None], indirect=True)
@pytest.mark.parametrize("path", ["/v1/primes", "/v1/provenance/available"])
def test_auth_off_is_unfiltered(client: TestClient, allocation_service: AsyncMock, path: str) -> None:
    assert client.get(path).status_code == 200
    assert allocation_service.list_primes.await_args.kwargs["allowed_vaults"] is None


@pytest.mark.parametrize("client", [frozenset({VAULT})], indirect=True)
def test_provenance_reports_only_primes_the_caller_may_view(client: TestClient, allocation_service: AsyncMock) -> None:
    """The filtered query is the whole control: whatever it returns is what the
    response names, so an unfiltered call would publish every prime's name."""
    allocation_service.list_primes.return_value = [_prime("spark", VAULT)]

    body = client.get("/v1/provenance/available").json()

    assert [row["name"] for row in body["primes"]] == ["spark"]


@pytest.mark.parametrize("client", [frozenset({VAULT})], indirect=True)
def test_activity_pushes_the_allow_list_into_the_query(client: TestClient, allocation_service: AsyncMock) -> None:
    assert client.get("/v1/allocations/activity").status_code == 200
    assert allocation_service.list_allocation_activity.await_args.kwargs["allowed_vaults"] == [EthAddress(VAULT)]


# --- the aggregated activity path -------------------------------------------
#
# Buckets sum across primes. The route used to refuse an unscoped aggregate and
# that was the whole control; the allow-list now travels into the bucket query
# as well, so the invariant lives with the rows it is about.

OTHER = "0x" + "c" * 40


@pytest.mark.parametrize("client", [frozenset({VAULT})], indirect=True)
def test_aggregated_activity_pushes_the_allow_list_into_the_query(
    client: TestClient, allocation_service: AsyncMock
) -> None:
    assert client.get(f"/v1/allocations/activity?aggregate=true&prime_id={VAULT}").status_code == 200
    assert allocation_service.list_activity_buckets.await_args.kwargs["allowed_vaults"] == [EthAddress(VAULT)]


@pytest.mark.parametrize("client", [frozenset({VAULT})], indirect=True)
def test_an_unscoped_aggregate_is_refused_while_authorization_is_on(client: TestClient) -> None:
    """One number over every prime the caller may view is not a view anyone
    asked for; name the prime the bucket is about."""
    response = client.get("/v1/allocations/activity?aggregate=true")

    assert response.status_code == 422
    assert response.json()["detail"] == "prime_id is required for aggregated activity"


@pytest.mark.parametrize("client", [None], indirect=True)
def test_an_unscoped_aggregate_is_unchanged_while_auth_is_dark(
    client: TestClient, allocation_service: AsyncMock
) -> None:
    assert client.get("/v1/allocations/activity?aggregate=true").status_code == 200
    assert allocation_service.list_activity_buckets.await_args.kwargs["allowed_vaults"] is None


@pytest.mark.parametrize("client", [frozenset({VAULT})], indirect=True)
@pytest.mark.parametrize("aggregate", ["", "&aggregate=true"], ids=["raw", "aggregated"])
def test_a_prime_the_caller_may_not_view_is_an_empty_list_not_a_denial(client: TestClient, aggregate: str) -> None:
    """prime_id is a FILTER here, not a path resource. A 403 on a prime that
    exists and a 404 on one that does not would between them enumerate the
    primes the list filtering is there to hide; both are no rows instead."""
    response = client.get(f"/v1/allocations/activity?prime_id={OTHER}{aggregate}")

    assert response.status_code == 200
    assert response.json()["data"] == []
