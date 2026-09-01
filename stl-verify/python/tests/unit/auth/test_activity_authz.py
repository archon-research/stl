"""L2-B2 regression: authorization joins the activity QUERY, before LIMIT."""

from __future__ import annotations

from unittest.mock import AsyncMock

from app.domain.entities.allocation import EthAddress
from app.services.allocation_service import AllocationService

V1 = EthAddress("0x" + "a" * 40)
P1 = EthAddress("0x" + "b" * 40)
P2 = EthAddress("0x" + "c" * 40)


def _service() -> tuple[AllocationService, AsyncMock]:
    repo = AsyncMock()
    return AllocationService(repo), repo


async def test_allowed_vaults_become_the_query_proxy_filter():
    svc, repo = _service()
    repo.list_proxy_addresses_for_vaults.return_value = [P1, P2]
    repo.list_allocation_activity.return_value = []
    await svc.list_allocation_activity(limit=50, allowed_vaults=[V1])
    repo.list_proxy_addresses_for_vaults.assert_awaited_once_with([V1])
    kwargs = repo.list_allocation_activity.await_args.kwargs
    assert kwargs["proxy_addresses"] == [P1, P2]  # filter reaches the SQL, pre-LIMIT
    assert kwargs["limit"] == 50


async def test_no_authorized_primes_short_circuits_to_empty():
    svc, repo = _service()
    repo.list_proxy_addresses_for_vaults.return_value = []
    assert await svc.list_allocation_activity(allowed_vaults=[V1]) == []
    repo.list_allocation_activity.assert_not_awaited()


async def test_requested_prime_intersects_with_allowed():
    svc, repo = _service()
    repo.list_prime_proxy_addresses.return_value = [P1, P2]
    repo.list_proxy_addresses_for_vaults.return_value = [P1]
    repo.list_allocation_activity.return_value = []
    await svc.list_allocation_activity(prime_id=P1, allowed_vaults=[V1])
    assert repo.list_allocation_activity.await_args.kwargs["proxy_addresses"] == [P1]


async def test_auth_off_touches_no_authorization_path():
    svc, repo = _service()
    repo.list_prime_proxy_addresses.return_value = None
    repo.list_allocation_activity.return_value = []
    await svc.list_allocation_activity(prime_id=None, allowed_vaults=None)
    repo.list_proxy_addresses_for_vaults.assert_not_awaited()
