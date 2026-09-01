"""L2-B2 regression: authorization is part of the query semantics.

allowed_vaults travels dependency → service → repository → SQL WHERE, applied
before ORDER BY/LIMIT. Contract: None = auth off (no filter); [] = caller may
view no primes (no rows); non-empty = only those vaults.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

from app.domain.entities.allocation import EthAddress
from app.services.allocation_service import AllocationService

V1 = EthAddress("0x" + "a" * 40)
P1 = EthAddress("0x" + "b" * 40)


def _service() -> tuple[AllocationService, AsyncMock]:
    repo = AsyncMock()
    return AllocationService(repo), repo


async def test_allowed_vaults_reach_the_repository_query():
    svc, repo = _service()
    repo.list_prime_proxy_addresses.return_value = [P1]
    repo.list_allocation_activity.return_value = []
    await svc.list_allocation_activity(prime_id=P1, limit=50, allowed_vaults=[V1])
    kwargs = repo.list_allocation_activity.await_args.kwargs
    assert kwargs["allowed_vaults"] == [V1]  # into the SQL WHERE, pre-LIMIT
    assert kwargs["proxy_addresses"] == [P1]  # the prime filter stays independent
    assert kwargs["limit"] == 50


async def test_empty_allowed_set_is_passed_through_not_dropped():
    # [] means "may view nothing": the SQL ANY([]) matches no rows. It must not
    # be collapsed into None, which means "auth off, no filter".
    svc, repo = _service()
    repo.list_allocation_activity.return_value = []
    await svc.list_allocation_activity(allowed_vaults=[])
    assert repo.list_allocation_activity.await_args.kwargs["allowed_vaults"] == []


async def test_auth_off_passes_none():
    svc, repo = _service()
    repo.list_allocation_activity.return_value = []
    await svc.list_allocation_activity(allowed_vaults=None)
    assert repo.list_allocation_activity.await_args.kwargs["allowed_vaults"] is None


async def test_list_primes_filters_in_the_repository():
    svc, repo = _service()
    repo.list_primes.return_value = []
    await svc.list_primes(allowed_vaults=[V1])
    repo.list_primes.assert_awaited_once_with(allowed_vaults=[V1])
