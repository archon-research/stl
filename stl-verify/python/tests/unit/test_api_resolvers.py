from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from app.api.deps import PRIME_DENIED_DETAIL
from app.api.v1._resolvers import resolve_prime
from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime import PrimeIdentity

_SPARK = PrimeIdentity(id=1, name="spark", prime_key="prm_2d3ceee8415e59f3", vault_address=EthAddress("0x" + "ab" * 20))


@pytest.mark.asyncio
async def test_resolve_prime_returns_what_the_resolver_matched() -> None:
    resolver = AsyncMock()
    resolver.resolve.return_value = _SPARK

    assert await resolve_prime("spark", resolver) is _SPARK
    resolver.resolve.assert_awaited_once_with("spark")


@pytest.mark.asyncio
async def test_resolve_prime_raises_404_for_an_unknown_identifier() -> None:
    resolver = AsyncMock()
    resolver.resolve.return_value = None

    with pytest.raises(HTTPException) as exc:
        await resolve_prime("nope", resolver)

    assert exc.value.status_code == 404
    assert exc.value.detail == PRIME_DENIED_DETAIL
