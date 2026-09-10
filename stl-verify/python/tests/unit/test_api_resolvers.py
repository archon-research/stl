from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from app.api.deps import PRIME_DENIED_DETAIL
from app.api.v1._resolvers import resolve_prime
from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime import PrimeIdentity
from app.domain.exceptions import InvalidPrimeIdentifierError

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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raised", "status", "detail"),
    [
        (InvalidPrimeIdentifierError("bad"), 422, "malformed prime id"),
        (ValueError("Database query failed while resolving prime spark"), 503, "prime lookup unavailable"),
    ],
)
async def test_resolve_prime_matches_the_authz_gate_on_failures(raised, status, detail) -> None:
    resolver = AsyncMock()
    resolver.resolve.side_effect = raised

    with pytest.raises(HTTPException) as exc:
        await resolve_prime("spark", resolver)

    assert (exc.value.status_code, exc.value.detail) == (status, detail)
