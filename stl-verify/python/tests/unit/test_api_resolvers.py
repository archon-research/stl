from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from app.api.deps import PRIME_DENIED_DETAIL, resolve_prime, resolve_prime_scope
from app.api.errors import ApiRejectionError
from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime import PrimeIdentity
from app.domain.exceptions import InvalidPrimeIdentifierError
from app.domain.prime_registry import ProxyKind

_SPARK = PrimeIdentity(
    id=1, name="spark", external_id="4bd9ee3c-58df-4587-9c04-63b928f1a169", vault_address=EthAddress("0x" + "ab" * 20)
)


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
async def test_resolve_prime_rejects_a_malformed_identifier_like_the_authz_gate() -> None:
    resolver = AsyncMock()
    resolver.resolve.side_effect = InvalidPrimeIdentifierError("bad")

    with pytest.raises(ApiRejectionError, match="malformed prime id"):
        await resolve_prime("0xnope", resolver)


@pytest.mark.asyncio
async def test_resolve_prime_answers_503_when_the_lookup_fails() -> None:
    resolver = AsyncMock()
    resolver.resolve.side_effect = ValueError("Database query failed while resolving prime spark")

    with pytest.raises(HTTPException) as exc:
        await resolve_prime("spark", resolver)

    assert (exc.value.status_code, exc.value.detail) == (503, "prime lookup unavailable")


def _wallet(hex_byte: str, kind: ProxyKind = ProxyKind.ALM):
    from app.domain.entities.prime import ProxyWallet

    return ProxyWallet(address=EthAddress("0x" + hex_byte * 20), chain_id=1, kind=kind)


@pytest.mark.asyncio
async def test_resolve_prime_scope_splits_the_wallets_by_kind() -> None:
    resolver = AsyncMock()
    resolver.resolve.return_value = _SPARK
    resolver.list_proxies.return_value = [_wallet("22", ProxyKind.SUB_PROXY), _wallet("11")]

    scope = await resolve_prime_scope("spark", resolver)

    assert scope.identity is _SPARK
    assert scope.alm_proxies == (EthAddress("0x" + "11" * 20),)
    assert scope.subproxies == (EthAddress("0x" + "22" * 20),)


@pytest.mark.asyncio
async def test_resolve_prime_scope_is_identical_whichever_identifier_names_the_prime() -> None:
    """The property every prime-scoped route is sold on, held by the type rather
    than by each call site."""
    resolver = AsyncMock()
    resolver.resolve.return_value = _SPARK
    resolver.list_proxies.side_effect = [
        [_wallet("11"), _wallet("33")],
        [_wallet("33"), _wallet("11")],
    ]

    by_name = await resolve_prime_scope("spark", resolver)
    by_proxy = await resolve_prime_scope("0x" + "33" * 20, resolver)

    assert by_name == by_proxy


@pytest.mark.asyncio
async def test_resolve_prime_scope_answers_an_empty_wallet_set_not_an_error() -> None:
    resolver = AsyncMock()
    resolver.resolve.return_value = _SPARK
    resolver.list_proxies.return_value = []

    scope = await resolve_prime_scope("spark", resolver)

    assert (scope.alm_proxies, scope.subproxies) == ((), ())


@pytest.mark.asyncio
async def test_resolve_prime_scope_answers_503_when_listing_the_wallets_fails() -> None:
    """Never an empty wallet set: that would render as a prime holding nothing,
    which is the partial total this resolution removes wearing a different hat."""
    resolver = AsyncMock()
    resolver.resolve.return_value = _SPARK
    resolver.list_proxies.side_effect = ValueError("Database query failed while listing proxies")

    with pytest.raises(HTTPException) as exc:
        await resolve_prime_scope("spark", resolver)

    assert (exc.value.status_code, exc.value.detail) == (503, "prime lookup unavailable")


@pytest.mark.asyncio
async def test_resolve_prime_scope_names_the_chains_no_tracker_serves() -> None:
    """From the contract, the declared universe: only it can say a chain exists
    and is not indexed, which is what keeps a whole-prime total a lower bound."""
    resolver = AsyncMock()
    resolver.resolve.return_value = PrimeIdentity(
        id=2, name="grove", external_id="grove-id", vault_address=EthAddress("0x" + "cd" * 20)
    )
    resolver.list_proxies.return_value = []

    scope = await resolve_prime_scope("grove", resolver)

    assert scope.unserved_chains == ("monad", "plasma", "plume")
