import pytest

from app.api.deps import get_prime_resolver
from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime import PrimeIdentity, ProxyWallet
from app.domain.exceptions import InvalidPrimeIdentifierError
from app.domain.prime_registry import ProxyKind
from app.main import app
from tests.factories import make_prime_identity, make_proxy_wallet


def _default_wallets() -> list[ProxyWallet]:
    """One ALM proxy and one SubProxy treasury, the shape of every real prime."""
    return [
        make_proxy_wallet(address=EthAddress("0x" + "11" * 20), kind=ProxyKind.ALM),
        make_proxy_wallet(address=EthAddress("0x" + "22" * 20), kind=ProxyKind.SUB_PROXY),
    ]


class FakePrimeResolver:
    """In-memory ``PrimeResolver`` carrying the adapter's identifier semantics.

    Every prime-scoped route resolves its path segment through the gate, and the
    shared ``app`` these tests drive never runs its lifespan, so the resolver has
    to come from here. It resolves anything well-formed; a test about a prime
    that does NOT resolve installs its own override.
    """

    def __init__(
        self,
        identity: PrimeIdentity | None = None,
        wallets: list[ProxyWallet] | None = None,
    ) -> None:
        self.identity = identity if identity is not None else make_prime_identity()
        self.wallets = wallets if wallets is not None else _default_wallets()

    async def resolve(self, identifier: str) -> PrimeIdentity | None:
        # A 0x prefix commits the value to being an address, which is what makes
        # a malformed one a 422 rather than a name that resolves to nothing.
        if identifier.lower().startswith("0x"):
            try:
                EthAddress(identifier)
            except ValueError as exc:
                raise InvalidPrimeIdentifierError(f"Invalid prime identifier: {identifier}") from exc
        return self.identity

    async def list_proxies(self, prime_id: int) -> list[ProxyWallet]:
        return list(self.wallets)


@pytest.fixture(autouse=True)
def prime_resolver():
    """Wire a resolver onto the shared app, and hand it to tests that tune it."""
    resolver = FakePrimeResolver()
    app.dependency_overrides[get_prime_resolver] = lambda: resolver
    return resolver


@pytest.fixture(autouse=True)
def clear_dependency_overrides():
    yield
    app.dependency_overrides.clear()


@pytest.fixture
def authz_events(caplog):
    """Authorization decision events as logged, oldest first, read after the call."""
    import logging

    from app.api import deps

    caplog.set_level(logging.INFO, logger="app.api.deps")

    def _read() -> list[dict]:
        keys = ("gate", "decision", "reason", "principal", "resource", "status", "requested_prime")
        return [
            {k: getattr(r, k) for k in keys if hasattr(r, k)}
            for r in caplog.records
            if getattr(r, "event", None) == deps.AUTHZ_EVENT
        ]

    return _read
