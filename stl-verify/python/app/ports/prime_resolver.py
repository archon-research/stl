"""Outbound port turning any accepted prime identifier into one prime."""

from typing import Protocol

from app.domain.entities.prime import PrimeIdentity, ProxyWallet


class PrimeResolver(Protocol):
    """Resolve the external forms of a prime to its canonical identity.

    Resolution is silent — no redirect — and always whole-prime: a proxy address
    returns the prime that owns it, including the chains that proxy does not serve.
    """

    async def resolve(self, identifier: str) -> PrimeIdentity | None:
        """Return the prime ``identifier`` names, or ``None`` when nothing matches."""
        ...

    async def list_proxies(self, prime_id: int) -> list[ProxyWallet]:
        """Every wallet ``prime_proxy`` records for ``prime_id``, address-sorted.

        Empty for a prime with a vault and no proxies. Raises ``ValueError`` on a
        failed query, like ``resolve``, so the caller answers 503 rather than
        reporting a prime that holds nothing.
        """
        ...
