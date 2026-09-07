"""Outbound port for the primes STL holds positions for."""

from typing import Protocol

from app.domain.entities.allocation import Prime


class PrimeDirectory(Protocol):
    """List the prime/proxy rows ``/v1/primes`` is built from.

    Narrower than the allocation repository on purpose: the reference path needs
    only to name the prime an address belongs to, not to read its positions.
    """

    async def list_primes(self) -> list[Prime]:
        """Return one row per (proxy, chain) STL holds positions for."""
        ...
