"""Outbound port for the upstream Star risk-capital monitor."""

from typing import Protocol

from app.domain.entities.reference_risk_capital import ReferencePrimeRiskCapital


class ReferenceRiskCapitalProvider(Protocol):
    """Fetch a prime's current risk-capital snapshot from the upstream monitor."""

    async def get_prime(self, star: str) -> ReferencePrimeRiskCapital | None:
        """Return the upstream snapshot for ``star``, or ``None`` if it tracks no such prime.

        ``None`` is a real answer, not an error: the monitor covers a subset of
        the primes STL indexes, so an untracked prime has no reference figures
        and must be reported as such rather than as zeros.
        """
        ...
