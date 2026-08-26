"""Outbound port for STL's stored Star-monitor risk-capital snapshots."""

from typing import Protocol

from app.domain.entities.reference_risk_capital import ReferencePrimeRiskCapital


class ReferenceRiskCapitalProvider(Protocol):
    """Read a prime's most recently observed risk-capital snapshot."""

    async def covered_stars(self) -> frozenset[str]:
        """Return every star reference figures have been observed for, lowercased.

        Answers "can this prime be served from reference at all" without reading
        a snapshot per prime. A prime the indexer has never landed a cycle for
        has no reference figures, which is the same answer the monitor's own
        tracked set used to give — the indexer writes a row per prime the
        monitor covers, per cycle.
        """
        ...

    async def get_prime(self, star: str) -> ReferencePrimeRiskCapital | None:
        """Return ``star``'s latest observed snapshot, or ``None`` if none exists.

        ``None`` is a real answer, not an error: the monitor covers a subset of
        the primes STL indexes, so an uncovered prime has no reference figures
        and must be reported as such rather than as zeros.
        """
        ...
