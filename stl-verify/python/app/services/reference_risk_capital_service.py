"""Reference (Star monitor) risk capital for a prime, as STL observed it.

The counterpart to :class:`~app.services.prime_risk_capital_service.PrimeRiskCapitalService`:
same question, upstream's answer. No on-chain work and no model run — it reads
STL's own record of what the monitor published for the named prime.
"""

from app.domain.entities.reference_risk_capital import ReferencePrimeRiskCapital
from app.ports.reference_risk_capital import ReferenceRiskCapitalProvider


class ReferenceRiskCapitalService:
    """Reads a prime's most recently observed risk-capital snapshot."""

    def __init__(self, provider: ReferenceRiskCapitalProvider) -> None:
        self._provider = provider

    async def get(self, star: str) -> ReferencePrimeRiskCapital | None:
        """Return the observed snapshot for the prime named ``star``.

        ``None`` for a prime no cycle has reported on — a coverage answer, not a
        failure, and never to be served as zeros.
        """
        return await self._provider.get_prime(star)

    async def covered_stars(self) -> frozenset[str]:
        """Every prime reference figures have been observed for, lowercased."""
        return await self._provider.covered_stars()
