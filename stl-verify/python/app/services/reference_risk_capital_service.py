"""Reference (Star monitor) risk capital for a prime, as STL observed it.

The counterpart to :class:`~app.services.prime_risk_capital_service.PrimeRiskCapitalService`:
same question, upstream's answer. It resolves the queried ALM proxy to a star
name via the in-memory axis-synome registry — no on-chain work and no model
run — then hands that name to the reader of STL's own reference tables.
"""

from app.domain.entities.allocation import EthAddress
from app.domain.entities.reference_risk_capital import ReferencePrimeRiskCapital
from app.ports.prime_directory import PrimeDirectory
from app.ports.reference_risk_capital import ReferenceRiskCapitalProvider
from app.services.star_resolution import star_for


class ReferenceRiskCapitalService:
    """Reads a prime's most recently observed risk-capital snapshot."""

    def __init__(self, provider: ReferenceRiskCapitalProvider, primes: PrimeDirectory) -> None:
        self._provider = provider
        self._primes = primes

    async def get(self, proxy_address: EthAddress) -> ReferencePrimeRiskCapital | None:
        """Return the observed snapshot for the prime owning ``proxy_address``.

        ``None`` when the address names no prime STL knows, and whatever the
        reader answers otherwise — which is also ``None`` for a prime no cycle
        has reported on. Both are coverage answers, not failures, and neither may
        be served as zeros.
        """
        star = await star_for(proxy_address, self._primes)
        if star is None:
            return None

        return await self._provider.get_prime(star)

    async def covered_stars(self) -> frozenset[str]:
        """Every prime reference figures have been observed for, lowercased."""
        return await self._provider.covered_stars()
