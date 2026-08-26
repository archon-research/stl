"""Reference (Star monitor) risk capital for a prime, as STL observed it.

The counterpart to :class:`~app.services.prime_risk_capital_service.PrimeRiskCapitalService`:
same question, upstream's answer. It resolves the queried ALM proxy to a star
name via the in-memory axis-synome registry — no on-chain work and no model
run — then hands that name to the reader of STL's own reference tables.

The registry join that attaches STL's receipt-token ids to upstream's rows is
the reader's SQL, not this service's fan-out: it was a point lookup per row,
which is a ``LEFT JOIN`` once the rows come from the same database.
"""

import logging

from app.domain.entities.allocation import EthAddress
from app.domain.entities.reference_risk_capital import ReferencePrimeRiskCapital
from app.ports.prime_directory import PrimeDirectory
from app.ports.reference_risk_capital import ReferenceRiskCapitalProvider
from app.services.star_resolution import star_for

logger = logging.getLogger(__name__)


class ReferenceRiskCapitalService:
    """Reads a prime's most recently observed risk-capital snapshot."""

    def __init__(self, provider: ReferenceRiskCapitalProvider, primes: PrimeDirectory) -> None:
        self._provider = provider
        self._primes = primes

    async def get(self, proxy_address: EthAddress) -> ReferencePrimeRiskCapital | None:
        """Return the observed snapshot for the prime owning ``proxy_address``.

        ``None`` means no reference figures exist for this prime — either the
        address names no prime STL knows, or no cycle has ever reported on that
        prime. Both are real answers about coverage, not failures, and neither
        may be served as zeros.
        """
        star = await star_for(proxy_address, self._primes)
        if star is None:
            return None

        return await self._provider.get_prime(star)

    async def covered_stars(self) -> frozenset[str]:
        """Every prime reference figures have been observed for, lowercased."""
        return await self._provider.covered_stars()
