"""Reference (upstream) balance-sheet positions for a prime, as STL observed it.

The allocation-list counterpart to
:class:`~app.services.reference_risk_capital_service.ReferenceRiskCapitalService`.
That service answers "what does Sky say this prime's *risk capital* is"; this
one answers "what does Sky say this prime *holds*". They read different tables
and different quantities — see :mod:`app.domain.entities.reference_position`.

Coverage is decided by the risk-capital snapshot, not by this feed's rows; the
reader owns that join and records why. Keeping one answer to it stops the
allocation list and the risk-capital card disagreeing about whether a prime is
covered.
"""

import logging

from app.domain.entities.allocation import EthAddress
from app.domain.entities.reference_position import ReferencePositionSnapshot
from app.ports.prime_directory import PrimeDirectory
from app.ports.reference_positions import ReferencePositionProvider
from app.services.star_resolution import star_for

logger = logging.getLogger(__name__)


class ReferencePositionsService:
    """Reads a prime's most recently observed balance sheet."""

    def __init__(self, positions: ReferencePositionProvider, primes: PrimeDirectory) -> None:
        self._positions = positions
        self._primes = primes

    async def get(self, proxy_address: EthAddress) -> ReferencePositionSnapshot | None:
        """Return the observed positions for the prime owning ``proxy_address``.

        ``None`` means no reference data exists for this prime — either the
        address names no prime STL knows, or no cycle has ever reported on that
        prime. Both are real answers about coverage, not failures, and neither
        may be served as an empty list: an empty list is a prime that holds
        nothing, which is a different claim.
        """
        star = await star_for(proxy_address, self._primes)
        if star is None:
            return None

        return await self._positions.get_positions(star)
