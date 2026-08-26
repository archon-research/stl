"""Reference (upstream) balance-sheet positions for a prime, as STL observed it.

The allocation-list counterpart to
:class:`~app.services.reference_risk_capital_service.ReferenceRiskCapitalService`.
That service answers "what does Sky say this prime's *risk capital* is"; this
one answers "what does Sky say this prime *holds*". They read different tables
and different quantities — see :mod:`app.domain.entities.reference_position`.
"""

from app.domain.entities.allocation import EthAddress
from app.domain.entities.reference_position import ReferencePositionSnapshot
from app.ports.prime_directory import PrimeDirectory
from app.ports.reference_positions import ReferencePositionProvider
from app.services.star_resolution import star_for


class ReferencePositionsService:
    """Reads a prime's most recently observed balance sheet."""

    def __init__(self, positions: ReferencePositionProvider, primes: PrimeDirectory) -> None:
        self._positions = positions
        self._primes = primes

    async def get(self, proxy_address: EthAddress) -> ReferencePositionSnapshot | None:
        """Return the observed positions for the prime owning ``proxy_address``.

        ``None`` when the address names no prime STL knows, and whatever the
        reader answers otherwise — which is also ``None`` for a prime no cycle
        has reported on. Both are coverage answers, not failures.
        """
        star = await star_for(proxy_address, self._primes)
        if star is None:
            return None

        return await self._positions.get_positions(star)
