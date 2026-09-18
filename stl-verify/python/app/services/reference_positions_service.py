"""Reference (upstream) balance-sheet positions for a prime, as STL observed it.

The allocation-list counterpart to
:class:`~app.services.reference_risk_capital_service.ReferenceRiskCapitalService`.
That service answers "what does Sky say this prime's *risk capital* is"; this
one answers "what does Sky say this prime *holds*". They read different tables
and different quantities — see :mod:`app.domain.entities.reference_position`.
"""

from app.domain.entities.reference_position import ReferencePositionSnapshot
from app.ports.reference_positions import ReferencePositionProvider


class ReferencePositionsService:
    """Reads a prime's most recently observed balance sheet."""

    def __init__(self, positions: ReferencePositionProvider) -> None:
        self._positions = positions

    async def get(self, star: str) -> ReferencePositionSnapshot | None:
        """Return the observed positions for the prime named ``star``.

        ``None`` for a prime no cycle has reported on — a coverage answer, not a
        failure.
        """
        return await self._positions.get_positions(star)
