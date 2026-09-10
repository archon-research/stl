"""Outbound port for STL's stored reference balance-sheet positions."""

from typing import Protocol

from app.domain.entities.reference_position import ReferencePositionSnapshot


class ReferencePositionProvider(Protocol):
    """Read every position a star held at its most recently observed cycle."""

    async def get_positions(self, star: str) -> ReferencePositionSnapshot | None:
        """Return ``star``'s latest observed balance sheet, or ``None`` if none exists.

        ``None`` means no reference data has been observed for the prime, which
        is a real answer about coverage rather than a failure, and must not be
        served as a prime holding nothing.
        """
        ...
