"""Outbound port for Sky's upstream balance-sheet feed."""

from typing import Protocol

from app.domain.entities.reference_position import ReferencePosition


class ReferencePositionProvider(Protocol):
    """Fetch every position a star holds, as upstream reports its balance sheet."""

    async def get_positions(self, star: str) -> tuple[ReferencePosition, ...]:
        """Return ``star``'s positions.

        An empty tuple means upstream reported a prime holding nothing, which
        only the caller can judge: this feed answers an unknown star with ``200``
        and an empty list, so a caller that has not already established the star
        is covered must not read the result as "holds nothing".
        """
        ...
