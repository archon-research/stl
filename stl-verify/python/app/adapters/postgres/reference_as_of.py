"""The effective date a read resolves reference-table versions at (ADR-0006 §4).

Reference tables like ``oracle_asset`` are append-on-change: which version applies is
decided by ``valid_from <= effective_at``. That ``effective_at`` must be an explicit
parameter the reader supplies, never ``now()``/``CURRENT_DATE`` inside the SQL — a replay
has to be able to pass the date the original read used and get the same rows back. The
``_current`` views evaluate the wall clock instead, which is why they are for operational
reads only.

The provider is injected so a calculation can pin its whole run to one recorded date;
until then the default is today (UTC), which reproduces the pre-conversion behaviour while
keeping the date visible in the SQL parameters rather than buried in the query.
"""

from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any

# Returns the date whose reference-data versions a read should resolve.
ReferenceEffectiveAtProvider = Callable[[], date]


def utc_today() -> date:
    """Today in UTC, so the answer does not shift with the server's local timezone."""
    return datetime.now(UTC).date()


class ReferenceAsOf:
    """Binds the reference effective date into a query's parameters."""

    def __init__(self, provider: ReferenceEffectiveAtProvider = utc_today) -> None:
        self._provider = provider

    @property
    def effective_at(self) -> date:
        return self._provider()

    def params(self, **query_params: Any) -> dict[str, Any]:
        """The query's own parameters plus the reference effective date.

        One resolution per call, so every reference read in a single query sees one date.
        """
        return {**query_params, "reference_effective_at": self._provider()}
