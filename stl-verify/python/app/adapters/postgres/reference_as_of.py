"""The effective instant a read resolves reference-table versions at (ADR-0006 §4).

Which version of an append-on-change reference table applies is decided by
``valid_from <= effective_at``, and that ``effective_at`` must be a parameter the reader
supplies, so a replay can pass the instant the original read used. The provider is
injected so a calculation can pin its whole run to one recorded instant; the default is
now (UTC), which reproduces the pre-conversion behaviour.
"""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

ReferenceEffectiveAtProvider = Callable[[], datetime]


def utc_now() -> datetime:
    """Now in UTC, timezone-aware, so the bound value is an absolute instant."""
    return datetime.now(UTC)


def pinned_to(effective_at: datetime | None) -> ReferenceEffectiveAtProvider:
    """A provider fixed to effective_at, or the default when it is None.

    A naive value is taken as UTC, because the settings accept a bare ``YYYY-MM-DD``.
    """
    if effective_at is None:
        return utc_now
    if effective_at.tzinfo is None:
        effective_at = effective_at.replace(tzinfo=UTC)
    return lambda: effective_at


class ReferenceAsOf:
    """Binds the reference effective instant into a query's parameters."""

    def __init__(self, provider: ReferenceEffectiveAtProvider = utc_now) -> None:
        self._provider = provider

    @property
    def effective_at(self) -> datetime:
        return self._provider()

    def params(self, **query_params: Any) -> dict[str, Any]:
        """The query's own parameters plus the reference effective instant.

        Resolved once per call, so every reference read in one query sees one instant.
        """
        return {**query_params, "reference_effective_at": self._provider()}
