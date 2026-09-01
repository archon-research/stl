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

ORACLE_ASSET_AS_OF = """(
    SELECT DISTINCT ON (oracle_id, token_id, feed_key) *
    FROM oracle_asset
    WHERE valid_from <= :reference_effective_at
    ORDER BY oracle_id, token_id, feed_key, valid_from DESC, processing_version DESC
)"""
"""The pinned oracle_asset read, interpolated into a query as a derived table.

Holds the version of each natural key (oracle_id, token_id, feed_key) effective at
``:reference_effective_at``, which ``ReferenceAsOf.params`` binds. Disabled versions are
returned too, so a caller filtering on ``enabled`` can still tell "retired then" from
"never registered".
"""


def utc_now() -> datetime:
    """Now in UTC, timezone-aware, so the bound value is an absolute instant."""
    return datetime.now(UTC)


def pinned_to(effective_at: datetime | None) -> ReferenceEffectiveAtProvider:
    """A provider fixed to effective_at in UTC, or the default when it is None.

    A naive value is rejected rather than assumed to be UTC: bound naive, Postgres reads it
    in the session's TimeZone instead of as an absolute instant.
    """
    if effective_at is None:
        return utc_now
    if effective_at.tzinfo is None:
        raise ValueError(f"reference effective instant {effective_at.isoformat()} carries no timezone")
    pinned = effective_at.astimezone(UTC)
    return lambda: pinned


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
