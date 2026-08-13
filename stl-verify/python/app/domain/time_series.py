"""Shared time-series query policy.

This module owns the normalization and validation rules for time-windowed
queries: the default and maximum window, the allowed downsampling resolutions,
and the window-to-resolution policy. It lives in the domain layer so the policy
is reusable by any caller (HTTP today, scheduled jobs or other transports later)
and is testable without FastAPI. It depends only on the standard library.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

# Default window applied when a caller omits one or both bounds.
DEFAULT_WINDOW = timedelta(hours=24)

# Hard ceiling on the requested window. Bounds the worst-case scan/aggregation
# cost so a single request cannot ask for an unbounded range.
MAX_WINDOW = timedelta(days=366)

# Ceiling on windows with no selective filter. Our hypertable indexes lead with
# entity columns (prime_id, protocol_id) rather than time, so an unfiltered scan
# of a wide window degenerates to a full hypertable read. Endpoints supply
# whether a selective filter is present and call ``enforce_filter_for_window``.
UNFILTERED_MAX_WINDOW = timedelta(days=30)


class TimeSeriesResolution(StrEnum):
    """Allowed ISO-8601 durations for time-series downsampling."""

    PT1M = "PT1M"
    PT5M = "PT5M"
    PT15M = "PT15M"
    PT1H = "PT1H"
    PT6H = "PT6H"
    P1D = "P1D"

    @property
    def duration(self) -> timedelta:
        return _RESOLUTION_TO_DURATION[self]

    @property
    def interval_ms(self) -> int:
        return int(self.duration.total_seconds() * 1000)


_RESOLUTION_TO_DURATION: dict["TimeSeriesResolution", timedelta] = {
    TimeSeriesResolution.PT1M: timedelta(minutes=1),
    TimeSeriesResolution.PT5M: timedelta(minutes=5),
    TimeSeriesResolution.PT15M: timedelta(minutes=15),
    TimeSeriesResolution.PT1H: timedelta(hours=1),
    TimeSeriesResolution.PT6H: timedelta(hours=6),
    TimeSeriesResolution.P1D: timedelta(days=1),
}

# Fail at import time (not at request time) if a resolution lacks a duration.
_missing_durations = set(TimeSeriesResolution) - set(_RESOLUTION_TO_DURATION)
if _missing_durations:
    raise RuntimeError(f"TimeSeriesResolution members missing a duration mapping: {_missing_durations}")


@dataclass(frozen=True)
class TimeSeriesQuery:
    """Normalized, validated time-series query parameters shared across endpoints.

    Both bounds are timezone-aware UTC datetimes. The invariants (bounds are
    aware, ``from <= to``) are enforced on the type itself, so any construction
    path is safe — not only the resolver factory.
    """

    from_timestamp: datetime
    to_timestamp: datetime
    resolution: TimeSeriesResolution
    aggregate: bool = False
    bounds_pinned: bool = False
    """True when both bounds were explicitly supplied by the caller (vs. defaulted
    to ``now``). Pinned windows are deterministic and therefore cacheable."""

    def __post_init__(self) -> None:
        if self.from_timestamp.tzinfo is None or self.to_timestamp.tzinfo is None:
            raise ValueError("time-series bounds must be timezone-aware")
        if self.from_timestamp > self.to_timestamp:
            raise ValueError("from_timestamp must be less than or equal to to_timestamp")

    @property
    def window(self) -> timedelta:
        return self.to_timestamp - self.from_timestamp

    @property
    def interval_ms(self) -> int:
        return self.resolution.interval_ms

    @property
    def bucket(self) -> timedelta:
        return self.resolution.duration


def minimum_resolution(window: timedelta) -> TimeSeriesResolution:
    """Return the finest resolution permitted for a window of the given size."""
    if window <= timedelta(hours=6):
        return TimeSeriesResolution.PT1M
    if window <= timedelta(hours=24):
        return TimeSeriesResolution.PT5M
    if window <= timedelta(days=7):
        return TimeSeriesResolution.PT15M
    if window <= timedelta(days=30):
        return TimeSeriesResolution.PT1H
    return TimeSeriesResolution.PT6H


def _to_utc(value: datetime) -> datetime:
    """Normalize a datetime to timezone-aware UTC, assuming naive inputs are UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def resolve_time_series_query(
    *,
    from_timestamp: datetime | None,
    to_timestamp: datetime | None,
    resolution: TimeSeriesResolution | None,
    now: datetime,
    aggregate: bool = False,
    default_window: timedelta = DEFAULT_WINDOW,
    max_window: timedelta = MAX_WINDOW,
) -> TimeSeriesQuery:
    """Apply defaults, normalize to UTC, and validate a time-series request.

    ``now`` is injected so the function stays pure and testable. Raises
    ``ValueError`` on an inverted range, a window exceeding ``max_window``, or a
    resolution finer than the window's minimum.
    """
    resolved_to = _to_utc(to_timestamp) if to_timestamp is not None else _to_utc(now)
    resolved_from = _to_utc(from_timestamp) if from_timestamp is not None else resolved_to - default_window
    bounds_pinned = from_timestamp is not None and to_timestamp is not None

    if resolved_from > resolved_to:
        raise ValueError("from_timestamp must be less than or equal to to_timestamp")

    window = resolved_to - resolved_from
    if window > max_window:
        raise ValueError(f"requested window of {window} exceeds the maximum allowed of {max_window}")

    floor = minimum_resolution(window)
    effective_resolution = resolution or floor
    if effective_resolution.duration < floor.duration:
        raise ValueError(f"resolution is too fine for the selected window; minimum allowed resolution is {floor.value}")

    return TimeSeriesQuery(
        from_timestamp=resolved_from,
        to_timestamp=resolved_to,
        resolution=effective_resolution,
        aggregate=aggregate,
        bounds_pinned=bounds_pinned,
    )


def enforce_filter_for_window(
    query: TimeSeriesQuery,
    *,
    has_selective_filter: bool,
    unfiltered_max_window: timedelta = UNFILTERED_MAX_WINDOW,
) -> None:
    """Reject wide unfiltered windows that would scan the hypertable end-to-end.

    A "selective filter" is one the storage layer can use to prune chunks or seek
    into an index without falling back to a full hypertable scan — typically a
    path id, an exact equality on an indexed column, or a tx-hash exact match.
    Substring/`LIKE` filters do not qualify because they cannot use the indexes.

    Raises ``ValueError`` (mapped to HTTP 422 at the boundary) when no selective
    filter is present and the window exceeds ``unfiltered_max_window``.
    """
    if has_selective_filter:
        return
    if query.window > unfiltered_max_window:
        raise ValueError(
            f"requested window of {query.window} exceeds the maximum allowed of "
            f"{unfiltered_max_window} for queries without a selective filter; "
            "narrow the window or add a selective filter (e.g. tx_hash or an exact id)"
        )
