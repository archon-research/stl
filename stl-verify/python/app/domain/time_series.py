"""Shared time-series query policy.

This module owns the normalization and validation rules for time-windowed
queries: the default and maximum window, the allowed downsampling frequencies,
the aggregation methods, and the window-to-frequency policy. It lives in the
domain layer so the policy is reusable by any caller (HTTP today, scheduled jobs
or other transports later) and is testable without FastAPI. It depends only on
the standard library.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import ClassVar

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


class TimeSeriesFrequency(StrEnum):
    """Allowed ISO-8601 durations for time-series resampling."""

    PT1M = "PT1M"
    PT5M = "PT5M"
    PT15M = "PT15M"
    PT1H = "PT1H"
    PT6H = "PT6H"
    P1D = "P1D"

    @property
    def duration(self) -> timedelta:
        return _FREQUENCY_TO_DURATION[self]

    @property
    def duration_ms(self) -> int:
        return int(self.duration.total_seconds() * 1000)


_FREQUENCY_TO_DURATION: dict["TimeSeriesFrequency", timedelta] = {
    TimeSeriesFrequency.PT1M: timedelta(minutes=1),
    TimeSeriesFrequency.PT5M: timedelta(minutes=5),
    TimeSeriesFrequency.PT15M: timedelta(minutes=15),
    TimeSeriesFrequency.PT1H: timedelta(hours=1),
    TimeSeriesFrequency.PT6H: timedelta(hours=6),
    TimeSeriesFrequency.P1D: timedelta(days=1),
}

# Fail at import time (not at request time) if a frequency lacks a duration.
_missing_durations = set(TimeSeriesFrequency) - set(_FREQUENCY_TO_DURATION)
if _missing_durations:
    raise RuntimeError(f"TimeSeriesFrequency members missing a duration mapping: {_missing_durations}")


# One member, because the bucketing in the Postgres adapters
# (``time_bucket_gapfill`` + ``locf(last(...))``) is end-period and nothing else
# is implemented. See ADR-0005 for the reserved methods and why they wait.
class AggregationMethod(StrEnum):
    """Resampling method applied to a resampled response.

    ``start-period``, ``period-mean`` and ``period-median`` are reserved names,
    not accepted values.
    """

    END_PERIOD = "end-period"


# Ceiling on the points a default-frequency response may carry. The frequency
# floor bounds a resampled response by construction but constrains row counts not
# at all, so one legal request against a dense series can ask for millions of
# stored observations. Sized above every window/floor pairing, so the floor is
# always a frequency the rejection can suggest.
MAX_POINTS = 50_000


class TimeSeriesQueryError(Exception):
    """A caller-fixable rejection of a time-series query.

    ``error_code`` is the stable, machine-readable slug the HTTP layer puts on the
    wire; the message is human-readable and never the only signal. Deliberately not
    a ``ValueError``: repository code raises these, and the routes wrap their reads
    in ``except ValueError`` to report a database failure — inheriting would let a
    422 carrying suggestions be downgraded to an opaque 500. Abstract here: only
    concrete subclasses carry a code, so reading ``.error_code`` off a bare instance
    fails loudly rather than leaking an undocumented slug into a response.
    """

    error_code: ClassVar[str]


class InvalidTimeRangeError(TimeSeriesQueryError):
    """The requested bounds are inverted."""

    error_code = "invalid_time_range"


class WindowTooLargeError(TimeSeriesQueryError):
    """The requested window exceeds the ceiling for the request."""

    error_code = "window_too_large"


class FrequencyTooFineError(TimeSeriesQueryError):
    """The requested frequency is finer than the window's floor allows."""

    error_code = "frequency_too_fine"


class FrequencyWithoutAggregationMethodError(TimeSeriesQueryError):
    """A frequency was supplied with no aggregation method to cut on it."""

    error_code = "frequency_requires_aggregation_method"


class MaxPointsExceededError(TimeSeriesQueryError):
    """The default-frequency response would carry more points than the ceiling.

    Carries a narrower window and a frequency, so a client re-tiles or resamples
    without parsing the message. The frequency fits by construction; the window is
    proportional to the window's average density and so an estimate — a series
    clustered in the suggested span is rejected again. See ``enforce_max_points``.
    """

    error_code = "max_points_exceeded"

    def __init__(
        self,
        *,
        point_count: int,
        max_points: int,
        suggested_from_timestamp: datetime | None,
        suggested_to_timestamp: datetime | None,
        suggested_frequency: TimeSeriesFrequency,
    ) -> None:
        narrower = (
            f"retry within {suggested_from_timestamp.isoformat()}/{suggested_to_timestamp.isoformat()} or "
            if suggested_from_timestamp is not None and suggested_to_timestamp is not None
            else ""
        )
        super().__init__(
            f"the requested window holds {point_count} observations, above the maximum of "
            f"{max_points} for a default-frequency response; {narrower}"
            f"request aggregation_method=end-period at frequency {suggested_frequency.value}"
        )
        self.point_count = point_count
        self.max_points = max_points
        self.suggested_from_timestamp = suggested_from_timestamp
        self.suggested_to_timestamp = suggested_to_timestamp
        self.suggested_frequency = suggested_frequency


@dataclass(frozen=True, kw_only=True)
class TimeWindow:
    """A resolved, validated UTC window.

    Both bounds are timezone-aware UTC datetimes. The invariants (bounds are
    aware, ``from <= to``) are enforced on the type itself, so any construction
    path is safe — not only the resolver factories. History and the bounded
    ``/latest`` lookback share it; they differ only in whether a frequency is
    part of the request.
    """

    from_timestamp: datetime
    to_timestamp: datetime
    bounds_pinned: bool = False
    """True when the caller pinned the window rather than letting a bound default
    to ``now``. Pinned windows are deterministic and therefore cacheable."""

    def __post_init__(self) -> None:
        # Plain ValueErrors: these are invariants on the type, so a breach is a
        # bug in whatever computed the bounds. The resolvers reject caller input
        # before construction, with the codes a client can act on.
        if self.from_timestamp.tzinfo is None or self.to_timestamp.tzinfo is None:
            raise ValueError("time-series bounds must be timezone-aware")
        if self.from_timestamp > self.to_timestamp:
            raise ValueError("from_timestamp must be less than or equal to to_timestamp")

    @property
    def window(self) -> timedelta:
        return self.to_timestamp - self.from_timestamp


@dataclass(frozen=True, kw_only=True)
class TimeSeriesQuery(TimeWindow):
    """Normalized, validated time-series query parameters shared across endpoints."""

    frequency: TimeSeriesFrequency
    aggregation_method: AggregationMethod | None = None

    @property
    def is_bucketed(self) -> bool:
        """True when the caller asked for resampled buckets.

        The presence of ``aggregation_method`` is the switch: absent means the
        stored frequency, present means buckets on the requested grid.
        """
        return self.aggregation_method is not None

    @property
    def frequency_ms(self) -> int:
        return self.frequency.duration_ms

    @property
    def bucket(self) -> timedelta:
        return self.frequency.duration


def minimum_frequency(window: timedelta) -> TimeSeriesFrequency:
    """Return the finest frequency permitted for a window of the given size."""
    if window <= timedelta(hours=6):
        return TimeSeriesFrequency.PT1M
    if window <= timedelta(hours=24):
        return TimeSeriesFrequency.PT5M
    if window <= timedelta(days=7):
        return TimeSeriesFrequency.PT15M
    if window <= timedelta(days=30):
        return TimeSeriesFrequency.PT1H
    return TimeSeriesFrequency.PT6H


# The earliest instant a window may reach back to. Stepping back from a bound
# close to it raises OverflowError, which is no rejection type and would surface
# as a 500 on an otherwise well-formed request.
_EARLIEST = datetime.min.replace(tzinfo=UTC)


def _to_utc(value: datetime) -> datetime:
    """Normalize a datetime to timezone-aware UTC, assuming naive inputs are UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    try:
        return value.astimezone(UTC)
    except OverflowError as exc:
        raise InvalidTimeRangeError(f"timestamp {value.isoformat()} is outside the representable range") from exc


def _step_back(moment: datetime, span: timedelta) -> datetime:
    """Step back by ``span``, stopping at the earliest representable instant."""
    try:
        return moment - span
    except OverflowError:
        return _EARLIEST


def resolve_time_series_query(
    *,
    from_timestamp: datetime | None,
    to_timestamp: datetime | None,
    frequency: TimeSeriesFrequency | None,
    now: datetime,
    aggregation_method: AggregationMethod | None = None,
    default_aggregation_method: AggregationMethod | None = None,
    default_window: timedelta = DEFAULT_WINDOW,
    max_window: timedelta = MAX_WINDOW,
) -> TimeSeriesQuery:
    """Apply defaults, normalize to UTC, and validate a time-series request.

    ``default_aggregation_method`` is how a route that only ever answers with a
    resampled series names its method, so its query is resampled whether or not
    the caller spelled one.

    ``now`` is injected so the function stays pure and testable. Raises a
    ``TimeSeriesQueryError`` on an inverted range, a window exceeding
    ``max_window``, a frequency supplied without a method, or a frequency finer
    than the window's minimum.
    """
    effective_method = aggregation_method or default_aggregation_method
    # A frequency names the grid a method cuts on, so without one it would be
    # validated and then dropped — the silent no-op the echo cannot report.
    if frequency is not None and effective_method is None:
        raise FrequencyWithoutAggregationMethodError(
            "frequency names the grid an aggregation_method cuts on; "
            "supply aggregation_method=end-period or omit frequency"
        )
    resolved_to = _to_utc(to_timestamp) if to_timestamp is not None else _to_utc(now)
    resolved_from = _to_utc(from_timestamp) if from_timestamp is not None else _step_back(resolved_to, default_window)
    bounds_pinned = from_timestamp is not None and to_timestamp is not None

    if resolved_from > resolved_to:
        raise InvalidTimeRangeError("from_timestamp must be less than or equal to to_timestamp")

    window = resolved_to - resolved_from
    if window > max_window:
        raise WindowTooLargeError(f"requested window of {window} exceeds the maximum allowed of {max_window}")

    floor = minimum_frequency(window)
    effective_frequency = frequency or floor
    if effective_frequency.duration < floor.duration:
        raise FrequencyTooFineError(
            f"frequency is too fine for the selected window; minimum allowed frequency is {floor.value}"
        )

    return TimeSeriesQuery(
        from_timestamp=resolved_from,
        to_timestamp=resolved_to,
        frequency=effective_frequency,
        aggregation_method=effective_method,
        bounds_pinned=bounds_pinned,
    )


def resolve_latest_query(
    *,
    to_timestamp: datetime | None,
    now: datetime,
    max_window: timedelta = MAX_WINDOW,
) -> TimeWindow:
    """Resolve the bounded lookback behind a ``/latest`` request.

    ``/latest`` answers with the newest observation at or before ``to_timestamp``,
    which defaults to ``now``. The backward search is bounded by ``max_window`` so
    an as-of read on a sparse series cannot degenerate into a full-history scan,
    and an observation older than that reads as absent rather than as latest.
    The caller echoes the returned window, which is what makes an omitted upper
    bound visible in the response.
    """
    resolved_to = _to_utc(to_timestamp) if to_timestamp is not None else _to_utc(now)
    return TimeWindow(
        from_timestamp=_step_back(resolved_to, max_window),
        to_timestamp=resolved_to,
        bounds_pinned=to_timestamp is not None,
    )


def enforce_max_points(point_count: int, *, query: TimeWindow, max_points: int = MAX_POINTS) -> None:
    """Reject a default-frequency response carrying more than ``max_points`` points.

    Rejecting, not truncating and not paginating: a statistic computed over a
    silently shortened series is wrong and looks right, and pagination would let a
    correction landing between pages assemble a series that never existed. Because
    rejection replaces the truncation flag, the suggestions carried by the raised
    error are the only machine-readable way out — a client re-tiles the window or
    drops to the suggested frequency without parsing the message.

    The window suggestion is proportional: ``max_points / point_count`` of the
    requested span, anchored at the upper bound. That is exact only for evenly
    spaced observations — a series clustered in the trailing span holds more than
    ``max_points`` there and is rejected again, on a span the same ratio narrows
    further each round, so a re-tiling client converges geometrically. The
    frequency suggestion fits by construction, which is the one-round-trip way out.

    ``point_count`` must be counted over the same latest-version rows the response
    would carry, from the same snapshot; see ``read_bounded_series``.
    """
    if point_count <= max_points:
        return
    fitting_span = timedelta(seconds=int(query.window.total_seconds() * max_points / point_count))
    # Below a second the proportional window rounds to nothing, and suggesting a
    # window no narrower than the rejected one sends a re-tiling client round a
    # loop. Such a series is too dense to serve unresampled at any width, so the
    # frequency is the only honest way out and the window suggestion is omitted.
    fits = fitting_span >= timedelta(seconds=1)
    raise MaxPointsExceededError(
        point_count=point_count,
        max_points=max_points,
        suggested_from_timestamp=_step_back(query.to_timestamp, fitting_span) if fits else None,
        suggested_to_timestamp=query.to_timestamp if fits else None,
        # Every frequency the floor permits keeps a resampled response under
        # MAX_POINTS, so the floor is both the finest and a fitting suggestion.
        suggested_frequency=minimum_frequency(query.window),
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

    Raises ``WindowTooLargeError`` (mapped to HTTP 422 at the boundary) when no selective
    filter is present and the window exceeds ``unfiltered_max_window``.
    """
    if has_selective_filter:
        return
    if query.window > unfiltered_max_window:
        raise WindowTooLargeError(
            f"requested window of {query.window} exceeds the maximum allowed of "
            f"{unfiltered_max_window} for queries without a selective filter; "
            "narrow the window or add a selective filter (e.g. tx_hash or an exact id)"
        )
