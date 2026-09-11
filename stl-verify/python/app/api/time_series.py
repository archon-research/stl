"""FastAPI integration for the shared time-series query policy.

This is the inbound adapter for the domain ``time_series`` policy: it declares
the HTTP query parameters and delegates normalization/validation to the domain
resolvers, whose rejections travel as ``TimeSeriesQueryError`` and are rendered
by the shared handler in ``app.api.errors``. The response envelope types live
here too, since they are an HTTP-contract concern.
"""

from datetime import UTC, datetime

from fastapi import Query, Response
from pydantic import BaseModel, Field, SerializerFunctionWrapHandler, model_serializer

from app.domain.time_series import (
    AggregationMethod,
    TimeSeriesFrequency,
    TimeSeriesQuery,
    TimeWindow,
    resolve_latest_query,
    resolve_time_series_query,
)

# Public cache lifetime for responses with a pinned window. A pinned window can
# still change — a correction or a backfill writes inside a window already served
# — but only those two can, and they are rare, so bounded staleness on a copy we
# cannot recall is the accepted trade.
_PINNED_WINDOW_CACHE_MAX_AGE_SECONDS = 300

# Shared by both dependencies below, which differ only in how they default the
# aggregation method.
_FREQUENCY_DESCRIPTION = (
    "ISO-8601 duration frequency for the resampled grid (for example `PT5M`, `PT1H`). Always "
    "validated against the window's floor, and rejected without an `aggregation_method` to cut "
    "on it. Defaults to the finest frequency the window allows."
)


def get_time_series_query_params(
    from_timestamp: datetime | None = Query(
        default=None,
        description="Inclusive lower timestamp bound (ISO-8601). Defaults to 24h before `to_timestamp`.",
    ),
    to_timestamp: datetime | None = Query(
        default=None,
        description="Inclusive upper timestamp bound (ISO-8601). Defaults to the current UTC time.",
    ),
    frequency: TimeSeriesFrequency | None = Query(
        default=None,
        description=_FREQUENCY_DESCRIPTION,
    ),
    aggregation_method: AggregationMethod | None = Query(
        default=None,
        description=(
            "Resampling method for the returned grid. Supplying it returns a resampled series; "
            "omitting it returns the series at its stored frequency. `end-period` is the only "
            "accepted value."
        ),
    ),
) -> TimeSeriesQuery:
    return resolve_time_series_query(
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        frequency=frequency,
        aggregation_method=aggregation_method,
        now=datetime.now(UTC),
    )


def get_resampled_time_series_query_params(
    from_timestamp: datetime | None = Query(
        default=None,
        description="Inclusive lower timestamp bound (ISO-8601). Defaults to 24h before `to_timestamp`.",
    ),
    to_timestamp: datetime | None = Query(
        default=None,
        description="Inclusive upper timestamp bound (ISO-8601). Defaults to the current UTC time.",
    ),
    frequency: TimeSeriesFrequency | None = Query(
        default=None,
        description=_FREQUENCY_DESCRIPTION,
    ),
    aggregation_method: AggregationMethod | None = Query(
        default=AggregationMethod.END_PERIOD,
        description=(
            "Resampling method for the returned grid. This route only serves a resampled series, "
            "so omitting it applies `end-period`, the only accepted value."
        ),
    ),
) -> TimeSeriesQuery:
    """The dependency for a route with no default-frequency mode.

    Defaulting the method here rather than ignoring it keeps ``is_bucketed``
    true on a route whose answer is always buckets, so the resolved query and
    the response agree.
    """
    return resolve_time_series_query(
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        frequency=frequency,
        aggregation_method=aggregation_method,
        default_aggregation_method=AggregationMethod.END_PERIOD,
        now=datetime.now(UTC),
    )


def get_latest_query_params(
    to_timestamp: datetime | None = Query(
        default=None,
        description=(
            "As-of upper bound (ISO-8601, inclusive): the newest observation at or before it is "
            "returned. Defaults to the current UTC time, which the response's window echo reports."
        ),
    ),
) -> TimeWindow:
    """The dependency for a ``/latest`` route: an as-of bound over a bounded lookback."""
    return resolve_latest_query(to_timestamp=to_timestamp, now=datetime.now(UTC))


class BucketPoint(BaseModel):
    """Base for one point of a bucketed series, carrying the filled marker.

    The wire form of the marker ``app.domain.gap_policy`` sets: serialized only
    where it is true, so a response pays one extra field per filled point
    rather than one per point.
    """

    filled: bool = Field(
        default=False,
        description=(
            "Present and `true` only when the value was carried into an empty bucket rather "
            "than observed in it. Absent means it was not carried: the bucket was either "
            "observed, or it precedes the series' first observation and its value is `null`."
        ),
    )

    # Deliberately unannotated: pydantic derives a subclass' published
    # serialization schema from this return type, and any annotation it can read
    # replaces the fields with a bare object.
    @model_serializer(mode="wrap")
    def _drop_unfilled_marker(self, handler: SerializerFunctionWrapHandler):
        serialized = handler(self)
        if not self.filled:
            serialized.pop("filled", None)
        return serialized


class TimeSeriesWindow(BaseModel):
    """The resolved window applied to a request.

    Echoing this back lets consumers distinguish an empty result caused by the
    window from one caused by the absence of data.
    """

    from_timestamp: datetime = Field(description="Inclusive lower bound applied (UTC).")
    to_timestamp: datetime = Field(description="Inclusive upper bound applied (UTC).")


class ResampledTimeSeriesWindow(TimeSeriesWindow):
    """The window echo for a resampled response, naming the grid it sits on.

    A default-frequency response carries the points at their stored frequency,
    so it echoes the bare window above — a frequency there would name a grid the
    points are not on, and a `null` would still put the key on the wire.
    """

    frequency: TimeSeriesFrequency = Field(description="Resampled grid the points sit on.")
    frequency_ms: int = Field(description="`frequency` in milliseconds.")


def build_raw_window(query: TimeWindow) -> TimeSeriesWindow:
    """The echo for a response with no grid: the unresampled arm of a route, and ``/latest``."""
    return TimeSeriesWindow(from_timestamp=query.from_timestamp, to_timestamp=query.to_timestamp)


def build_resampled_window(query: TimeSeriesQuery) -> ResampledTimeSeriesWindow:
    """The echo for a route that only ever answers with a resampled series."""
    return ResampledTimeSeriesWindow(
        from_timestamp=query.from_timestamp,
        to_timestamp=query.to_timestamp,
        frequency=query.frequency,
        frequency_ms=query.frequency_ms,
    )


def apply_cache_control(response: Response, query: TimeWindow) -> None:
    """Set ``Cache-Control`` on responses whose window is fully pinned by the caller.

    When a bound is defaulted to ``now``, two requests one second apart answer
    over different windows, so the response must not be cached. A pinned window
    is deterministic and is cached publicly for a short period.
    """
    if query.bounds_pinned:
        response.headers["Cache-Control"] = f"public, max-age={_PINNED_WINDOW_CACHE_MAX_AGE_SECONDS}"
    else:
        response.headers["Cache-Control"] = "no-store"
