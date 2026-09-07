"""FastAPI integration for the shared time-series query policy.

This is the inbound adapter for the domain ``time_series`` policy: it declares
the HTTP query parameters, delegates normalization/validation to the domain
resolver, and maps domain ``ValueError``s to HTTP 422. The response envelope
types live here too, since they are an HTTP-contract concern.
"""

from datetime import UTC, datetime

from fastapi import HTTPException, Query, Response
from pydantic import BaseModel, Field

from app.domain.time_series import (
    TimeSeriesQuery,
    TimeSeriesResolution,
    resolve_time_series_query,
)

# Public cache lifetime for responses with a pinned window. Pinned-window
# responses cannot change going forward (the underlying rows are immutable once
# observed), so a long TTL is safe and dramatically reduces hypertable load.
_PINNED_WINDOW_CACHE_MAX_AGE_SECONDS = 300


def get_time_series_query_params(
    from_timestamp: datetime | None = Query(
        default=None,
        description="Inclusive lower timestamp bound (ISO-8601). Defaults to 24h before `to_timestamp`.",
    ),
    to_timestamp: datetime | None = Query(
        default=None,
        description="Inclusive upper timestamp bound (ISO-8601). Defaults to the current UTC time.",
    ),
    resolution: TimeSeriesResolution | None = Query(
        default=None,
        description=(
            "ISO-8601 duration resolution (for example `PT5M`, `PT1H`). Used for time-bucketing "
            "when `aggregate=true`; defaults to the finest resolution allowed for the window."
        ),
    ),
    aggregate: bool = Query(
        default=False,
        description="When true, return time-bucketed aggregates instead of raw rows.",
    ),
) -> TimeSeriesQuery:
    try:
        return resolve_time_series_query(
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            resolution=resolution,
            aggregate=aggregate,
            now=datetime.now(UTC),
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


class TimeSeriesWindow(BaseModel):
    """The resolved window and resolution actually applied to a request.

    Echoing this back lets consumers distinguish an empty result caused by the
    window from one caused by the absence of data.
    """

    from_timestamp: datetime = Field(description="Inclusive lower bound applied (UTC).")
    to_timestamp: datetime = Field(description="Inclusive upper bound applied (UTC).")
    resolution: TimeSeriesResolution = Field(description="Resolution applied (relevant when aggregated).")
    interval_ms: int = Field(description="Resolution width in milliseconds.")


def build_window(query: TimeSeriesQuery) -> TimeSeriesWindow:
    """Build the response window descriptor from a resolved query."""
    return TimeSeriesWindow(
        from_timestamp=query.from_timestamp,
        to_timestamp=query.to_timestamp,
        resolution=query.resolution,
        interval_ms=query.interval_ms,
    )


def apply_cache_control(response: Response, query: TimeSeriesQuery) -> None:
    """Set ``Cache-Control`` on responses whose window is fully pinned by the caller.

    When ``to_timestamp`` is defaulted to ``now``, two requests one second apart
    return different windows, so the response must not be cached. When both
    bounds are supplied explicitly the window is deterministic and the response
    is safe to cache publicly for a short period.
    """
    if query.bounds_pinned:
        response.headers["Cache-Control"] = f"public, max-age={_PINNED_WINDOW_CACHE_MAX_AGE_SECONDS}"
    else:
        response.headers["Cache-Control"] = "no-store"
