from datetime import UTC, datetime, timedelta
from enum import StrEnum

from fastapi import HTTPException, Query
from pydantic import BaseModel, Field

DEFAULT_WINDOW = timedelta(hours=24)


class TimeSeriesResolution(StrEnum):
    """Allowed ISO-8601 durations for time-series downsampling."""

    PT1M = "PT1M"
    PT5M = "PT5M"
    PT15M = "PT15M"
    PT1H = "PT1H"
    PT6H = "PT6H"
    P1D = "P1D"


_RESOLUTION_TO_DURATION = {
    TimeSeriesResolution.PT1M: timedelta(minutes=1),
    TimeSeriesResolution.PT5M: timedelta(minutes=5),
    TimeSeriesResolution.PT15M: timedelta(minutes=15),
    TimeSeriesResolution.PT1H: timedelta(hours=1),
    TimeSeriesResolution.PT6H: timedelta(hours=6),
    TimeSeriesResolution.P1D: timedelta(days=1),
}


class TimeSeriesQueryParams(BaseModel):
    """Normalized time-series query parameters shared across endpoints."""

    from_timestamp: datetime = Field(description="Inclusive lower timestamp bound (UTC).")
    to_timestamp: datetime = Field(description="Inclusive upper timestamp bound (UTC).")
    resolution: TimeSeriesResolution = Field(description="ISO-8601 duration enum for requested resolution.")
    interval_ms: int = Field(description="Resolution in milliseconds.")


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _minimum_resolution(window: timedelta) -> TimeSeriesResolution:
    if window <= timedelta(hours=6):
        return TimeSeriesResolution.PT1M
    if window <= timedelta(hours=24):
        return TimeSeriesResolution.PT5M
    if window <= timedelta(days=7):
        return TimeSeriesResolution.PT15M
    if window <= timedelta(days=30):
        return TimeSeriesResolution.PT1H
    return TimeSeriesResolution.PT6H


def get_time_series_query_params(
    from_timestamp: datetime | None = Query(
        default=None,
        description="Inclusive lower timestamp bound (ISO-8601). Defaults to 24h before `to_timestamp`.",
    ),
    to_timestamp: datetime | None = Query(
        default=None,
        description="Inclusive upper timestamp bound (ISO-8601). Defaults to current UTC time.",
    ),
    resolution: TimeSeriesResolution | None = Query(
        default=None,
        description="ISO-8601 duration resolution (for example `PT5M`, `PT1H`).",
    ),
) -> TimeSeriesQueryParams:
    now_utc = datetime.now(UTC)
    normalized_to = _normalize_utc(to_timestamp) if to_timestamp is not None else now_utc
    normalized_from = _normalize_utc(from_timestamp) if from_timestamp is not None else normalized_to - DEFAULT_WINDOW

    if normalized_from > normalized_to:
        raise HTTPException(status_code=422, detail="from_timestamp must be less than or equal to to_timestamp")

    window = normalized_to - normalized_from
    min_resolution = _minimum_resolution(window)
    effective_resolution = resolution or min_resolution

    if _RESOLUTION_TO_DURATION[effective_resolution] < _RESOLUTION_TO_DURATION[min_resolution]:
        raise HTTPException(
            status_code=422,
            detail=(
                "resolution is too fine for the selected window; "
                f"minimum allowed resolution is {min_resolution.value}"
            ),
        )

    return TimeSeriesQueryParams(
        from_timestamp=normalized_from,
        to_timestamp=normalized_to,
        resolution=effective_resolution,
        interval_ms=int(_RESOLUTION_TO_DURATION[effective_resolution].total_seconds() * 1000),
    )
