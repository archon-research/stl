from datetime import UTC, datetime, timedelta

import pytest
from fastapi import HTTPException

from app.api.time_series import (
    TimeSeriesResolution,
    get_time_series_query_params,
)


def test_defaults_to_last_24h_window_and_window_based_resolution() -> None:
    params = get_time_series_query_params(
        from_timestamp=None,
        to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
        resolution=None,
    )

    assert params.from_timestamp == datetime(2026, 3, 4, 12, 0)
    assert params.to_timestamp == datetime(2026, 3, 5, 12, 0)
    assert params.resolution == TimeSeriesResolution.PT5M
    assert params.interval_ms == 5 * 60 * 1000


def test_rejects_inverted_time_range() -> None:
    with pytest.raises(HTTPException, match="from_timestamp"):
        get_time_series_query_params(
            from_timestamp=datetime(2026, 3, 6, 0, 0, tzinfo=UTC),
            to_timestamp=datetime(2026, 3, 5, 0, 0, tzinfo=UTC),
            resolution=TimeSeriesResolution.PT5M,
        )


def test_rejects_resolution_finer_than_window_minimum() -> None:
    with pytest.raises(HTTPException, match="minimum allowed resolution"):
        get_time_series_query_params(
            from_timestamp=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            to_timestamp=datetime(2026, 2, 15, 0, 0, tzinfo=UTC),
            resolution=TimeSeriesResolution.PT1M,
        )


def test_accepts_resolution_when_not_finer_than_window_minimum() -> None:
    params = get_time_series_query_params(
        from_timestamp=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        to_timestamp=datetime(2026, 2, 15, 0, 0, tzinfo=UTC),
        resolution=TimeSeriesResolution.P1D,
    )

    assert params.resolution == TimeSeriesResolution.P1D
    assert params.interval_ms == int(timedelta(days=1).total_seconds() * 1000)
