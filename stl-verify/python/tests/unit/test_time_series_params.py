from datetime import UTC, datetime, timedelta

import pytest
from fastapi import HTTPException, Response

from app.api.time_series import apply_cache_control, build_window, get_time_series_query_params
from app.domain.time_series import TimeSeriesResolution


def test_returns_resolved_query_with_defaults() -> None:
    query = get_time_series_query_params(
        from_timestamp=None,
        to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
        resolution=None,
        aggregate=False,
    )

    assert query.from_timestamp == datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    assert query.to_timestamp == datetime(2026, 3, 5, 12, 0, tzinfo=UTC)
    assert query.resolution == TimeSeriesResolution.PT5M
    assert query.aggregate is False


def test_defaults_to_bound_to_current_time() -> None:
    before = datetime.now(UTC)
    query = get_time_series_query_params(from_timestamp=None, to_timestamp=None, resolution=None)
    after = datetime.now(UTC)

    assert query.to_timestamp.tzinfo is not None
    assert before <= query.to_timestamp <= after
    assert query.window == timedelta(hours=24)


def test_passes_aggregate_flag_through() -> None:
    query = get_time_series_query_params(
        from_timestamp=None,
        to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
        resolution=None,
        aggregate=True,
    )
    assert query.aggregate is True


def test_maps_inverted_range_to_http_422() -> None:
    with pytest.raises(HTTPException) as exc_info:
        get_time_series_query_params(
            from_timestamp=datetime(2026, 3, 6, 0, 0, tzinfo=UTC),
            to_timestamp=datetime(2026, 3, 5, 0, 0, tzinfo=UTC),
            resolution=None,
        )
    assert exc_info.value.status_code == 422
    assert "from_timestamp" in exc_info.value.detail


def test_maps_resolution_too_fine_to_http_422() -> None:
    with pytest.raises(HTTPException) as exc_info:
        get_time_series_query_params(
            from_timestamp=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            to_timestamp=datetime(2026, 2, 15, 0, 0, tzinfo=UTC),
            resolution=TimeSeriesResolution.PT1M,
        )
    assert exc_info.value.status_code == 422
    assert "minimum allowed resolution" in exc_info.value.detail


def test_maps_window_too_large_to_http_422() -> None:
    with pytest.raises(HTTPException) as exc_info:
        get_time_series_query_params(
            from_timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
            to_timestamp=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            resolution=None,
        )
    assert exc_info.value.status_code == 422
    assert "exceeds the maximum" in exc_info.value.detail


def test_build_window_reflects_resolved_query() -> None:
    query = get_time_series_query_params(
        from_timestamp=datetime(2026, 3, 5, 6, 0, tzinfo=UTC),
        to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
        resolution=None,
    )
    window = build_window(query)
    assert window.from_timestamp == query.from_timestamp
    assert window.to_timestamp == query.to_timestamp
    assert window.resolution == query.resolution
    assert window.interval_ms == query.interval_ms


def test_apply_cache_control_sets_public_max_age_for_pinned_window() -> None:
    query = get_time_series_query_params(
        from_timestamp=datetime(2026, 3, 5, 6, 0, tzinfo=UTC),
        to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
        resolution=None,
    )
    response = Response()
    apply_cache_control(response, query)
    assert response.headers["Cache-Control"] == "public, max-age=300"


def test_apply_cache_control_sets_no_store_when_to_defaulted() -> None:
    query = get_time_series_query_params(
        from_timestamp=datetime(2026, 3, 5, 6, 0, tzinfo=UTC),
        to_timestamp=None,
        resolution=None,
    )
    response = Response()
    apply_cache_control(response, query)
    assert response.headers["Cache-Control"] == "no-store"


def test_apply_cache_control_sets_no_store_when_both_defaulted() -> None:
    query = get_time_series_query_params(from_timestamp=None, to_timestamp=None, resolution=None)
    response = Response()
    apply_cache_control(response, query)
    assert response.headers["Cache-Control"] == "no-store"
