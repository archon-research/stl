from datetime import UTC, datetime, timedelta

import pytest
from fastapi import HTTPException, Response

from app.api.time_series import (
    ResampledTimeSeriesWindow,
    apply_cache_control,
    build_raw_window,
    build_resampled_window,
    get_resampled_time_series_query_params,
    get_time_series_query_params,
)
from app.domain.time_series import AggregationMethod, TimeSeriesFrequency


def test_returns_resolved_query_with_defaults() -> None:
    query = get_time_series_query_params(
        from_timestamp=None,
        to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
        frequency=None,
        aggregation_method=None,
    )

    assert query.from_timestamp == datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    assert query.to_timestamp == datetime(2026, 3, 5, 12, 0, tzinfo=UTC)
    assert query.frequency == TimeSeriesFrequency.PT5M
    assert query.is_bucketed is False


def test_defaults_to_bound_to_current_time() -> None:
    before = datetime.now(UTC)
    query = get_time_series_query_params(
        from_timestamp=None, to_timestamp=None, frequency=None, aggregation_method=None
    )
    after = datetime.now(UTC)

    assert query.to_timestamp.tzinfo is not None
    assert before <= query.to_timestamp <= after
    assert query.window == timedelta(hours=24)


def test_passes_aggregation_method_through() -> None:
    query = get_time_series_query_params(
        from_timestamp=None,
        to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
        frequency=None,
        aggregation_method=AggregationMethod.END_PERIOD,
    )
    assert query.aggregation_method is AggregationMethod.END_PERIOD
    assert query.is_bucketed is True


def test_maps_inverted_range_to_http_422() -> None:
    with pytest.raises(HTTPException) as exc_info:
        get_time_series_query_params(
            from_timestamp=datetime(2026, 3, 6, 0, 0, tzinfo=UTC),
            to_timestamp=datetime(2026, 3, 5, 0, 0, tzinfo=UTC),
            frequency=None,
            aggregation_method=None,
        )
    assert exc_info.value.status_code == 422
    assert "from_timestamp" in exc_info.value.detail


def test_maps_frequency_too_fine_to_http_422() -> None:
    with pytest.raises(HTTPException) as exc_info:
        get_time_series_query_params(
            from_timestamp=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            to_timestamp=datetime(2026, 2, 15, 0, 0, tzinfo=UTC),
            frequency=TimeSeriesFrequency.PT1M,
            aggregation_method=AggregationMethod.END_PERIOD,
        )
    assert exc_info.value.status_code == 422
    assert "minimum allowed frequency" in exc_info.value.detail


def test_maps_a_frequency_with_no_method_to_http_422() -> None:
    with pytest.raises(HTTPException) as exc_info:
        get_time_series_query_params(
            from_timestamp=None,
            to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
            frequency=TimeSeriesFrequency.PT1H,
            aggregation_method=None,
        )
    assert exc_info.value.status_code == 422
    assert "aggregation_method" in exc_info.value.detail


def test_maps_window_too_large_to_http_422() -> None:
    with pytest.raises(HTTPException) as exc_info:
        get_time_series_query_params(
            from_timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
            to_timestamp=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            frequency=None,
            aggregation_method=None,
        )
    assert exc_info.value.status_code == 422
    assert "exceeds the maximum" in exc_info.value.detail


def test_build_resampled_window_echoes_the_frequency_of_a_bucketed_query() -> None:
    query = get_time_series_query_params(
        from_timestamp=datetime(2026, 3, 5, 6, 0, tzinfo=UTC),
        to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
        frequency=None,
        aggregation_method=AggregationMethod.END_PERIOD,
    )
    window = build_resampled_window(query)
    assert window.from_timestamp == query.from_timestamp
    assert window.to_timestamp == query.to_timestamp
    assert window.frequency == query.frequency
    assert window.frequency_ms == query.frequency_ms


def test_build_raw_window_omits_the_frequency_pair() -> None:
    query = get_time_series_query_params(
        from_timestamp=datetime(2026, 3, 5, 6, 0, tzinfo=UTC),
        to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
        frequency=None,
        aggregation_method=None,
    )
    window = build_raw_window(query)
    assert not isinstance(window, ResampledTimeSeriesWindow)
    assert "frequency" not in window.model_dump()
    assert "frequency_ms" not in window.model_dump()


def test_build_resampled_window_carries_the_frequency_without_an_aggregation_method() -> None:
    query = get_time_series_query_params(
        from_timestamp=datetime(2026, 3, 5, 6, 0, tzinfo=UTC),
        to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
        frequency=None,
        aggregation_method=None,
    )
    dumped = build_resampled_window(query).model_dump()
    assert dumped["frequency"] == query.frequency
    assert dumped["frequency_ms"] == query.frequency_ms


def test_the_resampled_dependency_resamples_a_query_that_named_no_method() -> None:
    query = get_resampled_time_series_query_params(
        from_timestamp=datetime(2026, 3, 5, 6, 0, tzinfo=UTC),
        to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
        frequency=None,
        aggregation_method=None,
    )
    assert query.is_bucketed is True


def test_apply_cache_control_sets_public_max_age_for_pinned_window() -> None:
    query = get_time_series_query_params(
        from_timestamp=datetime(2026, 3, 5, 6, 0, tzinfo=UTC),
        to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
        frequency=None,
        aggregation_method=None,
    )
    response = Response()
    apply_cache_control(response, query)
    assert response.headers["Cache-Control"] == "public, max-age=300"


def test_apply_cache_control_sets_no_store_when_to_defaulted() -> None:
    query = get_time_series_query_params(
        from_timestamp=datetime(2026, 3, 5, 6, 0, tzinfo=UTC),
        to_timestamp=None,
        frequency=None,
        aggregation_method=None,
    )
    response = Response()
    apply_cache_control(response, query)
    assert response.headers["Cache-Control"] == "no-store"


def test_apply_cache_control_sets_no_store_when_both_defaulted() -> None:
    query = get_time_series_query_params(
        from_timestamp=None, to_timestamp=None, frequency=None, aggregation_method=None
    )
    response = Response()
    apply_cache_control(response, query)
    assert response.headers["Cache-Control"] == "no-store"
