from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from fastapi import Response

from app.api.time_series import (
    BucketPoint,
    ResampledTimeSeriesWindow,
    apply_cache_control,
    build_raw_window,
    build_resampled_window,
    get_latest_query_params,
    get_resampled_time_series_query_params,
    get_time_series_query_params,
)
from app.domain.time_series import (
    MAX_WINDOW,
    AggregationMethod,
    FrequencyTooFineError,
    FrequencyWithoutAggregationMethodError,
    InvalidTimeRangeError,
    TimeSeriesFrequency,
    WindowTooLargeError,
)


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


def test_raises_the_inverted_range_error() -> None:
    with pytest.raises(InvalidTimeRangeError):
        get_time_series_query_params(
            from_timestamp=datetime(2026, 3, 6, 0, 0, tzinfo=UTC),
            to_timestamp=datetime(2026, 3, 5, 0, 0, tzinfo=UTC),
            frequency=None,
            aggregation_method=None,
        )


def test_raises_the_frequency_too_fine_error() -> None:
    with pytest.raises(FrequencyTooFineError):
        get_time_series_query_params(
            from_timestamp=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            to_timestamp=datetime(2026, 2, 15, 0, 0, tzinfo=UTC),
            frequency=TimeSeriesFrequency.PT1M,
            aggregation_method=AggregationMethod.END_PERIOD,
        )


def test_raises_the_frequency_without_a_method_error() -> None:
    with pytest.raises(FrequencyWithoutAggregationMethodError):
        get_time_series_query_params(
            from_timestamp=None,
            to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
            frequency=TimeSeriesFrequency.PT1H,
            aggregation_method=None,
        )


def test_raises_the_window_too_large_error() -> None:
    with pytest.raises(WindowTooLargeError):
        get_time_series_query_params(
            from_timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
            to_timestamp=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            frequency=None,
            aggregation_method=None,
        )


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


class _Point(BucketPoint):
    """One bucket of a series, for the schema tests."""

    bucket_start: datetime
    value: Decimal | None = None


_BUCKET_START = datetime(2026, 3, 5, 8, 0, tzinfo=UTC)


def test_a_filled_point_carries_the_marker() -> None:
    point = _Point(bucket_start=_BUCKET_START, value=Decimal("1"), filled=True)

    assert point.model_dump()["filled"] is True


def test_an_observed_point_leaves_the_marker_off_the_wire() -> None:
    point = _Point(bucket_start=_BUCKET_START, value=Decimal("1"))

    assert "filled" not in point.model_dump(mode="json")


def test_a_point_before_the_first_observation_leaves_the_marker_off_the_wire() -> None:
    point = _Point(bucket_start=_BUCKET_START)

    assert "filled" not in point.model_dump(mode="json")


def test_the_published_schema_keeps_the_fields_under_the_marker() -> None:
    schema = _Point.model_json_schema(mode="serialization")

    assert set(schema["properties"]) == {"bucket_start", "value", "filled"}


def test_the_published_schema_keeps_the_model_name_and_docstring() -> None:
    schema = _Point.model_json_schema(mode="serialization")

    assert schema["title"] == "_Point"
    assert schema["description"] == "One bucket of a series, for the schema tests."


def test_latest_dependency_bounds_the_lookback_by_the_max_window() -> None:
    window = get_latest_query_params(to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC))

    assert window.to_timestamp == datetime(2026, 3, 5, 12, 0, tzinfo=UTC)
    assert window.from_timestamp == datetime(2026, 3, 5, 12, 0, tzinfo=UTC) - MAX_WINDOW


def test_latest_dependency_defaults_its_bound_to_now() -> None:
    before = datetime.now(UTC)
    window = get_latest_query_params(to_timestamp=None)
    after = datetime.now(UTC)

    assert before <= window.to_timestamp <= after


def test_apply_cache_control_caches_a_latest_request_with_an_explicit_bound() -> None:
    response = Response()
    apply_cache_control(response, get_latest_query_params(to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC)))
    assert response.headers["Cache-Control"] == "public, max-age=300"


def test_apply_cache_control_does_not_cache_a_latest_request_defaulted_to_now() -> None:
    response = Response()
    apply_cache_control(response, get_latest_query_params(to_timestamp=None))
    assert response.headers["Cache-Control"] == "no-store"


def test_build_raw_window_echoes_the_resolved_latest_window() -> None:
    resolved = get_latest_query_params(to_timestamp=None)
    echo = build_raw_window(resolved)
    assert echo.to_timestamp == resolved.to_timestamp
    assert echo.from_timestamp == resolved.from_timestamp
