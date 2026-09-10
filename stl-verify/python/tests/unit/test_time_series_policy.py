from datetime import UTC, datetime, timedelta, timezone

import pytest

from app.domain.time_series import (
    DEFAULT_WINDOW,
    MAX_WINDOW,
    UNFILTERED_MAX_WINDOW,
    AggregationMethod,
    TimeSeriesFrequency,
    TimeSeriesQuery,
    enforce_filter_for_window,
    minimum_frequency,
    resolve_time_series_query,
)

_NOW = datetime(2026, 3, 5, 12, 0, tzinfo=UTC)


def _resolve(
    *,
    from_timestamp: datetime | None = None,
    to_timestamp: datetime | None = None,
    frequency: TimeSeriesFrequency | None = None,
    aggregation_method: AggregationMethod | None = None,
    default_aggregation_method: AggregationMethod | None = None,
    now: datetime = _NOW,
) -> TimeSeriesQuery:
    return resolve_time_series_query(
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        frequency=frequency,
        aggregation_method=aggregation_method,
        default_aggregation_method=default_aggregation_method,
        now=now,
    )


# --- frequency enum -------------------------------------------------------


def test_every_frequency_has_duration_and_duration_ms() -> None:
    for frequency in TimeSeriesFrequency:
        assert frequency.duration.total_seconds() > 0
        assert frequency.duration_ms == int(frequency.duration.total_seconds() * 1000)


# --- minimum_frequency: every window bucket -------------------------------


@pytest.mark.parametrize(
    ("window", "expected"),
    [
        (timedelta(hours=3), TimeSeriesFrequency.PT1M),  # <= 6h
        (timedelta(hours=6), TimeSeriesFrequency.PT1M),  # boundary
        (timedelta(hours=12), TimeSeriesFrequency.PT5M),  # <= 24h
        (timedelta(hours=24), TimeSeriesFrequency.PT5M),  # boundary
        (timedelta(days=3), TimeSeriesFrequency.PT15M),  # <= 7d
        (timedelta(days=7), TimeSeriesFrequency.PT15M),  # boundary
        (timedelta(days=20), TimeSeriesFrequency.PT1H),  # <= 30d
        (timedelta(days=30), TimeSeriesFrequency.PT1H),  # boundary
        (timedelta(days=60), TimeSeriesFrequency.PT6H),  # > 30d
    ],
)
def test_minimum_frequency_covers_every_bucket(window: timedelta, expected: TimeSeriesFrequency) -> None:
    assert minimum_frequency(window) == expected


@pytest.mark.parametrize(
    ("window", "expected"),
    [
        (timedelta(hours=3), TimeSeriesFrequency.PT1M),
        (timedelta(hours=12), TimeSeriesFrequency.PT5M),
        (timedelta(days=3), TimeSeriesFrequency.PT15M),
        (timedelta(days=20), TimeSeriesFrequency.PT1H),
        (timedelta(days=60), TimeSeriesFrequency.PT6H),
    ],
)
def test_default_frequency_matches_window_bucket(window: timedelta, expected: TimeSeriesFrequency) -> None:
    query = _resolve(from_timestamp=_NOW - window, to_timestamp=_NOW, frequency=None)
    assert query.frequency == expected
    assert query.frequency_ms == expected.duration_ms


# --- defaulting ------------------------------------------------------------


def test_defaults_both_bounds_to_last_24h_window() -> None:
    query = _resolve()
    assert query.to_timestamp == _NOW
    assert query.from_timestamp == _NOW - DEFAULT_WINDOW
    assert query.window == DEFAULT_WINDOW
    assert query.frequency == TimeSeriesFrequency.PT5M


def test_defaults_to_bound_to_now_when_only_from_given() -> None:
    query = _resolve(from_timestamp=_NOW - timedelta(hours=3))
    assert query.to_timestamp == _NOW


# --- timezone normalization ------------------------------------------------


def test_naive_bounds_are_assumed_utc() -> None:
    query = _resolve(
        from_timestamp=datetime(2026, 3, 5, 6, 0),  # naive
        to_timestamp=datetime(2026, 3, 5, 12, 0),  # naive
    )
    assert query.from_timestamp == datetime(2026, 3, 5, 6, 0, tzinfo=UTC)
    assert query.to_timestamp == datetime(2026, 3, 5, 12, 0, tzinfo=UTC)


def test_non_utc_aware_bounds_are_converted_to_utc() -> None:
    eastern = timezone(timedelta(hours=-5))
    query = _resolve(
        from_timestamp=datetime(2026, 3, 5, 1, 0, tzinfo=eastern),  # 06:00 UTC
        to_timestamp=datetime(2026, 3, 5, 7, 0, tzinfo=eastern),  # 12:00 UTC
    )
    assert query.from_timestamp == datetime(2026, 3, 5, 6, 0, tzinfo=UTC)
    assert query.to_timestamp == datetime(2026, 3, 5, 12, 0, tzinfo=UTC)


# --- validation ------------------------------------------------------------


def test_rejects_inverted_range() -> None:
    with pytest.raises(ValueError, match="from_timestamp"):
        _resolve(from_timestamp=_NOW, to_timestamp=_NOW - timedelta(hours=1))


def test_allows_zero_width_window() -> None:
    query = _resolve(from_timestamp=_NOW, to_timestamp=_NOW)
    assert query.window == timedelta(0)
    assert query.frequency == TimeSeriesFrequency.PT1M


def test_rejects_window_exceeding_max() -> None:
    with pytest.raises(ValueError, match="exceeds the maximum"):
        _resolve(from_timestamp=_NOW - (MAX_WINDOW + timedelta(days=1)), to_timestamp=_NOW)


def test_allows_window_at_max() -> None:
    query = _resolve(from_timestamp=_NOW - MAX_WINDOW, to_timestamp=_NOW)
    assert query.window == MAX_WINDOW


def test_rejects_frequency_finer_than_window_minimum() -> None:
    with pytest.raises(ValueError, match="minimum allowed frequency"):
        _resolve(
            from_timestamp=_NOW - timedelta(days=45),
            to_timestamp=_NOW,
            frequency=TimeSeriesFrequency.PT1M,
            aggregation_method=AggregationMethod.END_PERIOD,
        )


def test_accepts_frequency_not_finer_than_minimum() -> None:
    query = _resolve(
        from_timestamp=_NOW - timedelta(days=45),
        to_timestamp=_NOW,
        frequency=TimeSeriesFrequency.P1D,
        aggregation_method=AggregationMethod.END_PERIOD,
    )
    assert query.frequency == TimeSeriesFrequency.P1D


def test_rejects_a_frequency_with_no_method_to_cut_on_it() -> None:
    with pytest.raises(ValueError, match="aggregation_method"):
        _resolve(frequency=TimeSeriesFrequency.PT1H)


@pytest.mark.parametrize(
    ("method", "expected"),
    [(AggregationMethod.END_PERIOD, True), (None, False)],
    ids=["resampled", "default-frequency"],
)
def test_aggregation_method_is_the_bucketing_switch(method: AggregationMethod | None, expected: bool) -> None:
    assert _resolve(aggregation_method=method).is_bucketed is expected


def test_a_default_method_resamples_a_query_that_named_none() -> None:
    query = _resolve(default_aggregation_method=AggregationMethod.END_PERIOD)
    assert query.aggregation_method is AggregationMethod.END_PERIOD
    assert query.is_bucketed is True


def test_a_default_method_lets_a_frequency_through_without_one_named() -> None:
    query = _resolve(
        frequency=TimeSeriesFrequency.PT1H,
        default_aggregation_method=AggregationMethod.END_PERIOD,
    )
    assert query.frequency == TimeSeriesFrequency.PT1H


# --- TimeSeriesQuery invariants (any construction path) --------------------


def test_query_rejects_naive_bounds() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        TimeSeriesQuery(
            from_timestamp=datetime(2026, 3, 5, 6, 0),
            to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
            frequency=TimeSeriesFrequency.PT5M,
        )


def test_query_rejects_inverted_bounds() -> None:
    with pytest.raises(ValueError, match="from_timestamp"):
        TimeSeriesQuery(
            from_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
            to_timestamp=datetime(2026, 3, 5, 6, 0, tzinfo=UTC),
            frequency=TimeSeriesFrequency.PT5M,
        )


def test_query_derives_frequency_ms_and_bucket_from_frequency() -> None:
    query = TimeSeriesQuery(
        from_timestamp=_NOW - timedelta(hours=1),
        to_timestamp=_NOW,
        frequency=TimeSeriesFrequency.PT5M,
    )
    assert query.frequency_ms == 5 * 60 * 1000
    assert query.bucket == timedelta(minutes=5)


# --- bounds_pinned ---------------------------------------------------------


def test_bounds_pinned_true_when_both_bounds_supplied() -> None:
    query = _resolve(from_timestamp=_NOW - timedelta(hours=3), to_timestamp=_NOW)
    assert query.bounds_pinned is True


def test_bounds_pinned_false_when_to_defaulted_to_now() -> None:
    query = _resolve(from_timestamp=_NOW - timedelta(hours=3), to_timestamp=None)
    assert query.bounds_pinned is False


def test_bounds_pinned_false_when_from_defaulted() -> None:
    query = _resolve(from_timestamp=None, to_timestamp=_NOW)
    assert query.bounds_pinned is False


def test_bounds_pinned_false_when_both_defaulted() -> None:
    assert _resolve().bounds_pinned is False


# --- enforce_filter_for_window --------------------------------------------


def test_enforce_filter_allows_any_window_with_selective_filter() -> None:
    # Even a window larger than UNFILTERED_MAX_WINDOW is fine when filtered.
    query = _resolve(from_timestamp=_NOW - (UNFILTERED_MAX_WINDOW + timedelta(days=30)), to_timestamp=_NOW)
    enforce_filter_for_window(query, has_selective_filter=True)


def test_enforce_filter_allows_unfiltered_window_within_cap() -> None:
    query = _resolve(from_timestamp=_NOW - UNFILTERED_MAX_WINDOW, to_timestamp=_NOW)
    enforce_filter_for_window(query, has_selective_filter=False)


def test_enforce_filter_rejects_unfiltered_window_beyond_cap() -> None:
    query = _resolve(from_timestamp=_NOW - (UNFILTERED_MAX_WINDOW + timedelta(days=1)), to_timestamp=_NOW)
    with pytest.raises(ValueError, match="selective filter"):
        enforce_filter_for_window(query, has_selective_filter=False)


def test_enforce_filter_honors_custom_cap_override() -> None:
    query = _resolve(from_timestamp=_NOW - timedelta(hours=2), to_timestamp=_NOW)
    with pytest.raises(ValueError, match="selective filter"):
        enforce_filter_for_window(
            query,
            has_selective_filter=False,
            unfiltered_max_window=timedelta(hours=1),
        )
