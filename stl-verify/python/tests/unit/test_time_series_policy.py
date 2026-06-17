from datetime import UTC, datetime, timedelta, timezone

import pytest

from app.domain.time_series import (
    DEFAULT_WINDOW,
    MAX_WINDOW,
    UNFILTERED_MAX_WINDOW,
    TimeSeriesQuery,
    TimeSeriesResolution,
    enforce_filter_for_window,
    minimum_resolution,
    resolve_time_series_query,
)

_NOW = datetime(2026, 3, 5, 12, 0, tzinfo=UTC)


def _resolve(
    *,
    from_timestamp: datetime | None = None,
    to_timestamp: datetime | None = None,
    resolution: TimeSeriesResolution | None = None,
    aggregate: bool = False,
    now: datetime = _NOW,
) -> TimeSeriesQuery:
    return resolve_time_series_query(
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        resolution=resolution,
        aggregate=aggregate,
        now=now,
    )


# --- resolution enum -------------------------------------------------------


def test_every_resolution_has_duration_and_interval_ms() -> None:
    for resolution in TimeSeriesResolution:
        assert resolution.duration.total_seconds() > 0
        assert resolution.interval_ms == int(resolution.duration.total_seconds() * 1000)


# --- minimum_resolution: every window bucket -------------------------------


@pytest.mark.parametrize(
    ("window", "expected"),
    [
        (timedelta(hours=3), TimeSeriesResolution.PT1M),  # <= 6h
        (timedelta(hours=6), TimeSeriesResolution.PT1M),  # boundary
        (timedelta(hours=12), TimeSeriesResolution.PT5M),  # <= 24h
        (timedelta(hours=24), TimeSeriesResolution.PT5M),  # boundary
        (timedelta(days=3), TimeSeriesResolution.PT15M),  # <= 7d
        (timedelta(days=7), TimeSeriesResolution.PT15M),  # boundary
        (timedelta(days=20), TimeSeriesResolution.PT1H),  # <= 30d
        (timedelta(days=30), TimeSeriesResolution.PT1H),  # boundary
        (timedelta(days=60), TimeSeriesResolution.PT6H),  # > 30d
    ],
)
def test_minimum_resolution_covers_every_bucket(window: timedelta, expected: TimeSeriesResolution) -> None:
    assert minimum_resolution(window) == expected


@pytest.mark.parametrize(
    ("window", "expected"),
    [
        (timedelta(hours=3), TimeSeriesResolution.PT1M),
        (timedelta(hours=12), TimeSeriesResolution.PT5M),
        (timedelta(days=3), TimeSeriesResolution.PT15M),
        (timedelta(days=20), TimeSeriesResolution.PT1H),
        (timedelta(days=60), TimeSeriesResolution.PT6H),
    ],
)
def test_default_resolution_matches_window_bucket(window: timedelta, expected: TimeSeriesResolution) -> None:
    query = _resolve(from_timestamp=_NOW - window, to_timestamp=_NOW, resolution=None)
    assert query.resolution == expected
    assert query.interval_ms == expected.interval_ms


# --- defaulting ------------------------------------------------------------


def test_defaults_both_bounds_to_last_24h_window() -> None:
    query = _resolve()
    assert query.to_timestamp == _NOW
    assert query.from_timestamp == _NOW - DEFAULT_WINDOW
    assert query.window == DEFAULT_WINDOW
    assert query.resolution == TimeSeriesResolution.PT5M


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
    assert query.resolution == TimeSeriesResolution.PT1M


def test_rejects_window_exceeding_max() -> None:
    with pytest.raises(ValueError, match="exceeds the maximum"):
        _resolve(from_timestamp=_NOW - (MAX_WINDOW + timedelta(days=1)), to_timestamp=_NOW)


def test_allows_window_at_max() -> None:
    query = _resolve(from_timestamp=_NOW - MAX_WINDOW, to_timestamp=_NOW)
    assert query.window == MAX_WINDOW


def test_rejects_resolution_finer_than_window_minimum() -> None:
    with pytest.raises(ValueError, match="minimum allowed resolution"):
        _resolve(
            from_timestamp=_NOW - timedelta(days=45),
            to_timestamp=_NOW,
            resolution=TimeSeriesResolution.PT1M,
        )


def test_accepts_resolution_not_finer_than_minimum() -> None:
    query = _resolve(
        from_timestamp=_NOW - timedelta(days=45),
        to_timestamp=_NOW,
        resolution=TimeSeriesResolution.P1D,
    )
    assert query.resolution == TimeSeriesResolution.P1D


def test_aggregate_flag_is_carried_through() -> None:
    assert _resolve(aggregate=True).aggregate is True
    assert _resolve().aggregate is False


# --- TimeSeriesQuery invariants (any construction path) --------------------


def test_query_rejects_naive_bounds() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        TimeSeriesQuery(
            from_timestamp=datetime(2026, 3, 5, 6, 0),
            to_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
            resolution=TimeSeriesResolution.PT5M,
        )


def test_query_rejects_inverted_bounds() -> None:
    with pytest.raises(ValueError, match="from_timestamp"):
        TimeSeriesQuery(
            from_timestamp=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
            to_timestamp=datetime(2026, 3, 5, 6, 0, tzinfo=UTC),
            resolution=TimeSeriesResolution.PT5M,
        )


def test_query_derives_interval_and_bucket_from_resolution() -> None:
    query = TimeSeriesQuery(
        from_timestamp=_NOW - timedelta(hours=1),
        to_timestamp=_NOW,
        resolution=TimeSeriesResolution.PT5M,
    )
    assert query.interval_ms == 5 * 60 * 1000
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
