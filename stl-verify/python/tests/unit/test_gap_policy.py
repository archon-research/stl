from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from app.domain.gap_policy import GapFilledPoint, SeriesKind, apply_gap_policy, bucket_starts
from app.domain.time_series import AggregationMethod, TimeSeriesFrequency, TimeSeriesQuery

_HOUR = timedelta(hours=1)
_WINDOW_START = datetime(2026, 3, 5, 8, 0, tzinfo=UTC)


def _query(
    *,
    from_timestamp: datetime = _WINDOW_START,
    to_timestamp: datetime = _WINDOW_START + 4 * _HOUR + timedelta(minutes=30),
    frequency: TimeSeriesFrequency = TimeSeriesFrequency.PT1H,
) -> TimeSeriesQuery:
    return TimeSeriesQuery(
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        frequency=frequency,
        aggregation_method=AggregationMethod.END_PERIOD,
    )


def _bucket(hours: int) -> datetime:
    return _WINDOW_START + hours * _HOUR


# --- the bucket grid ------------------------------------------------------


def test_grid_covers_the_window_newest_first() -> None:
    assert bucket_starts(_query()) == [_bucket(4), _bucket(3), _bucket(2), _bucket(1), _bucket(0)]


def test_grid_generates_no_bucket_for_an_upper_bound_on_a_boundary() -> None:
    query = _query(to_timestamp=_bucket(4))

    assert bucket_starts(query) == [_bucket(3), _bucket(2), _bucket(1), _bucket(0)]


def test_grid_is_empty_for_a_window_with_no_bucket_to_generate() -> None:
    assert bucket_starts(_query(to_timestamp=_WINDOW_START)) == []


def test_grid_answers_a_bound_inside_a_bucket_with_that_whole_bucket() -> None:
    query = _query(
        from_timestamp=_WINDOW_START + timedelta(minutes=20),
        to_timestamp=_WINDOW_START + timedelta(minutes=95),
    )

    assert bucket_starts(query) == [_bucket(1), _bucket(0)]


def test_grid_aligns_daily_buckets_to_midnight_like_time_bucket() -> None:
    query = _query(
        from_timestamp=datetime(2026, 3, 5, 8, 0, tzinfo=UTC),
        to_timestamp=datetime(2026, 3, 6, 17, 0, tzinfo=UTC),
        frequency=TimeSeriesFrequency.P1D,
    )

    assert bucket_starts(query) == [
        datetime(2026, 3, 6, tzinfo=UTC),
        datetime(2026, 3, 5, tzinfo=UTC),
    ]


# --- level series ---------------------------------------------------------


def test_level_carries_the_last_value_into_empty_buckets_and_marks_them() -> None:
    observed = {_bucket(1): Decimal("100"), _bucket(3): Decimal("120")}

    points = apply_gap_policy(SeriesKind.LEVEL, observed, query=_query())

    assert points == [
        GapFilledPoint(_bucket(4), Decimal("120"), filled=True),
        GapFilledPoint(_bucket(3), Decimal("120")),
        GapFilledPoint(_bucket(2), Decimal("100"), filled=True),
        GapFilledPoint(_bucket(1), Decimal("100")),
        GapFilledPoint(_bucket(0), None),
    ]


def test_level_keeps_a_bucket_observed_at_the_exclusive_upper_bound() -> None:
    query = _query(to_timestamp=_bucket(4))

    points = apply_gap_policy(SeriesKind.LEVEL, {_bucket(4): Decimal("7")}, query=query)

    assert points[0] == GapFilledPoint(_bucket(4), Decimal("7"))


def test_level_leaves_buckets_before_the_first_observation_null_and_unmarked() -> None:
    points = apply_gap_policy(SeriesKind.LEVEL, {_bucket(3): Decimal("5")}, query=_query())

    leading = [point for point in points if point.bucket_start < _bucket(3)]
    assert [point.value for point in leading] == [None, None, None]
    assert not any(point.filled for point in leading)


def test_level_fills_leading_buckets_from_a_value_observed_before_the_window() -> None:
    points = apply_gap_policy(SeriesKind.LEVEL, {}, query=_query(), prior=Decimal("42"))

    assert all(point.value == Decimal("42") for point in points)
    assert all(point.filled for point in points)


def test_level_treats_an_observed_null_as_observed() -> None:
    points = apply_gap_policy(SeriesKind.LEVEL, {_bucket(2): None}, query=_query())

    observed_point = next(point for point in points if point.bucket_start == _bucket(2))
    assert observed_point == GapFilledPoint(_bucket(2), None)


def test_level_carries_an_observed_null_without_marking_the_buckets_it_fills() -> None:
    points = apply_gap_policy(SeriesKind.LEVEL, {_bucket(1): Decimal("3"), _bucket(2): None}, query=_query())

    after_the_null = [point for point in points if point.bucket_start > _bucket(2)]
    assert [point.value for point in after_the_null] == [None, None]
    assert not any(point.filled for point in after_the_null)


# --- flow series ----------------------------------------------------------


def test_flow_zeroes_empty_buckets_after_the_first_observation_without_marking_them() -> None:
    observed = {_bucket(1): 7, _bucket(3): 2}

    points = apply_gap_policy(SeriesKind.FLOW, observed, query=_query(), zero=0)

    assert points == [
        GapFilledPoint(_bucket(4), 0),
        GapFilledPoint(_bucket(3), 2),
        GapFilledPoint(_bucket(2), 0),
        GapFilledPoint(_bucket(1), 7),
        GapFilledPoint(_bucket(0), None),
    ]


def test_flow_distinguishes_no_events_from_no_coverage() -> None:
    points = apply_gap_policy(SeriesKind.FLOW, {_bucket(2): 1}, query=_query(), zero=0)

    by_bucket = {point.bucket_start: point.value for point in points}
    assert by_bucket[_bucket(0)] is None
    assert by_bucket[_bucket(3)] == 0


def test_flow_zero_may_be_any_shape_of_empty_point() -> None:
    empty = (0, Decimal("0"))

    points = apply_gap_policy(SeriesKind.FLOW, {_bucket(0): (3, Decimal("9"))}, query=_query(), zero=empty)

    assert points[0] == GapFilledPoint(_bucket(4), empty)


def test_flow_without_a_zero_is_rejected() -> None:
    with pytest.raises(ValueError, match="zero"):
        apply_gap_policy(SeriesKind.FLOW, {}, query=_query())


def test_flow_with_a_prior_is_rejected() -> None:
    with pytest.raises(ValueError, match="no prior value"):
        apply_gap_policy(SeriesKind.FLOW, {}, query=_query(), zero=0, prior=99)


# --- misuse ---------------------------------------------------------------


def test_an_observation_off_the_grid_is_rejected_rather_than_emitted() -> None:
    off_grid = _bucket(1) + timedelta(minutes=17)

    with pytest.raises(ValueError, match="must be bucket starts within the requested window"):
        apply_gap_policy(SeriesKind.LEVEL, {off_grid: Decimal("1")}, query=_query())


def test_an_observation_outside_the_window_is_rejected_rather_than_dropped() -> None:
    with pytest.raises(ValueError, match="must be bucket starts within the requested window"):
        apply_gap_policy(SeriesKind.LEVEL, {_bucket(-1): Decimal("1")}, query=_query())

    with pytest.raises(ValueError, match="must be bucket starts within the requested window"):
        apply_gap_policy(SeriesKind.LEVEL, {_bucket(5): Decimal("1")}, query=_query())
