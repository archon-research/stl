"""Gap policy for bucketed time-series output (ADR-0005 decision 7).

How an empty bucket is filled depends on what kind of quantity the series
carries, declared per series:

===========  ==============================================================
Series kind  Empty bucket
===========  ==============================================================
level        Carry the last observed value forward, and mark the point as
             filled — a price between trades is still the last price
flow         Zero — no events in a period means none occurred
either       ``null`` before the first observation: nothing to carry, and
             for a flow series nothing observed the period at all
===========  ==============================================================

Marking filled points is the load-bearing part. A mean computed over
forward-filled data is biased toward stale values and looks entirely
plausible, and arithmetic over a returned series is a first-class use of this
API. The marker rides only on filled points, so a response carries one extra
field per filled point rather than one per point.

Domain layer, standard library only, so any transport can share one policy.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from app.domain.time_series import TimeSeriesQuery

# ``time_bucket`` cuts on multiples of the bucket width measured from the
# Postgres epoch, so the grid below is anchored there too. A width that does not
# divide a day evenly, or a week multiple (which TimescaleDB anchors on Monday
# 2000-01-03 instead), would need the origin it actually uses rather than this
# one; ``_reject_uncovered_observations`` turns that disagreement into a loud
# error rather than a shifted series.
_BUCKET_ORIGIN = datetime(2000, 1, 1, tzinfo=UTC)


class SeriesKind(StrEnum):
    """The kind of quantity a series carries, which decides its gap policy."""

    LEVEL = "level"
    FLOW = "flow"


@dataclass(frozen=True)
class GapFilledPoint[T]:
    """One point of a bucketed series after the gap policy is applied.

    ``value`` is ``None`` where there is nothing to report: before the series'
    first observation, or where a level series' last observation was itself
    ``None`` — which is what ``locf`` carries. ``filled`` is true only where a
    value was carried into an empty bucket rather than observed in it, so it
    never sits beside a ``None``.
    """

    bucket_start: datetime
    value: T | None
    filled: bool = False


def _floor_to_bucket(moment: datetime, width: timedelta) -> datetime:
    return _BUCKET_ORIGIN + (moment - _BUCKET_ORIGIN) // width * width


def bucket_starts(query: TimeSeriesQuery) -> list[datetime]:
    """Every bucket ``time_bucket_gapfill`` generates for the query, newest first.

    A bound inside a bucket is answered by that whole bucket, but the upper
    bound is exclusive for a *generated* bucket: a window ending exactly on a
    boundary gets no empty bucket there. An observation at that instant does
    come back from the same query, and ``apply_gap_policy`` keeps it.
    """
    width = query.bucket
    oldest = _floor_to_bucket(query.from_timestamp, width)
    newest = _floor_to_bucket(query.to_timestamp, width)
    if newest == query.to_timestamp:
        newest -= width
    if newest < oldest:
        return []
    return [newest - width * step for step in range((newest - oldest) // width + 1)]


def apply_gap_policy[T](
    kind: SeriesKind,
    observed: Mapping[datetime, T],
    *,
    query: TimeSeriesQuery,
    zero: T | None = None,
) -> list[GapFilledPoint[T]]:
    """Fill the query's bucket grid from ``observed``, newest point first.

    ``observed`` is keyed by bucket start; membership is what makes a bucket
    observed, so a bucket whose value is genuinely ``None`` stays unmarked.
    ``zero`` is what an empty bucket of a flow series carries, supplied by the
    caller because a flow point is not always a bare count.

    Raises ``ValueError`` on a flow series without a ``zero``, on a level series
    with one — it would be read as the empty-bucket value and is not — and on an
    ``observed`` key the query could not have returned.
    """
    _reject_uncovered_observations(query, observed)
    if kind is SeriesKind.FLOW and zero is None:
        raise ValueError("a flow series needs the zero value its empty buckets carry")
    if kind is SeriesKind.LEVEL and zero is not None:
        raise ValueError("a level series carries its last observed value into an empty bucket, not a zero")

    carried: T | None = None
    observed_yet = False
    points: list[GapFilledPoint[T]] = []
    for bucket_start in sorted({*bucket_starts(query), *observed}):
        if bucket_start in observed:
            carried = observed[bucket_start]
            observed_yet = True
            points.append(GapFilledPoint(bucket_start, carried))
        elif kind is SeriesKind.LEVEL:
            points.append(GapFilledPoint(bucket_start, carried, filled=carried is not None))
        else:
            points.append(GapFilledPoint(bucket_start, zero if observed_yet else None))

    points.reverse()
    return points


def _reject_uncovered_observations(query: TimeSeriesQuery, observed: Mapping[datetime, object]) -> None:
    """Refuse an observation the query could not have answered with.

    An observation keyed off the grid, or outside the window, is a caller
    defect. Emitting it as a point of its own — which the union with the
    generated grid would do — answers with a series that reads as complete
    while sitting on a grid nobody asked for.
    """
    width = query.bucket
    oldest = _floor_to_bucket(query.from_timestamp, width)
    uncovered = sorted(
        bucket for bucket in observed if (bucket - _BUCKET_ORIGIN) % width or not oldest <= bucket <= query.to_timestamp
    )
    if uncovered:
        raise ValueError(f"observed buckets must be bucket starts within the requested window: {uncovered}")
