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
API. The marker rides only on filled points, so a long response carries a
handful of extra fields rather than one per point.

A requested frequency is deliberately never validated against a series'
cadence: an hourly request on a daily series answers with 23 filled points and
one observed point per day, which is honest and visible to the caller.

Domain layer, standard library only, so any transport can share one policy.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from app.domain.time_series import TimeSeriesQuery

# ``time_bucket`` cuts on multiples of the bucket width since the Unix epoch,
# so the grid below is anchored there too and the two agree bucket for bucket.
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class SeriesKind(StrEnum):
    """The kind of quantity a series carries, which decides its gap policy."""

    LEVEL = "level"
    FLOW = "flow"


@dataclass(frozen=True)
class GapFilledPoint[T]:
    """One point of a bucketed series after the gap policy is applied.

    ``value`` is ``None`` for buckets before the series' first observation.
    ``filled`` is true only where the value was carried into an empty bucket
    rather than observed in it.
    """

    bucket_start: datetime
    value: T | None
    filled: bool = False


def _floor_to_bucket(moment: datetime, width: timedelta) -> datetime:
    return _EPOCH + (moment - _EPOCH) // width * width


def bucket_starts(query: TimeSeriesQuery) -> list[datetime]:
    """Every bucket start the query's window covers, newest first.

    Both bounds are inclusive, and a bound inside a bucket is answered by that
    whole bucket — the same grid ``time_bucket_gapfill`` generates for the same
    window and width.
    """
    width = query.bucket
    oldest = _floor_to_bucket(query.from_timestamp, width)
    newest = _floor_to_bucket(query.to_timestamp, width)
    return [newest - width * step for step in range((newest - oldest) // width + 1)]


def apply_gap_policy[T](
    kind: SeriesKind,
    observed: Mapping[datetime, T],
    *,
    query: TimeSeriesQuery,
    zero: T | None = None,
    prior: T | None = None,
) -> list[GapFilledPoint[T]]:
    """Fill the query's bucket grid from ``observed``, newest point first.

    ``observed`` is keyed by bucket start; membership is what makes a bucket
    observed, so a bucket whose value is genuinely ``None`` stays unmarked.
    ``zero`` is what an empty bucket of a flow series carries, supplied by the
    caller because a flow point is not always a bare count. ``prior`` is the
    last value observed *before* the window, where the caller can read one: it
    fills the leading buckets of a level series instead of leaving them
    ``null``.

    Raises ``ValueError`` when a flow series is applied without a ``zero``.
    """
    if kind is SeriesKind.FLOW and zero is None:
        raise ValueError("a flow series needs the zero value its empty buckets carry")

    carried = prior
    observed_yet = prior is not None
    points: list[GapFilledPoint[T]] = []
    for bucket_start in reversed(bucket_starts(query)):
        if bucket_start in observed:
            carried = observed[bucket_start]
            observed_yet = True
            points.append(GapFilledPoint(bucket_start, carried))
        elif not observed_yet:
            points.append(GapFilledPoint(bucket_start, None))
        elif kind is SeriesKind.LEVEL:
            points.append(GapFilledPoint(bucket_start, carried, filled=True))
        else:
            points.append(GapFilledPoint(bucket_start, zero))

    points.reverse()
    return points
