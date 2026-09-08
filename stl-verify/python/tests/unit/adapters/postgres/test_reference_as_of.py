"""The reference effective instant bound into reference-table reads (ADR-0006 §4)."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from app.adapters.postgres.reference_as_of import ReferenceAsOf, pinned_to, utc_now

_JUNE_1 = datetime(2026, 6, 1, 14, 30, tzinfo=UTC)
_JUNE_2 = datetime(2026, 6, 2, 9, 0, tzinfo=UTC)


def test_params_carries_the_effective_instant_alongside_the_query_params() -> None:
    as_of = ReferenceAsOf(lambda: _JUNE_1)

    assert as_of.params(token_id=7) == {"token_id": 7, "reference_effective_at": _JUNE_1}


def test_params_resolves_once_per_call() -> None:
    """Every reference read in one query sees one instant, even as the provider advances."""
    instants = iter([_JUNE_1, _JUNE_2])
    as_of = ReferenceAsOf(lambda: next(instants))

    first = as_of.params()
    second = as_of.params()

    assert first["reference_effective_at"] == _JUNE_1
    assert second["reference_effective_at"] == _JUNE_2


def test_pinned_to_an_instant_ignores_the_clock() -> None:
    """How a replay reproduces a past reference view."""
    assert pinned_to(_JUNE_1)() == _JUNE_1


def test_pinned_to_a_zoned_instant_is_normalised_to_utc() -> None:
    assert pinned_to(datetime(2026, 6, 1, 16, 30, tzinfo=timezone(timedelta(hours=2))))() == _JUNE_1


def test_pinned_to_a_naive_instant_is_rejected() -> None:
    """Bound naive, Postgres would read it in the session's TimeZone."""
    with pytest.raises(ValueError, match="carries no timezone"):
        pinned_to(datetime(2026, 6, 1))


def test_pinned_to_none_falls_back_to_now() -> None:
    assert pinned_to(None) is utc_now
    assert abs((utc_now() - datetime.now(UTC)).total_seconds()) < 1
