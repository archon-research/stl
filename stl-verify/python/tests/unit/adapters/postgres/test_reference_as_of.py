"""The reference effective date bound into reference-table reads (ADR-0006 §4)."""

from datetime import UTC, date, datetime

from app.adapters.postgres.reference_as_of import ReferenceAsOf, pinned_to, utc_today


def test_params_carries_the_effective_date_alongside_the_query_params() -> None:
    as_of = ReferenceAsOf(lambda: date(2026, 6, 1))

    assert as_of.params(token_id=7) == {"token_id": 7, "reference_effective_at": date(2026, 6, 1)}


def test_params_resolves_once_per_call() -> None:
    """Every reference read in one query sees one date, even as the provider advances."""
    dates = iter([date(2026, 6, 1), date(2026, 6, 2)])
    as_of = ReferenceAsOf(lambda: next(dates))

    first = as_of.params()
    second = as_of.params()

    assert first["reference_effective_at"] == date(2026, 6, 1)
    assert second["reference_effective_at"] == date(2026, 6, 2)


def test_pinned_to_a_date_ignores_the_clock() -> None:
    """How a replay reproduces a past reference view."""
    assert pinned_to(date(2026, 6, 1))() == date(2026, 6, 1)


def test_pinned_to_none_falls_back_to_today() -> None:
    assert pinned_to(None) is utc_today
    assert utc_today() == datetime.now(UTC).date()
