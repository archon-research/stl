"""The shared 422 contract as a client sees it, over the real app.

The conformance cases run against `app.main.app`. The typed body is asserted on every
published operation, so an operation that loses it fails here. The window echo, the
cache policy, and a known-but-empty answer told apart from an unknown resource are
asserted once each -- on `/v1/protocol-events` and `/v1/primes/{prime_id}/debt` -- as
the shared statement of what a route owes; each router's own tests hold it to that.

A probe app built from the same shared parts carries what no route reaches without
inventing a repository: the max-points ceiling, which needs a point count no mocked
service produces; `/latest`, which no route serves yet; and the start-of-time
overflow guard behind its lookback.
"""

from datetime import UTC, datetime, timedelta
from http import HTTPMethod
from unittest.mock import AsyncMock

import pytest
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query
from fastapi.openapi.utils import get_openapi
from fastapi.testclient import TestClient

from app.api.errors import API_ERROR_RESPONSES, ApiErrorResponse, RejectionType, register_error_handlers
from app.api.time_series import (
    build_raw_window,
    get_latest_query_params,
    get_time_series_query_params,
)
from app.api.v1 import prime_debts, protocol_events
from app.domain.time_series import (
    MAX_POINTS,
    MAX_WINDOW,
    TimeSeriesQuery,
    TimeSeriesQueryError,
    TimeWindow,
    enforce_max_points,
)
from app.main import app
from app.services.prime_debt_service import PrimeDebtService
from app.services.protocol_event_service import ProtocolEventService

_KNOWN_PRIME = "0x" + "ab" * 20
_UNKNOWN_PRIME = "0x" + "cd" * 20

# A path item may also carry `parameters`, `summary` or `servers`; only operations
# declare responses.
_HTTP_METHODS = {method.value for method in HTTPMethod}

# A window an hour wide ending at `now`: pinned below, and narrow enough that the
# unfiltered-window ceiling never answers first.
_AN_HOUR_AGO = (datetime.now(UTC) - timedelta(hours=1)).isoformat().replace("+00:00", "Z")


def _override_with(service: AsyncMock):
    async def _dep():
        yield service

    return _dep


@pytest.fixture
def events_client() -> TestClient:
    """`/v1/protocol-events`: a real default-frequency route, reading an empty repository."""
    service = AsyncMock(spec=ProtocolEventService)
    service.list_events.return_value = []
    service.list_event_buckets.return_value = []
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_with(service)
    return TestClient(app)


@pytest.fixture
def debt_client() -> TestClient:
    """`/v1/primes/{prime_id}/debt`: a real route over an identified resource, with no rows for it."""
    service = AsyncMock(spec=PrimeDebtService)
    service.resolve_prime_id.side_effect = lambda address: 7 if address == _KNOWN_PRIME else None
    service.list_debt_snapshots.return_value = []
    service.list_debt_buckets.return_value = []
    app.dependency_overrides[prime_debts._get_prime_debt_service] = _override_with(service)
    return TestClient(app)


def _events(client: TestClient, **params):
    return client.get("/v1/protocol-events", params=params)


def _source_schema() -> dict:
    # get_openapi over the routes rather than app.openapi(), which strips the
    # operations tagged `internal`: they are published to the UI's typed client
    # and inherit the same contract.
    return get_openapi(title=app.title, version=app.version, routes=app.routes)


# --- one typed body on every operation ------------------------------------


def test_the_schema_publishes_the_rejection_types_as_a_closed_set() -> None:
    schema = _source_schema()

    assert set(schema["components"]["schemas"]["RejectionType"]["enum"]) == set(RejectionType)


def test_every_domain_rejection_is_published_as_a_rejection_type() -> None:
    assert {error.error_type for error in TimeSeriesQueryError.__subclasses__()} <= set(RejectionType)


def test_the_schema_declares_the_shared_body_on_every_route() -> None:
    schema = _source_schema()

    for path, operations in schema["paths"].items():
        for method, operation in operations.items():
            if method.upper() not in _HTTP_METHODS:
                continue
            content = operation.get("responses", {}).get("422", {}).get("content", {})
            declared = content.get("application/json", {}).get("schema", {}).get("$ref", "")
            assert declared.endswith("/ApiErrorResponse"), f"{method} {path}"


# --- the window that answered ---------------------------------------------


def test_a_response_echoes_the_window_that_answered(events_client: TestClient) -> None:
    body = _events(events_client, from_timestamp="2026-03-05T08:00:00Z", to_timestamp="2026-03-05T13:00:00Z").json()

    assert body["window"] == {"from_timestamp": "2026-03-05T08:00:00Z", "to_timestamp": "2026-03-05T13:00:00Z"}


def test_a_known_prime_with_nothing_in_range_is_an_empty_two_hundred(debt_client: TestClient) -> None:
    response = debt_client.get(
        f"/v1/primes/{_KNOWN_PRIME}/debt",
        params={"from_timestamp": "2020-01-01T00:00:00Z", "to_timestamp": "2020-01-02T00:00:00Z"},
    )

    assert response.status_code == 200
    assert response.json()["data"] == []
    assert response.json()["window"]["to_timestamp"] == "2020-01-02T00:00:00Z"


def test_an_unknown_prime_is_a_404(debt_client: TestClient) -> None:
    assert debt_client.get(f"/v1/primes/{_UNKNOWN_PRIME}/debt").status_code == 404


# --- one typed body for every rejection -----------------------------------


def test_a_domain_rejection_carries_no_suggestion_fields(events_client: TestClient) -> None:
    response = _events(events_client, from_timestamp="2020-01-01T00:00:00Z", to_timestamp="2026-01-01T00:00:00Z")

    assert response.status_code == 422
    body = response.json()
    assert body["type"] == "window_too_large"
    assert set(body) == {"type", "title", "status", "detail"}


def test_a_validation_failure_uses_the_same_model(events_client: TestClient) -> None:
    response = _events(events_client, to_timestamp="not-a-timestamp")

    assert response.status_code == 422
    body = response.json()
    assert body["type"] == "invalid_request"
    assert "to_timestamp" in body["detail"]
    assert ApiErrorResponse.model_validate(body)


def test_a_rejected_enum_value_uses_the_same_model(events_client: TestClient) -> None:
    response = _events(events_client, aggregation_method="period-mean")

    assert response.status_code == 422
    assert response.json()["type"] == "invalid_request"


def test_a_validation_failure_does_not_echo_what_was_sent(events_client: TestClient) -> None:
    response = _events(events_client, to_timestamp="totally-bogus-value")

    assert "totally-bogus-value" not in response.text


def test_a_validator_that_splices_the_value_into_its_own_message_still_does_not_echo_it(
    events_client: TestClient,
) -> None:
    response = _events(events_client, tx_hash="0xnot-a-hash")

    assert response.status_code == 422
    assert "0xnot-a-hash" not in response.text


def test_a_validation_failure_names_each_parameter_and_why_without_prose_parsing(events_client: TestClient) -> None:
    response = _events(events_client, to_timestamp="not-a-timestamp", aggregation_method="period-mean")

    errors = response.json()["errors"]
    assert {error["field"] for error in errors} == {"query.to_timestamp", "query.aggregation_method"}
    assert all(error["code"] for error in errors)


def test_a_domain_rejection_carries_no_per_field_errors(events_client: TestClient) -> None:
    response = _events(events_client, from_timestamp="2020-01-01T00:00:00Z", to_timestamp="2026-01-01T00:00:00Z")

    assert "errors" not in response.json()


# --- cache policy ---------------------------------------------------------


@pytest.mark.parametrize(
    "params,expected",
    [
        ({"from_timestamp": "2026-03-05T08:00:00Z", "to_timestamp": "2026-03-05T13:00:00Z"}, "private, max-age=300"),
        ({"from_timestamp": _AN_HOUR_AGO}, "no-store"),
        ({}, "no-store"),
    ],
    ids=["pinned", "open-upper-bound", "defaulted"],
)
def test_cache_control_follows_whether_the_window_is_pinned(
    events_client: TestClient, params: dict, expected: str
) -> None:
    assert _events(events_client, **params).headers["Cache-Control"] == expected


# --- the probe: the shared parts no route reaches yet ----------------------

_KNOWN_SERIES = "known"

# Observations with a gap in the middle: nothing in the shared path may
# regularize the spacing or synthesize a point for the missing hour.
_OBSERVATIONS = [
    datetime(2026, 3, 5, 9, 0, tzinfo=UTC),
    datetime(2026, 3, 5, 10, 0, tzinfo=UTC),
    datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
]


def _probe_app() -> FastAPI:
    probe = FastAPI(responses=API_ERROR_RESPONSES)
    register_error_handlers(probe)
    router = APIRouter()

    @router.get("/probe/{identifier}")
    def history(
        identifier: str,
        query: TimeSeriesQuery = Depends(get_time_series_query_params),
        point_count: int = Query(default=0),
    ) -> dict:
        _require_known(identifier)
        if not query.is_bucketed:
            enforce_max_points(point_count or len(_in_window(query)), query=query)
        return {"window": build_raw_window(query).model_dump(mode="json"), "data": _in_window(query)}

    @router.get("/probe/{identifier}/latest")
    def latest(identifier: str, window: TimeWindow = Depends(get_latest_query_params)) -> dict:
        _require_known(identifier)
        observed = _in_window(window)
        return {"window": build_raw_window(window).model_dump(mode="json"), "data": observed[-1:]}

    probe.include_router(router)
    return probe


def _require_known(identifier: str) -> None:
    if identifier != _KNOWN_SERIES:
        raise HTTPException(status_code=404, detail="series not found")


def _in_window(query: TimeWindow) -> list[str]:
    return [
        observed_at.isoformat().replace("+00:00", "Z")
        for observed_at in _OBSERVATIONS
        if query.from_timestamp <= observed_at <= query.to_timestamp
    ]


@pytest.fixture
def client() -> TestClient:
    return TestClient(_probe_app())


# A 24h window whose density is four times the ceiling: the rejection it draws
# suggests a quarter of the window, or the floor frequency.
_OVERSIZED_FROM = "2026-03-05T00:00:00Z"
_OVERSIZED_TO = "2026-03-06T00:00:00Z"


def _history(client: TestClient, **params):
    return client.get(f"/probe/{_KNOWN_SERIES}", params=params)


# --- default-frequency history --------------------------------------------


def test_history_returns_the_observations_with_their_gap_intact(client: TestClient) -> None:
    body = _history(client, from_timestamp="2026-03-05T08:00:00Z", to_timestamp="2026-03-05T13:00:00Z").json()

    assert body["data"] == ["2026-03-05T09:00:00Z", "2026-03-05T10:00:00Z", "2026-03-05T12:00:00Z"]


def test_history_includes_an_observation_exactly_on_the_bound(client: TestClient) -> None:
    body = _history(client, from_timestamp="2026-03-05T10:00:00Z", to_timestamp="2026-03-05T12:00:00Z").json()

    assert body["data"] == ["2026-03-05T10:00:00Z", "2026-03-05T12:00:00Z"]


# --- max-points rejection -------------------------------------------------


def test_an_oversized_request_is_rejected_with_the_typed_body(client: TestClient) -> None:
    response = _history(
        client,
        from_timestamp="2026-03-05T00:00:00Z",
        to_timestamp="2026-03-06T00:00:00Z",
        point_count=MAX_POINTS * 4,
    )

    assert response.status_code == 422
    body = response.json()
    assert body["type"] == "max_points_exceeded"
    assert body["title"] == "Too many points"
    assert body["status"] == 422
    assert body["point_count"] == MAX_POINTS * 4
    assert body["max_points"] == MAX_POINTS
    assert body["suggestions"]["narrower_window"] == {
        "from_timestamp": "2026-03-05T18:00:00Z",
        "to_timestamp": "2026-03-06T00:00:00Z",
    }
    assert body["suggestions"]["resampled"] == {"frequency": "PT5M", "aggregation_method": "end-period"}
    assert body["detail"]


def test_the_resampled_suggestion_merges_into_the_request_it_was_sent_for(client: TestClient) -> None:
    # What grouping the suggestions buys: the keys match the query parameters, so a
    # retry is a merge, carrying the density that was rejected. `aggregation_method`
    # rides along because a frequency without one is itself a rejection, and it is
    # what takes the retry off the arm the point ceiling governs.
    sent = {"from_timestamp": _OVERSIZED_FROM, "to_timestamp": _OVERSIZED_TO, "point_count": MAX_POINTS * 4}
    rejection = _history(client, **sent).json()

    retried = _history(client, **(sent | rejection["suggestions"]["resampled"]))

    assert retried.status_code == 200


def test_the_narrower_window_suggestion_merges_into_the_request_it_was_sent_for(client: TestClient) -> None:
    # The suggested span is `max_points / point_count` of the one sent — here a
    # quarter of 24h — so an evenly spaced series holds exactly the ceiling in it.
    sent = {"from_timestamp": _OVERSIZED_FROM, "to_timestamp": _OVERSIZED_TO, "point_count": MAX_POINTS * 4}
    rejection = _history(client, **sent).json()

    retried = _history(client, **(sent | rejection["suggestions"]["narrower_window"] | {"point_count": MAX_POINTS}))

    assert retried.status_code == 200
    assert retried.json()["window"]["from_timestamp"] == "2026-03-05T18:00:00Z"


def test_an_oversized_request_is_rejected_rather_than_truncated(client: TestClient) -> None:
    response = _history(client, point_count=MAX_POINTS + 1)

    assert response.status_code == 422
    assert "data" not in response.json()


def test_a_rejection_that_cannot_suggest_a_window_still_suggests_a_frequency(client: TestClient) -> None:
    body = _history(
        client,
        from_timestamp="2026-03-05T11:50:00Z",
        to_timestamp="2026-03-05T12:00:00Z",
        point_count=MAX_POINTS * 1000,
    ).json()

    assert "narrower_window" not in body["suggestions"]
    assert body["suggestions"]["resampled"]["frequency"] == "PT1M"


# --- bounded /latest ------------------------------------------------------


def test_latest_returns_the_newest_observation_at_or_before_the_bound(client: TestClient) -> None:
    body = client.get(f"/probe/{_KNOWN_SERIES}/latest", params={"to_timestamp": "2026-03-05T11:00:00Z"}).json()

    assert body["data"] == ["2026-03-05T10:00:00Z"]


def test_latest_echoes_the_lookback_it_applied(client: TestClient) -> None:
    body = client.get(f"/probe/{_KNOWN_SERIES}/latest", params={"to_timestamp": "2026-03-05T11:00:00Z"}).json()

    to_timestamp = datetime.fromisoformat(body["window"]["to_timestamp"])
    from_timestamp = datetime.fromisoformat(body["window"]["from_timestamp"])
    assert to_timestamp - from_timestamp == MAX_WINDOW


def test_latest_does_not_reach_past_the_bounded_lookback(client: TestClient) -> None:
    stale = (_OBSERVATIONS[-1] + MAX_WINDOW + timedelta(days=1)).isoformat().replace("+00:00", "Z")

    response = client.get(f"/probe/{_KNOWN_SERIES}/latest", params={"to_timestamp": stale})

    assert response.status_code == 200
    assert response.json()["data"] == []


def test_latest_distinguishes_an_unknown_series_from_an_empty_one(client: TestClient) -> None:
    assert client.get("/probe/mistyped/latest").status_code == 404


def test_a_bound_near_the_start_of_time_is_rejected_rather_than_crashing(client: TestClient) -> None:
    start_of_time = {"to_timestamp": "0001-01-01T00:00:00Z"}

    latest = client.get(f"/probe/{_KNOWN_SERIES}/latest", params=start_of_time)
    history = _history(client, **start_of_time)

    assert latest.status_code == 422
    assert latest.json()["type"] == "timestamp_out_of_range"
    assert history.status_code == 422
    assert history.json()["type"] == "timestamp_out_of_range"
