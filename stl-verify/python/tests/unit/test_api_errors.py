"""The shared 422 contract as a client sees it, over routes built from the shared parts.

A probe app rather than a dataset route: the dataset routes land in later tickets,
and what is under test here is the machinery every one of them will inherit — the
handlers, the typed body, the window echo, and the cache policy.
"""

from datetime import UTC, datetime, timedelta

import pytest
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Response
from fastapi.testclient import TestClient

from app.api.errors import API_ERROR_RESPONSES, ApiErrorResponse, register_error_handlers
from app.api.time_series import (
    apply_cache_control,
    build_raw_window,
    get_latest_query_params,
    get_time_series_query_params,
)
from app.domain.time_series import MAX_POINTS, MAX_WINDOW, TimeSeriesQuery, TimeWindow, enforce_max_points

_KNOWN_SERIES = "known"

# Observations with a gap in the middle: nothing in the shared path may
# regularize the spacing or synthesize a point for the missing hour.
_OBSERVATIONS = [
    datetime(2026, 3, 5, 9, 0, tzinfo=UTC),
    datetime(2026, 3, 5, 10, 0, tzinfo=UTC),
    datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
]


def _probe_app() -> FastAPI:
    app = FastAPI(responses=API_ERROR_RESPONSES)
    register_error_handlers(app)
    router = APIRouter()

    @router.get("/probe/{identifier}")
    def history(
        identifier: str,
        response: Response,
        query: TimeSeriesQuery = Depends(get_time_series_query_params),
        point_count: int = Query(default=0),
    ) -> dict:
        _require_known(identifier)
        apply_cache_control(response, query)
        enforce_max_points(point_count or len(_in_window(query)), query=query)
        return {"window": build_raw_window(query).model_dump(mode="json"), "data": _in_window(query)}

    @router.get("/probe/{identifier}/latest")
    def latest(identifier: str, response: Response, window: TimeWindow = Depends(get_latest_query_params)) -> dict:
        _require_known(identifier)
        apply_cache_control(response, window)
        observed = _in_window(window)
        return {"window": build_raw_window(window).model_dump(mode="json"), "data": observed[-1:]}

    app.include_router(router)
    return app


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


def _history(client: TestClient, **params):
    return client.get(f"/probe/{_KNOWN_SERIES}", params=params)


# --- default-frequency history --------------------------------------------


def test_history_returns_the_observations_with_their_gap_intact(client: TestClient) -> None:
    body = _history(client, from_timestamp="2026-03-05T08:00:00Z", to_timestamp="2026-03-05T13:00:00Z").json()

    assert body["data"] == ["2026-03-05T09:00:00Z", "2026-03-05T10:00:00Z", "2026-03-05T12:00:00Z"]


def test_history_includes_an_observation_exactly_on_the_bound(client: TestClient) -> None:
    body = _history(client, from_timestamp="2026-03-05T10:00:00Z", to_timestamp="2026-03-05T12:00:00Z").json()

    assert body["data"] == ["2026-03-05T10:00:00Z", "2026-03-05T12:00:00Z"]


def test_history_echoes_the_window_that_answered(client: TestClient) -> None:
    body = _history(client, from_timestamp="2026-03-05T08:00:00Z", to_timestamp="2026-03-05T13:00:00Z").json()

    assert body["window"] == {"from_timestamp": "2026-03-05T08:00:00Z", "to_timestamp": "2026-03-05T13:00:00Z"}


def test_a_known_series_with_nothing_in_range_is_an_empty_two_hundred(client: TestClient) -> None:
    response = _history(client, from_timestamp="2020-01-01T00:00:00Z", to_timestamp="2020-01-02T00:00:00Z")

    assert response.status_code == 200
    assert response.json()["data"] == []
    assert response.json()["window"]["to_timestamp"] == "2020-01-02T00:00:00Z"


def test_an_unknown_series_is_a_404(client: TestClient) -> None:
    assert client.get("/probe/mistyped").status_code == 404


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
    assert body["error_code"] == "max_points_exceeded"
    assert body["point_count"] == MAX_POINTS * 4
    assert body["max_points"] == MAX_POINTS
    assert body["suggested_to_timestamp"] == "2026-03-06T00:00:00Z"
    assert body["suggested_from_timestamp"] == "2026-03-05T18:00:00Z"
    assert body["suggested_frequency"] == "PT5M"
    assert body["message"]


def test_an_oversized_request_is_rejected_rather_than_truncated(client: TestClient) -> None:
    response = _history(client, point_count=MAX_POINTS + 1)

    assert response.status_code == 422
    assert "data" not in response.json()


# --- one typed body for every rejection -----------------------------------


def test_a_domain_rejection_carries_no_suggestion_fields(client: TestClient) -> None:
    response = _history(client, from_timestamp="2020-01-01T00:00:00Z", to_timestamp="2026-01-01T00:00:00Z")

    assert response.status_code == 422
    body = response.json()
    assert body["error_code"] == "window_too_large"
    assert set(body) == {"error_code", "message"}


def test_a_validation_failure_uses_the_same_model(client: TestClient) -> None:
    response = _history(client, to_timestamp="not-a-timestamp")

    assert response.status_code == 422
    body = response.json()
    assert body["error_code"] == "invalid_request"
    assert "to_timestamp" in body["message"]
    assert ApiErrorResponse.model_validate(body)


def test_a_rejected_enum_value_uses_the_same_model(client: TestClient) -> None:
    response = _history(client, aggregation_method="period-mean")

    assert response.status_code == 422
    assert response.json()["error_code"] == "invalid_request"


def test_a_validation_failure_does_not_echo_what_was_sent(client: TestClient) -> None:
    response = _history(client, to_timestamp="totally-bogus-value")

    assert "totally-bogus-value" not in response.text


def test_the_schema_declares_the_shared_body_on_every_route(client: TestClient) -> None:
    schema = client.app.openapi()

    for path, operations in schema["paths"].items():
        for method, operation in operations.items():
            declared = operation["responses"]["422"]["content"]["application/json"]["schema"]["$ref"]
            assert declared.endswith("/ApiErrorResponse"), f"{method} {path}"


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


# --- cache policy ---------------------------------------------------------


@pytest.mark.parametrize(
    "path,params,expected",
    [
        (
            "/probe/known",
            {"from_timestamp": "2026-03-05T08:00:00Z", "to_timestamp": "2026-03-05T13:00:00Z"},
            "public, max-age=300",
        ),
        ("/probe/known", {"from_timestamp": "2026-03-05T08:00:00Z"}, "no-store"),
        ("/probe/known", {}, "no-store"),
        ("/probe/known/latest", {"to_timestamp": "2026-03-05T13:00:00Z"}, "public, max-age=300"),
        ("/probe/known/latest", {}, "no-store"),
    ],
    ids=["history-pinned", "history-open-upper-bound", "history-defaulted", "latest-pinned", "latest-defaulted"],
)
def test_cache_control_follows_whether_the_window_is_pinned(
    client: TestClient, path: str, params: dict, expected: str
) -> None:
    assert client.get(path, params=params).headers["Cache-Control"] == expected


def test_a_bound_near_the_start_of_time_is_answered_rather_than_crashing(client: TestClient) -> None:
    start_of_time = {"to_timestamp": "0001-01-01T00:00:00Z"}

    assert client.get(f"/probe/{_KNOWN_SERIES}/latest", params=start_of_time).status_code == 200
    assert _history(client, **start_of_time).status_code == 200


def test_a_rejection_that_cannot_suggest_a_window_still_suggests_a_frequency(client: TestClient) -> None:
    body = _history(
        client,
        from_timestamp="2026-03-05T11:50:00Z",
        to_timestamp="2026-03-05T12:00:00Z",
        point_count=MAX_POINTS * 1000,
    ).json()

    assert "suggested_from_timestamp" not in body
    assert body["suggested_frequency"] == "PT1M"
