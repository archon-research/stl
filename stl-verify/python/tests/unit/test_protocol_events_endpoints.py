from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from app.domain.entities.protocol_event import ProtocolEvent
from app.domain.entities.time_series_bucket import ProtocolEventBucket
from app.main import app
from app.services.protocol_event_service import ProtocolEventService

_VALID_TX_HASH = "0x" + "ab" * 32


@pytest.fixture(autouse=True)
def _clear_dependency_overrides():
    yield
    app.dependency_overrides.clear()


def _make_service(
    *,
    events: list[ProtocolEvent] | None = None,
    tx_events: list[ProtocolEvent] | None = None,
) -> Any:
    service = AsyncMock(spec=ProtocolEventService)
    service.list_events.return_value = events or []
    service.get_events_by_tx.return_value = tx_events or []
    service.list_event_buckets.return_value = []
    return service


def _override_service(service: Any):
    async def _dep():
        yield service

    return _dep


def _event() -> ProtocolEvent:
    return ProtocolEvent(
        tx_hash=_VALID_TX_HASH,
        log_index=7,
        chain_id=1,
        block_number=22_000_123,
        block_version=0,
        protocol_name="spark",
        event_name="Borrow",
        contract_address="0x" + "cd" * 20,
        event_data={"amount": "123"},
        created_at=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
    )


def test_list_protocol_events_returns_rows_and_applies_filters():
    from app.api.v1 import protocol_events

    event = _event()
    service = _make_service(events=[event])
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_service(service)
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get(
        "/v1/protocol-events",
        params={
            "tx_hash": _VALID_TX_HASH,
            "protocol_name": "spark",
            "from_timestamp": "2026-03-05T00:00:00Z",
            "to_timestamp": "2026-03-05T12:00:00Z",
            "resolution": "PT5M",
            "limit": 25,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "raw"
    assert len(payload["data"]) == 1
    assert payload["data"][0]["tx_hash"] == _VALID_TX_HASH
    kwargs = service.list_events.await_args.kwargs
    assert kwargs["tx_hash"] == _VALID_TX_HASH
    assert kwargs["protocol_name"] == "spark"
    assert kwargs["from_timestamp"] == datetime(2026, 3, 5, 0, 0, tzinfo=UTC)
    assert kwargs["to_timestamp"] == datetime(2026, 3, 5, 12, 0, tzinfo=UTC)
    assert kwargs["limit"] == 25


def test_list_protocol_events_returns_aggregated_buckets():
    from app.api.v1 import protocol_events

    service = _make_service()
    service.list_event_buckets.return_value = [
        ProtocolEventBucket(bucket_start=datetime(2026, 3, 5, 12, 0, tzinfo=UTC), event_count=4),
    ]
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(
        "/v1/protocol-events",
        params={
            "protocol_name": "spark",
            "from_timestamp": "2026-03-05T00:00:00Z",
            "to_timestamp": "2026-03-05T12:00:00Z",
            "aggregate": "true",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "aggregated"
    assert payload["data"] == [{"bucket_start": "2026-03-05T12:00:00Z", "event_count": 4}]
    kwargs = service.list_event_buckets.await_args.kwargs
    assert kwargs["bucket_seconds"] == 5 * 60  # 12h window -> PT5M default
    service.list_events.assert_not_awaited()


def test_list_protocol_events_returns_422_for_invalid_tx_hash_query():
    from app.api.v1 import protocol_events

    service = _make_service()
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_service(service)
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/v1/protocol-events", params={"tx_hash": "foo"})

    assert response.status_code == 422
    service.list_events.assert_not_awaited()


def test_get_tx_events_returns_rows_for_valid_tx_hash():
    from app.api.v1 import protocol_events

    event = _event()
    service = _make_service(tx_events=[event])
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/tx/{_VALID_TX_HASH}/events")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["event_name"] == "Borrow"
    service.get_events_by_tx.assert_awaited_once_with(_VALID_TX_HASH)


def test_get_tx_events_returns_422_for_invalid_path_hash():
    from app.api.v1 import protocol_events

    service = _make_service()
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/tx/not-a-hash/events")

    assert response.status_code == 422
    service.get_events_by_tx.assert_not_awaited()


def test_list_protocol_events_returns_empty_list_when_no_matches():
    from app.api.v1 import protocol_events

    service = _make_service(events=[])
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/protocol-events", params={"protocol_name": "nonexistent"})

    assert response.status_code == 200
    body = response.json()
    assert body["mode"] == "raw"
    assert body["data"] == []
    service.list_events.assert_awaited_once()


def test_list_protocol_events_returns_422_for_limit_too_large():
    from app.api.v1 import protocol_events

    service = _make_service(events=[])
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/protocol-events", params={"limit": 600})

    assert response.status_code == 422
    service.list_events.assert_not_awaited()


def test_list_protocol_events_returns_422_for_too_fine_resolution_for_window():
    from app.api.v1 import protocol_events

    service = _make_service(events=[])
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(
        "/v1/protocol-events",
        params={
            "from_timestamp": "2026-01-01T00:00:00Z",
            "to_timestamp": "2026-02-15T00:00:00Z",
            "resolution": "PT1M",
        },
    )

    assert response.status_code == 422
    service.list_events.assert_not_awaited()


def test_get_tx_events_returns_empty_list_for_nonexistent_tx():
    from app.api.v1 import protocol_events

    service = _make_service(tx_events=[])
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/tx/{_VALID_TX_HASH}/events")

    assert response.status_code == 200
    assert response.json() == []
    service.get_events_by_tx.assert_awaited_once_with(_VALID_TX_HASH)


def test_list_protocol_events_returns_500_when_service_errors():
    from app.api.v1 import protocol_events

    service = _make_service()
    service.list_events.side_effect = ValueError("db failure")
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_service(service)
    try:
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get("/v1/protocol-events")

        assert response.status_code == 500
    finally:
        app.dependency_overrides.pop(protocol_events._get_protocol_event_service, None)


def test_get_tx_events_returns_500_when_service_errors():
    from app.api.v1 import protocol_events

    service = _make_service()
    service.get_events_by_tx.side_effect = ValueError("db failure")
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_service(service)
    try:
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get(f"/v1/tx/{_VALID_TX_HASH}/events")

        assert response.status_code == 500
    finally:
        app.dependency_overrides.pop(protocol_events._get_protocol_event_service, None)


def test_list_protocol_events_returns_422_for_wide_window_without_filter():
    from app.api.v1 import protocol_events

    service = _make_service()
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(
        "/v1/protocol-events",
        params={
            "from_timestamp": "2026-01-01T00:00:00Z",
            "to_timestamp": "2026-03-15T00:00:00Z",  # > 30d, no filter
        },
    )

    assert response.status_code == 422
    assert "selective filter" in response.json()["detail"]
    service.list_events.assert_not_awaited()


def test_list_protocol_events_allows_wide_window_with_tx_hash_filter():
    from app.api.v1 import protocol_events

    service = _make_service(events=[])
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(
        "/v1/protocol-events",
        params={
            "tx_hash": _VALID_TX_HASH,
            "from_timestamp": "2026-01-01T00:00:00Z",
            "to_timestamp": "2026-03-15T00:00:00Z",
            "resolution": "PT6H",
        },
    )

    assert response.status_code == 200
    service.list_events.assert_awaited_once()


def test_list_protocol_events_sets_public_cache_control_on_pinned_window():
    from app.api.v1 import protocol_events

    service = _make_service(events=[])
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(
        "/v1/protocol-events",
        params={
            "protocol_name": "spark",
            "from_timestamp": "2026-03-05T00:00:00Z",
            "to_timestamp": "2026-03-05T12:00:00Z",
        },
    )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "public, max-age=300"


def test_list_protocol_events_sets_no_store_when_bounds_not_pinned():
    from app.api.v1 import protocol_events

    service = _make_service(events=[])
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/protocol-events", params={"protocol_name": "spark"})

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"


def test_list_protocol_events_accepts_uppercase_0x_tx_hash():
    from app.api.v1 import protocol_events

    service = _make_service(events=[])
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/protocol-events", params={"tx_hash": "0X" + "AB" * 32})

    assert response.status_code == 200
    # Validator canonicalizes 0X -> 0x before passing to the service.
    assert service.list_events.await_args.kwargs["tx_hash"] == "0x" + "AB" * 32


def test_get_tx_events_accepts_uppercase_0x_tx_hash():
    from app.api.v1 import protocol_events

    service = _make_service(tx_events=[])
    app.dependency_overrides[protocol_events._get_protocol_event_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/tx/" + "0X" + "AB" * 32 + "/events")

    assert response.status_code == 200
    service.get_events_by_tx.assert_awaited_once_with("0x" + "AB" * 32)
