from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.domain.entities.allocation import EthAddress
from app.domain.entities.time_series_bucket import TotalCapitalBucket
from app.main import app
from app.services.allocation_service import AllocationService

_VALID_ADDR = "0x" + "ab" * 20


def _make_service(
    *,
    exists: bool = True,
    buckets: list[TotalCapitalBucket] | None = None,
) -> AsyncMock:
    service = AsyncMock(spec=AllocationService)
    service.prime_exists.return_value = exists
    service.list_total_capital_buckets.return_value = buckets or []
    return service


def _override_service(service: AsyncMock):
    async def _dep():
        yield service

    return _dep


def test_list_prime_total_capital_returns_aggregated_buckets():
    from app.api.v1 import total_capital

    buckets = [
        TotalCapitalBucket(
            bucket_start=datetime(2026, 6, 18, 0, 0, tzinfo=UTC),
            total_capital_usd=Decimal("36359440.25"),
        ),
        TotalCapitalBucket(
            bucket_start=datetime(2026, 6, 17, 18, 0, tzinfo=UTC),
            total_capital_usd=None,
        ),
    ]
    service = _make_service(buckets=buckets)
    app.dependency_overrides[total_capital._get_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(
            f"/v1/primes/{_VALID_ADDR}/total-capital",
            params={
                "from_timestamp": "2026-05-19T00:00:00Z",
                "to_timestamp": "2026-06-18T00:00:00Z",
                "resolution": "PT6H",
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert body["mode"] == "aggregated"
        assert body["window"]["resolution"] == "PT6H"
        assert body["data"] == [
            {"bucket_start": "2026-06-18T00:00:00Z", "total_capital_usd": "36359440.25"},
            {"bucket_start": "2026-06-17T18:00:00Z", "total_capital_usd": None},
        ]
        service.prime_exists.assert_awaited_once_with(EthAddress(_VALID_ADDR))
        assert service.list_total_capital_buckets.await_args.args[0] == EthAddress(_VALID_ADDR)
        kwargs = service.list_total_capital_buckets.await_args.kwargs
        assert kwargs["bucket_seconds"] == 6 * 60 * 60
        assert kwargs["from_timestamp"] == datetime(2026, 5, 19, 0, 0, tzinfo=UTC)
        assert kwargs["to_timestamp"] == datetime(2026, 6, 18, 0, 0, tzinfo=UTC)
    finally:
        app.dependency_overrides.pop(total_capital._get_service, None)


def test_list_prime_total_capital_returns_empty_when_no_buckets():
    from app.api.v1 import total_capital

    service = _make_service(buckets=[])
    app.dependency_overrides[total_capital._get_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/total-capital")

        assert response.status_code == 200
        body = response.json()
        assert body["mode"] == "aggregated"
        assert body["data"] == []
    finally:
        app.dependency_overrides.pop(total_capital._get_service, None)


def test_list_prime_total_capital_returns_404_when_prime_missing():
    from app.api.v1 import total_capital

    service = _make_service(exists=False)
    app.dependency_overrides[total_capital._get_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/total-capital")

        assert response.status_code == 404
        assert response.json()["detail"] == "Prime not found"
        service.list_total_capital_buckets.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(total_capital._get_service, None)


def test_list_prime_total_capital_returns_422_for_invalid_prime_id():
    from app.api.v1 import total_capital

    service = _make_service()
    app.dependency_overrides[total_capital._get_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get("/v1/primes/0xdeadbeef/total-capital")

        assert response.status_code == 422
        service.prime_exists.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(total_capital._get_service, None)


def test_list_prime_total_capital_returns_422_for_limit_too_large():
    from app.api.v1 import total_capital

    service = _make_service()
    app.dependency_overrides[total_capital._get_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/total-capital?limit=600")

        assert response.status_code == 422
        service.prime_exists.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(total_capital._get_service, None)


def test_list_prime_total_capital_returns_500_when_service_errors():
    from app.api.v1 import total_capital

    service = _make_service()
    service.list_total_capital_buckets.side_effect = ValueError("db failure")
    app.dependency_overrides[total_capital._get_service] = _override_service(service)
    try:
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/total-capital")

        assert response.status_code == 500
    finally:
        app.dependency_overrides.pop(total_capital._get_service, None)
