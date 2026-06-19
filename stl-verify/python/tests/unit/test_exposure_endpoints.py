from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.domain.entities.allocation import EthAddress
from app.domain.entities.time_series_bucket import ExposureBucket
from app.main import app
from app.services.allocation_service import AllocationService

_VALID_ADDR = "0x" + "ab" * 20


def _make_service(*, exists: bool = True, buckets: list[ExposureBucket] | None = None) -> AsyncMock:
    service = AsyncMock(spec=AllocationService)
    service.prime_exists.return_value = exists
    service.list_exposure_buckets.return_value = buckets or []
    return service


def _override_service(service: AsyncMock):
    async def _dep():
        yield service

    return _dep


def test_list_prime_exposure_returns_aggregated_buckets():
    from app.api.v1 import exposure

    buckets = [
        ExposureBucket(
            bucket_start=datetime(2026, 6, 18, 0, 0, tzinfo=UTC),
            exposure_usd=Decimal("1459014561.88"),
        ),
        ExposureBucket(bucket_start=datetime(2026, 6, 17, 18, 0, tzinfo=UTC), exposure_usd=None),
    ]
    service = _make_service(buckets=buckets)
    app.dependency_overrides[exposure._get_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(
            f"/v1/primes/{_VALID_ADDR}/exposure",
            params={
                "from_timestamp": "2026-05-19T00:00:00Z",
                "to_timestamp": "2026-06-18T00:00:00Z",
                "resolution": "PT6H",
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert body["mode"] == "aggregated"
        assert body["data"] == [
            {"bucket_start": "2026-06-18T00:00:00Z", "exposure_usd": "1459014561.88"},
            {"bucket_start": "2026-06-17T18:00:00Z", "exposure_usd": None},
        ]
        assert service.list_exposure_buckets.await_args.args[0] == EthAddress(_VALID_ADDR)
        assert service.list_exposure_buckets.await_args.kwargs["bucket_seconds"] == 6 * 60 * 60
    finally:
        app.dependency_overrides.pop(exposure._get_service, None)


def test_list_prime_exposure_sets_public_cache_control_on_pinned_window():
    from app.api.v1 import exposure

    service = _make_service(buckets=[])
    app.dependency_overrides[exposure._get_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(
            f"/v1/primes/{_VALID_ADDR}/exposure",
            params={
                "from_timestamp": "2026-05-19T00:00:00Z",
                "to_timestamp": "2026-06-18T00:00:00Z",
            },
        )

        assert response.status_code == 200
        assert response.headers["cache-control"] == "public, max-age=300"
    finally:
        app.dependency_overrides.pop(exposure._get_service, None)


def test_list_prime_exposure_returns_404_when_prime_missing():
    from app.api.v1 import exposure

    service = _make_service(exists=False)
    app.dependency_overrides[exposure._get_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/exposure")

        assert response.status_code == 404
        assert response.json()["detail"] == "Prime not found"
        service.list_exposure_buckets.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(exposure._get_service, None)


def test_list_prime_exposure_returns_422_for_invalid_prime_id():
    from app.api.v1 import exposure

    service = _make_service()
    app.dependency_overrides[exposure._get_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get("/v1/primes/0xdeadbeef/exposure")

        assert response.status_code == 422
        service.prime_exists.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(exposure._get_service, None)
