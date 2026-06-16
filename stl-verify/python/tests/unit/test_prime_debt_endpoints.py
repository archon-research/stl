from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime_debt import PrimeDebtSnapshot
from app.domain.entities.time_series_bucket import PrimeDebtBucket
from app.main import app
from app.services.prime_debt_service import PrimeDebtService

_VALID_ADDR = "0x" + "ab" * 20


def _make_service(
    *,
    exists: bool = True,
    snapshots: list[PrimeDebtSnapshot] | None = None,
    buckets: list[PrimeDebtBucket] | None = None,
) -> AsyncMock:
    service = AsyncMock(spec=PrimeDebtService)
    service.prime_exists.return_value = exists
    service.list_debt_snapshots.return_value = snapshots or []
    service.list_debt_buckets.return_value = buckets or []
    return service


def _override_service(service: AsyncMock):
    async def _dep():
        yield service

    return _dep


def _snapshot() -> PrimeDebtSnapshot:
    return PrimeDebtSnapshot(
        prime_address=_VALID_ADDR,
        prime_name="spark",
        ilk_name="ETH-A",
        debt_wad=Decimal("123456789.123"),
        block_number=22000123,
        block_version=0,
        synced_at=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
    )


def test_list_prime_debt_snapshots_returns_rows():
    from app.api.v1 import prime_debts

    snap = _snapshot()
    service = _make_service(snapshots=[snap])
    app.dependency_overrides[prime_debts._get_prime_debt_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/debt?limit=25")

        assert response.status_code == 200
        body = response.json()
        assert body["mode"] == "raw"
        assert body["window"]["resolution"] == "PT5M"
        assert body["window"]["interval_ms"] == 5 * 60 * 1000
        assert body["data"] == [
            {
                "prime_address": _VALID_ADDR,
                "prime_name": "spark",
                "ilk_name": "ETH-A",
                "debt_wad": "123456789.123",
                "block_number": 22000123,
                "block_version": 0,
                "synced_at": "2026-03-05T12:00:00Z",
            }
        ]
        service.prime_exists.assert_awaited_once_with(EthAddress(_VALID_ADDR))
        kwargs = service.list_debt_snapshots.await_args.kwargs
        assert service.list_debt_snapshots.await_args.args[0] == EthAddress(_VALID_ADDR)
        assert kwargs["limit"] == 25
        # Bounds are timezone-aware UTC and default to a 24h window.
        assert kwargs["from_timestamp"].tzinfo is not None
        assert kwargs["to_timestamp"].tzinfo is not None
        assert (kwargs["to_timestamp"] - kwargs["from_timestamp"]).total_seconds() == 24 * 60 * 60
    finally:
        app.dependency_overrides.pop(prime_debts._get_prime_debt_service, None)


def test_list_prime_debt_snapshots_returns_empty_when_prime_has_no_snapshots():
    from app.api.v1 import prime_debts

    service = _make_service(snapshots=[])
    app.dependency_overrides[prime_debts._get_prime_debt_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/debt")

        assert response.status_code == 200
        body = response.json()
        assert body["mode"] == "raw"
        assert body["data"] == []
    finally:
        app.dependency_overrides.pop(prime_debts._get_prime_debt_service, None)


def test_list_prime_debt_returns_aggregated_buckets():
    from app.api.v1 import prime_debts

    buckets = [
        PrimeDebtBucket(bucket_start=datetime(2026, 3, 5, 12, 0, tzinfo=UTC), debt_wad=Decimal("1000")),
        PrimeDebtBucket(bucket_start=datetime(2026, 3, 5, 11, 0, tzinfo=UTC), debt_wad=None),
    ]
    service = _make_service(buckets=buckets)
    app.dependency_overrides[prime_debts._get_prime_debt_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(
            f"/v1/primes/{_VALID_ADDR}/debt",
            params={
                "from_timestamp": "2026-03-04T12:00:00Z",
                "to_timestamp": "2026-03-05T12:00:00Z",
                "aggregate": "true",
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert body["mode"] == "aggregated"
        assert body["data"] == [
            {"bucket_start": "2026-03-05T12:00:00Z", "debt_wad": "1000"},
            {"bucket_start": "2026-03-05T11:00:00Z", "debt_wad": None},
        ]
        kwargs = service.list_debt_buckets.await_args.kwargs
        assert kwargs["bucket_seconds"] == 5 * 60  # 24h window -> PT5M default
        service.list_debt_snapshots.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(prime_debts._get_prime_debt_service, None)


def test_list_prime_debt_snapshots_returns_404_when_prime_missing():
    from app.api.v1 import prime_debts

    service = _make_service(exists=False)
    app.dependency_overrides[prime_debts._get_prime_debt_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/debt")

        assert response.status_code == 404
        assert response.json()["detail"] == "Prime not found"
    finally:
        app.dependency_overrides.pop(prime_debts._get_prime_debt_service, None)


def test_list_prime_debt_snapshots_returns_422_for_invalid_prime_id():
    from app.api.v1 import prime_debts

    service = _make_service()
    app.dependency_overrides[prime_debts._get_prime_debt_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get("/v1/primes/0xdeadbeef/debt")

        assert response.status_code == 422
    finally:
        app.dependency_overrides.pop(prime_debts._get_prime_debt_service, None)


def test_list_prime_debt_snapshots_returns_422_for_limit_too_large():
    from app.api.v1 import prime_debts

    service = _make_service(snapshots=[])
    app.dependency_overrides[prime_debts._get_prime_debt_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/debt?limit=600")

        assert response.status_code == 422
        service.prime_exists.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(prime_debts._get_prime_debt_service, None)


def test_list_prime_debt_snapshots_returns_422_for_malformed_timestamp():
    from app.api.v1 import prime_debts

    service = _make_service()
    app.dependency_overrides[prime_debts._get_prime_debt_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/debt?from_timestamp=not-a-date")

        assert response.status_code == 422
        service.prime_exists.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(prime_debts._get_prime_debt_service, None)


def test_list_prime_debt_snapshots_returns_422_for_address_without_prefix():
    from app.api.v1 import prime_debts

    service = _make_service()
    app.dependency_overrides[prime_debts._get_prime_debt_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{'ab' * 20}/debt")

        assert response.status_code == 422
        service.prime_exists.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(prime_debts._get_prime_debt_service, None)


def test_list_prime_debt_snapshots_returns_422_for_address_too_long():
    from app.api.v1 import prime_debts

    service = _make_service()
    app.dependency_overrides[prime_debts._get_prime_debt_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get("/v1/primes/0xabababababababababababababababababababababab/debt")

        assert response.status_code == 422
        service.prime_exists.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(prime_debts._get_prime_debt_service, None)


def test_list_prime_debt_snapshots_returns_500_when_service_errors():
    from app.api.v1 import prime_debts

    service = _make_service()
    service.list_debt_snapshots.side_effect = ValueError("db failure")
    app.dependency_overrides[prime_debts._get_prime_debt_service] = _override_service(service)
    try:
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/debt")

        assert response.status_code == 500
    finally:
        app.dependency_overrides.pop(prime_debts._get_prime_debt_service, None)


def test_list_prime_debt_snapshots_forwards_explicit_time_window():
    from app.api.v1 import prime_debts

    service = _make_service(snapshots=[])
    app.dependency_overrides[prime_debts._get_prime_debt_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(
            f"/v1/primes/{_VALID_ADDR}/debt",
            params={
                "from_timestamp": "2026-03-01T00:00:00Z",
                "to_timestamp": "2026-03-05T00:00:00Z",
                "resolution": "PT15M",
            },
        )

        assert response.status_code == 200
        kwargs = service.list_debt_snapshots.await_args.kwargs
        assert kwargs["from_timestamp"] == datetime(2026, 3, 1, 0, 0, tzinfo=UTC)
        assert kwargs["to_timestamp"] == datetime(2026, 3, 5, 0, 0, tzinfo=UTC)
    finally:
        app.dependency_overrides.pop(prime_debts._get_prime_debt_service, None)
