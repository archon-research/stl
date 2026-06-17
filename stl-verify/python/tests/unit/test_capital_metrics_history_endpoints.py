from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.domain.entities.capital_metrics import CapitalMetricsSnapshot
from app.domain.entities.time_series_bucket import CapitalMetricsBucket
from app.main import app
from app.services.capital_metrics_history_service import CapitalMetricsHistoryService

_VALID_ADDR = "0x" + "ab" * 20


def _make_service(
    *,
    exists: bool = True,
    snapshots: list[CapitalMetricsSnapshot] | None = None,
    buckets: list[CapitalMetricsBucket] | None = None,
) -> AsyncMock:
    service = AsyncMock(spec=CapitalMetricsHistoryService)
    service.prime_exists.return_value = exists
    service.list_snapshots.return_value = snapshots or []
    service.list_buckets.return_value = buckets or []
    return service


def _override_service(service: AsyncMock):
    async def _dep():
        yield service

    return _dep


def _snapshot() -> CapitalMetricsSnapshot:
    return CapitalMetricsSnapshot(
        prime_address=_VALID_ADDR,
        prime_name="spark",
        risk_capital=Decimal("100"),
        total_capital=Decimal("200"),
        first_loss_capital=Decimal("50"),
        capital_buffer=Decimal("150"),
        risk_to_capital_ratio=Decimal("0.85"),
        benchmark_source="https://example.com/star",
        synced_at=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
    )


def test_returns_raw_snapshots():
    from app.api.v1 import capital_metrics_history

    service = _make_service(snapshots=[_snapshot()])
    app.dependency_overrides[capital_metrics_history._get_capital_metrics_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/capital-metrics?limit=25")

        assert response.status_code == 200
        body = response.json()
        assert body["mode"] == "raw"
        assert body["window"]["resolution"] == "PT5M"
        assert body["data"] == [
            {
                "prime_address": _VALID_ADDR,
                "prime_name": "spark",
                "risk_capital": "100",
                "total_capital": "200",
                "first_loss_capital": "50",
                "capital_buffer": "150",
                "risk_to_capital_ratio": "0.85",
                "benchmark_source": "https://example.com/star",
                "synced_at": "2026-03-05T12:00:00Z",
            }
        ]
        kwargs = service.list_snapshots.await_args.kwargs
        assert kwargs["limit"] == 25
        assert kwargs["from_timestamp"].tzinfo is not None
        assert (kwargs["to_timestamp"] - kwargs["from_timestamp"]).total_seconds() == 24 * 60 * 60
        service.list_buckets.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(capital_metrics_history._get_capital_metrics_service, None)


def test_returns_aggregated_buckets():
    from app.api.v1 import capital_metrics_history

    buckets = [
        CapitalMetricsBucket(
            bucket_start=datetime(2026, 3, 5, 12, 0, tzinfo=UTC),
            risk_capital=Decimal("100"),
            total_capital=Decimal("200"),
            first_loss_capital=Decimal("50"),
            capital_buffer=Decimal("150"),
            risk_to_capital_ratio=Decimal("0.85"),
        ),
        CapitalMetricsBucket(
            bucket_start=datetime(2026, 3, 5, 11, 0, tzinfo=UTC),
            risk_capital=None,
            total_capital=None,
            first_loss_capital=None,
            capital_buffer=None,
            risk_to_capital_ratio=None,
        ),
    ]
    service = _make_service(buckets=buckets)
    app.dependency_overrides[capital_metrics_history._get_capital_metrics_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(
            f"/v1/primes/{_VALID_ADDR}/capital-metrics",
            params={
                "from_timestamp": "2026-03-04T12:00:00Z",
                "to_timestamp": "2026-03-05T12:00:00Z",
                "aggregate": "true",
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert body["mode"] == "aggregated"
        assert body["data"][0]["capital_buffer"] == "150"
        assert body["data"][1]["capital_buffer"] is None
        assert body["data"][1]["total_capital"] is None
        kwargs = service.list_buckets.await_args.kwargs
        assert kwargs["bucket_seconds"] == 5 * 60  # 24h window -> PT5M default
        service.list_snapshots.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(capital_metrics_history._get_capital_metrics_service, None)


def test_unknown_prime_returns_404():
    from app.api.v1 import capital_metrics_history

    service = _make_service(exists=False)
    app.dependency_overrides[capital_metrics_history._get_capital_metrics_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/capital-metrics")

        assert response.status_code == 404
        service.list_snapshots.assert_not_awaited()
        service.list_buckets.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(capital_metrics_history._get_capital_metrics_service, None)
