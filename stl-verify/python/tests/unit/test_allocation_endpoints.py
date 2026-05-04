from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.domain.entities.allocation import EthAddress, Prime
from app.domain.entities.capital_metrics import CapitalMetrics
from app.main import app
from app.services.allocation_service import AllocationService
from app.services.capital_metrics_service import CapitalMetricsService
from tests.conftest import make_receipt_token_position

_VALID_ADDR = "0x" + "ab" * 20


def _make_service(primes=None, positions=None) -> AllocationService:
    service = AsyncMock(spec=AllocationService)
    service.list_primes.return_value = primes or []
    service.list_receipt_token_positions.return_value = positions or []
    return service


def _override_service(service: AllocationService):
    async def _dep():
        yield service

    return _dep


def test_list_primes_returns_200_with_prime_names():
    from app.api.v1 import allocations

    service = _make_service(
        primes=[
            Prime(id="0xaaa", name="grove", address="0xaaa"),
            Prime(id="0xbbb", name="spark", address="0xbbb"),
        ]
    )
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/primes")

    assert response.status_code == 200
    assert response.json() == [
        {"id": "0xaaa", "name": "grove", "address": "0xaaa"},
        {"id": "0xbbb", "name": "spark", "address": "0xbbb"},
    ]


def test_list_primes_returns_empty_list_when_no_primes():
    from app.api.v1 import allocations

    service = _make_service(primes=[])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/primes")

    assert response.status_code == 200
    assert response.json() == []


def test_list_allocations_returns_200_with_enriched_holdings():
    from app.api.v1 import allocations

    position = make_receipt_token_position()
    service = _make_service(positions=[position])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    data = response.json()
    assert data == [
        {
            "chain_id": 1,
            "receipt_token_id": 1,
            "receipt_token_address": "0x" + "a" * 40,
            "underlying_token_id": 10,
            "underlying_token_address": "0x" + "b" * 40,
            "symbol": "aUSDC",
            "underlying_symbol": "USDC",
            "protocol_name": "aave_v3",
            "balance": "100.0",
            "amount_usd": None,
            "latest_activity_at": None,
            "category": "allocation",
        }
    ]
    service.list_receipt_token_positions.assert_awaited_once_with(EthAddress(_VALID_ADDR))


def test_list_allocations_returns_empty_when_no_holdings():
    from app.api.v1 import allocations

    service = _make_service(positions=[])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    assert response.json() == []


def test_list_allocations_returns_422_for_invalid_prime_id():
    from app.api.v1 import allocations

    service = _make_service(positions=[])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/primes/0xdeadbeef/allocations")

    assert response.status_code == 422


# --- capital-metrics endpoint ---


def _make_capital_metrics_stub() -> CapitalMetrics:
    return CapitalMetrics(
        prime_id=_VALID_ADDR,
        prime_name="grove",
        risk_capital=Decimal("0"),
        capital_buffer=Decimal("0"),
        first_loss_capital=Decimal("0"),
        total_capital=Decimal("0"),
        risk_to_capital_ratio=Decimal("0"),
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        is_validated=False,
        validation_note="Capital metrics are pending accounting layer integration.",
    )


def test_list_capital_metrics_returns_200_with_all_primes():
    from app.api.v1 import allocations

    metrics = _make_capital_metrics_stub()
    metrics_two = CapitalMetrics(
        prime_id="0x" + "cd" * 20,
        prime_name="spark",
        risk_capital=Decimal("10"),
        capital_buffer=Decimal("2"),
        first_loss_capital=Decimal("1"),
        total_capital=Decimal("3"),
        risk_to_capital_ratio=Decimal("3.3333333333"),
        timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc),
        is_validated=False,
        validation_note="test",
    )
    mock_service = AsyncMock(spec=CapitalMetricsService)
    mock_service.list_all_capital_metrics.return_value = [metrics, metrics_two]
    app.dependency_overrides[allocations._get_capital_metrics_service] = lambda: mock_service
    client = TestClient(app)

    response = client.get("/v1/capital-metrics")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    assert data[0]["prime_id"] == _VALID_ADDR
    assert data[1]["prime_name"] == "spark"
    mock_service.list_all_capital_metrics.assert_awaited_once()


# --- data-sources endpoint ---


def test_get_data_sources_returns_200_with_sources():
    client = TestClient(app)

    response = client.get("/v1/data-sources")

    assert response.status_code == 200
    data = response.json()
    assert "sources" in data
    assert isinstance(data["sources"], list)
    assert len(data["sources"]) > 0


def test_get_star_risk_capital_requirements_returns_upstream_shape(monkeypatch):
    from app.api.v1 import allocations

    async def _fake_payload():
        return allocations.StarRiskCapitalResponse.model_validate(
            {
                "status": 200,
                "success": True,
                "data": {
                    "results": [
                        {
                            "star": "ALM",
                            "exposure": "123.45",
                            "total_rc": "67.89",
                            "financial_rrc": "12.34",
                            "exposure_share": "10.00%",
                            "risk_tolerance_ratio": "1.23",
                        }
                    ]
                },
            }
        )

    monkeypatch.setattr(allocations, "_fetch_star_risk_capital_payload", _fake_payload)
    client = TestClient(app)

    response = client.get("/v1/star-risk-capital/primes")

    assert response.status_code == 200
    assert response.json() == {
        "status": 200,
        "success": True,
        "data": {
            "results": [
                {
                    "star": "ALM",
                    "exposure": "123.45",
                    "total_rc": "67.89",
                    "financial_rrc": "12.34",
                    "exposure_share": "10.00%",
                    "risk_tolerance_ratio": "1.23",
                }
            ]
        },
    }


def test_get_star_risk_capital_requirements_returns_502_on_upstream_failure(monkeypatch):
    from app.api.v1 import allocations

    async def _raise_http_exception():
        raise HTTPException(status_code=502, detail="Risk capital upstream request failed")

    monkeypatch.setattr(allocations, "_fetch_star_risk_capital_payload", _raise_http_exception)
    client = TestClient(app)

    response = client.get("/v1/star-risk-capital/primes")

    assert response.status_code == 502
    assert response.json() == {"detail": "Risk capital upstream request failed"}
