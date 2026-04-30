from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

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


def test_get_capital_metrics_returns_200_for_known_prime():
    from app.api.v1 import allocations

    metrics = _make_capital_metrics_stub()
    mock_service = AsyncMock(spec=CapitalMetricsService)
    mock_service.get_capital_metrics.return_value = metrics
    app.dependency_overrides[allocations._get_capital_metrics_service] = lambda: mock_service
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/capital-metrics")

    assert response.status_code == 200
    data = response.json()
    assert data["prime_id"] == _VALID_ADDR
    assert data["prime_name"] == "grove"
    assert data["is_validated"] is False
    assert "timestamp" in data
    mock_service.get_capital_metrics.assert_awaited_once_with(EthAddress(_VALID_ADDR))


def test_get_capital_metrics_returns_404_for_unknown_prime():
    from app.api.v1 import allocations

    mock_service = AsyncMock(spec=CapitalMetricsService)
    mock_service.get_capital_metrics.return_value = None
    app.dependency_overrides[allocations._get_capital_metrics_service] = lambda: mock_service
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/capital-metrics")

    assert response.status_code == 404


# --- data-sources endpoint ---


def test_get_data_sources_returns_200_with_sources_and_methodology():
    client = TestClient(app)

    response = client.get("/v1/data-sources")

    assert response.status_code == 200
    data = response.json()
    assert "sources" in data
    assert "methodology_markdown" in data
    assert isinstance(data["sources"], list)
    assert len(data["sources"]) > 0
    assert isinstance(data["methodology_markdown"], str)
