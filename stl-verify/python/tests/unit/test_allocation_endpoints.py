from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.domain.entities.allocation import EthAddress, Prime
from app.main import app
from app.services.allocation_service import AllocationService
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


def test_list_capital_metrics_maps_star_risk_capital_data(monkeypatch):
    """Metrics are sourced from Star risk capital upstream, matched by prime name."""
    from app.api.v1 import allocations

    grove_addr = _VALID_ADDR
    spark_addr = "0x" + "cd" * 20

    service = _make_service(
        primes=[
            Prime(id=grove_addr, name="grove", address=grove_addr),
            Prime(id=spark_addr, name="spark", address=spark_addr),
        ]
    )
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    async def _fake_payload():
        return allocations.StarRiskCapitalResponse.model_validate(
            {
                "status": 200,
                "success": True,
                "data": {
                    "results": [
                        {
                            "star": "grove",
                            "exposure": "500.00",
                            "total_rc": "100.00",
                            "financial_rrc": "40.00",
                            "exposure_share": "50.00%",
                            "risk_tolerance_ratio": "5.00",
                        },
                        {
                            "star": "spark",
                            "exposure": "200.00",
                            "total_rc": "80.00",
                            "financial_rrc": "30.00",
                            "exposure_share": "20.00%",
                            "risk_tolerance_ratio": "2.50",
                        },
                    ]
                },
            }
        )

    monkeypatch.setattr(allocations, "_fetch_star_risk_capital_payload", _fake_payload)
    client = TestClient(app)

    response = client.get("/v1/capital-metrics")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2

    grove = next(m for m in data if m["prime_id"] == grove_addr)
    assert grove["risk_capital"] == "500.00"
    assert grove["total_capital"] == "100.00"
    assert grove["first_loss_capital"] == "40.00"
    assert grove["capital_buffer"] == "60.00"
    assert grove["risk_to_capital_ratio"] == "5.00"

    spark = next(m for m in data if m["prime_id"] == spark_addr)
    assert spark["risk_capital"] == "200.00"
    assert spark["risk_to_capital_ratio"] == "2.50"


def test_list_capital_metrics_skips_primes_with_no_star_row(monkeypatch):
    """Primes not present in Star risk capital data are omitted from results."""
    from app.api.v1 import allocations

    grove_addr = _VALID_ADDR
    service = _make_service(
        primes=[
            Prime(id=grove_addr, name="grove", address=grove_addr),
            Prime(id="0x" + "ee" * 20, name="unknown-prime", address="0x" + "ee" * 20),
        ]
    )
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    async def _fake_payload():
        return allocations.StarRiskCapitalResponse.model_validate(
            {
                "status": 200,
                "success": True,
                "data": {
                    "results": [
                        {
                            "star": "grove",
                            "exposure": "100.00",
                            "total_rc": "50.00",
                            "financial_rrc": "20.00",
                            "exposure_share": "10.00%",
                            "risk_tolerance_ratio": "2.00",
                        }
                    ]
                },
            }
        )

    monkeypatch.setattr(allocations, "_fetch_star_risk_capital_payload", _fake_payload)
    client = TestClient(app)

    response = client.get("/v1/capital-metrics")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["prime_name"] == "grove"


def test_list_capital_metrics_returns_502_for_invalid_numeric_payload(monkeypatch):
    from app.api.v1 import allocations

    grove_addr = _VALID_ADDR
    service = _make_service(primes=[Prime(id=grove_addr, name="grove", address=grove_addr)])
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    async def _fake_payload():
        return allocations.StarRiskCapitalResponse.model_validate(
            {
                "status": 200,
                "success": True,
                "data": {
                    "results": [
                        {
                            "star": "grove",
                            "exposure": "not-a-number",
                            "total_rc": "50.00",
                            "financial_rrc": "20.00",
                            "exposure_share": "10.00%",
                            "risk_tolerance_ratio": "2.00",
                        }
                    ]
                },
            }
        )

    monkeypatch.setattr(allocations, "_fetch_star_risk_capital_payload", _fake_payload)
    client = TestClient(app)

    response = client.get("/v1/capital-metrics")

    assert response.status_code == 502
    assert "invalid numeric value" in response.json()["detail"]


# --- data-sources endpoint ---


def test_get_data_sources_returns_200_with_sources():
    client = TestClient(app)

    response = client.get("/v1/data-sources")

    assert response.status_code == 200
    data = response.json()
    assert "sources" in data
    assert isinstance(data["sources"], list)
    assert len(data["sources"]) > 0


