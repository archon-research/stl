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
