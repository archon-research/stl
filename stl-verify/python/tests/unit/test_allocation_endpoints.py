from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.domain.entities.allocation import EthAddress, Prime
from app.main import app
from app.services.allocation_service import AllocationService
from tests.conftest import make_allocation_position

_VALID_ADDR = "0x" + "ab" * 20


def _make_service(primes=None, positions=None) -> AllocationService:
    service = AsyncMock(spec=AllocationService)
    service.list_primes.return_value = primes or []
    service.list_allocations_by_prime.return_value = positions or []
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


def test_list_allocations_returns_200_with_positions():
    from app.api.v1 import allocations

    position = make_allocation_position()
    service = _make_service(positions=[position])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["name"] == "spark"
    assert data[0]["token_symbol"] == "USDC"
    assert data[0]["direction"] == "in"
    service.list_allocations_by_prime.assert_awaited_once_with(EthAddress(_VALID_ADDR), None)


def test_list_allocations_with_block_number_passes_param_to_service():
    from app.api.v1 import allocations

    position = make_allocation_position(block_number=5000)
    service = _make_service(positions=[position])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?block_number=5000")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["block_number"] == 5000
    service.list_allocations_by_prime.assert_awaited_once_with(EthAddress(_VALID_ADDR), 5000)


def test_list_allocations_returns_422_for_invalid_prime_id():
    from app.api.v1 import allocations

    service = _make_service(positions=[])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/primes/0xdeadbeef/allocations")

    assert response.status_code == 422
