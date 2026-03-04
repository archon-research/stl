from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.domain.entities.allocation import Star
from app.main import app
from app.services.allocation_service import AllocationService
from tests.conftest import make_allocation_position


def _make_service(stars=None, positions=None) -> AllocationService:
    service = AsyncMock(spec=AllocationService)
    service.list_stars.return_value = stars or []
    service.list_allocations_by_star.return_value = positions or []
    return service


def _override_service(service: AllocationService):
    async def _dep():
        yield service

    return _dep


def test_list_stars_returns_200_with_star_names():
    from app.api.v1 import allocations

    service = _make_service(
        stars=[
            Star(id="0xaaa", name="grove", address="0xaaa"),
            Star(id="0xbbb", name="spark", address="0xbbb"),
        ]
    )
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/stars")

    assert response.status_code == 200
    assert response.json() == [
        {"id": "0xaaa", "name": "grove", "address": "0xaaa"},
        {"id": "0xbbb", "name": "spark", "address": "0xbbb"},
    ]


def test_list_stars_returns_empty_list_when_no_stars():
    from app.api.v1 import allocations

    service = _make_service(stars=[])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/stars")

    assert response.status_code == 200
    assert response.json() == []


def test_list_allocations_returns_200_with_positions():
    from app.api.v1 import allocations

    position = make_allocation_position()
    service = _make_service(positions=[position])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/stars/0xabc/allocations")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["star"] == "spark"
    assert data[0]["token_symbol"] == "USDC"
    assert data[0]["direction"] == "in"
    service.list_allocations_by_star.assert_awaited_once_with("0xabc", None)


def test_list_allocations_with_block_number_passes_param_to_service():
    from app.api.v1 import allocations

    position = make_allocation_position(block_number=5000)
    service = _make_service(positions=[position])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/stars/0xabc/allocations?block_number=5000")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["block_number"] == 5000
    service.list_allocations_by_star.assert_awaited_once_with("0xabc", 5000)


def test_list_allocations_returns_empty_for_unknown_star():
    from app.api.v1 import allocations

    service = _make_service(positions=[])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/stars/0xdeadbeef/allocations")

    assert response.status_code == 200
    assert response.json() == []


