from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.domain.entities.psm3 import Psm3Reserves
from app.main import app
from app.services.psm3_service import Psm3Service

_PSM3_ADDR = "0x" + "ab" * 20


def _reserves(network: str = "base") -> Psm3Reserves:
    return Psm3Reserves(
        network=network,
        address=_PSM3_ADDR,
        usds_balance=Decimal("2"),
        susds_balance=Decimal("4"),
        usdc_balance=Decimal("5"),
        usds_balance_usd=Decimal("1.98"),
        susds_balance_usd=Decimal("4.158"),
        usdc_balance_usd=Decimal("5.05"),
        usds_price=Decimal("0.99"),
        usdc_price=Decimal("1.01"),
        susds_price=Decimal("1.0395"),
        total_assets=Decimal("10"),
        block_number=30000000,
        block_version=0,
        block_timestamp=datetime(2026, 6, 12, 12, 0, tzinfo=UTC),
    )


def _install_service(rows: list[Psm3Reserves]) -> AsyncMock:
    from app.api.v1 import psm3

    service = AsyncMock(spec=Psm3Service)
    service.list_reserves.return_value = rows
    service.list_reserve_history.return_value = rows

    async def _dep():
        return service

    app.dependency_overrides[psm3._get_psm3_service] = _dep
    return service


def test_list_reserves_returns_rows_with_decimal_strings():
    service = _install_service([_reserves()])
    client = TestClient(app)

    response = client.get("/v1/psm3/reserves")

    assert response.status_code == 200
    assert response.json() == [
        {
            "network": "base",
            "address": _PSM3_ADDR,
            "usds_balance": "2",
            "susds_balance": "4",
            "usdc_balance": "5",
            "usds_balance_usd": "1.98",
            "susds_balance_usd": "4.158",
            "usdc_balance_usd": "5.05",
            "usds_price": "0.99",
            "usdc_price": "1.01",
            "susds_price": "1.0395",
            "total_assets": "10",
            "block_number": 30000000,
            "block_version": 0,
            "block_timestamp": "2026-06-12T12:00:00Z",
        }
    ]
    service.list_reserves.assert_awaited_once_with(None)


def test_list_reserves_passes_network_filter():
    service = _install_service([_reserves("optimism")])
    client = TestClient(app)

    response = client.get("/v1/psm3/reserves?network=optimism")

    assert response.status_code == 200
    assert response.json()[0]["network"] == "optimism"
    service.list_reserves.assert_awaited_once_with("optimism")


def test_list_reserves_returns_422_for_unknown_network():
    service = _install_service([])
    client = TestClient(app)

    response = client.get("/v1/psm3/reserves?network=mainnet")

    assert response.status_code == 422
    service.list_reserves.assert_not_awaited()


def test_list_reserve_history_returns_rows():
    service = _install_service([_reserves()])
    client = TestClient(app)

    response = client.get("/v1/psm3/reserves/history?network=base&limit=25")

    assert response.status_code == 200
    assert len(response.json()) == 1
    service.list_reserve_history.assert_awaited_once_with("base", limit=25)


def test_list_reserve_history_returns_422_when_network_missing():
    service = _install_service([])
    client = TestClient(app)

    response = client.get("/v1/psm3/reserves/history")

    assert response.status_code == 422
    service.list_reserve_history.assert_not_awaited()


def test_list_reserve_history_returns_422_for_limit_zero():
    service = _install_service([])
    client = TestClient(app)

    response = client.get("/v1/psm3/reserves/history?network=base&limit=0")

    assert response.status_code == 422
    service.list_reserve_history.assert_not_awaited()


def test_list_reserve_history_returns_422_for_limit_too_large():
    service = _install_service([])
    client = TestClient(app)

    response = client.get("/v1/psm3/reserves/history?network=base&limit=501")

    assert response.status_code == 422
    service.list_reserve_history.assert_not_awaited()


def test_network_literal_matches_domain_mapping():
    from typing import get_args

    from app.api.v1.psm3 import Network
    from app.domain.entities.psm3 import NETWORK_TO_CHAIN_ID

    assert set(get_args(Network)) == set(NETWORK_TO_CHAIN_ID)


def test_list_reserves_returns_500_when_service_errors():
    service = _install_service([])
    service.list_reserves.side_effect = ValueError("db failure")
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/v1/psm3/reserves")

    assert response.status_code == 500
