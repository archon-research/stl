from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime_debt import PrimeDebtSnapshot
from app.main import app
from app.services.prime_debt_service import PrimeDebtService

_VALID_ADDR = "0x" + "ab" * 20


def _make_service(
    *,
    exists: bool = True,
    snapshots: list[PrimeDebtSnapshot] | None = None,
) -> PrimeDebtService:
    service = AsyncMock(spec=PrimeDebtService)
    service.prime_exists.return_value = exists
    service.list_debt_snapshots.return_value = snapshots or []
    return service


def _override_service(service: PrimeDebtService):
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
        assert response.json() == [
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
        service.list_debt_snapshots.assert_awaited_once_with(EthAddress(_VALID_ADDR), limit=25)
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
        assert response.json() == []
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


def test_list_prime_debt_snapshots_returns_422_for_address_without_prefix():
    from app.api.v1 import prime_debts

    service = _make_service()
    app.dependency_overrides[prime_debts._get_prime_debt_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get("/v1/primes/abababababababababababababababababababab/debt")

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
