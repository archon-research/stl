from decimal import Decimal
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime_risk_capital import AllocationRiskCapital, PrimeRiskCapital
from app.main import app
from app.services.prime_risk_capital_service import PrimeRiskCapitalService

_VALID_ADDR = "0x" + "ab" * 20


def _make_service(*, exists: bool = True, result: PrimeRiskCapital | None = None) -> AsyncMock:
    service = AsyncMock(spec=PrimeRiskCapitalService)
    service.prime_exists.return_value = exists
    service.compute.return_value = result
    return service


def _override_service(service: AsyncMock):
    async def _dep():
        yield service

    return _dep


def _result() -> PrimeRiskCapital:
    return PrimeRiskCapital(
        prime_id=_VALID_ADDR,
        model="gap_sweep",
        exposure_usd=Decimal("1000"),
        total_risk_capital_usd=Decimal("100"),
        required_risk_capital_usd=Decimal("30"),
        encumbrance_ratio=Decimal("0.3000"),
        modeled_exposure_usd=Decimal("600"),
        modeled_pct=Decimal("0.6000"),
        per_allocation=[
            AllocationRiskCapital(
                receipt_token_id=1,
                symbol="spUSDT",
                protocol_name="SparkLend",
                exposure_usd=Decimal("600"),
                applied=True,
                required_risk_capital_usd=Decimal("30"),
                crr_pct=Decimal("5"),
                model="gap_sweep",
            ),
            AllocationRiskCapital(
                receipt_token_id=2,
                symbol="spDAI",
                protocol_name="SparkLend",
                exposure_usd=Decimal("400"),
                applied=False,
                required_risk_capital_usd=None,
                crr_pct=None,
                model=None,
                unpriced_reason="no_model",
            ),
        ],
    )


def test_get_prime_risk_capital_returns_self_computed_envelope():
    from app.api.v1 import prime_risk_capital

    service = _make_service(result=_result())
    app.dependency_overrides[prime_risk_capital._get_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital")

        assert response.status_code == 200
        body = response.json()
        assert body["model"] == "gap_sweep"
        assert body["exposure_usd"] == "1000"
        assert body["total_risk_capital_usd"] == "100"
        assert body["required_risk_capital_usd"] == "30"
        assert body["encumbrance_ratio"] == "0.3000"
        assert body["modeled_pct"] == "0.6000"
        assert len(body["per_allocation"]) == 2
        modeled = body["per_allocation"][0]
        assert modeled["applied"] is True
        assert modeled["required_risk_capital_usd"] == "30"
        assert modeled["unpriced_reason"] is None
        unmodeled = body["per_allocation"][1]
        assert unmodeled["applied"] is False
        assert unmodeled["required_risk_capital_usd"] is None
        assert unmodeled["unpriced_reason"] == "no_model"
        service.prime_exists.assert_awaited_once_with(EthAddress(_VALID_ADDR))
    finally:
        app.dependency_overrides.pop(prime_risk_capital._get_service, None)


def test_get_prime_risk_capital_returns_404_when_prime_missing():
    from app.api.v1 import prime_risk_capital

    service = _make_service(exists=False)
    app.dependency_overrides[prime_risk_capital._get_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital")

        assert response.status_code == 404
        assert response.json()["detail"] == "Prime not found"
        service.compute.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(prime_risk_capital._get_service, None)


def test_get_prime_risk_capital_reports_share_missing_allocation_as_unpriced():
    """A backed allocation whose share lookup failed is surfaced as unpriced
    (200, applied=false + unpriced_reason), not a whole-response 503."""
    from app.api.v1 import prime_risk_capital

    result = PrimeRiskCapital(
        prime_id=_VALID_ADDR,
        model="gap_sweep",
        exposure_usd=Decimal("1000"),
        total_risk_capital_usd=Decimal("100"),
        required_risk_capital_usd=Decimal("0"),
        encumbrance_ratio=Decimal("0.0000"),
        modeled_exposure_usd=Decimal("0"),
        modeled_pct=Decimal("0.0000"),
        per_allocation=[
            AllocationRiskCapital(
                receipt_token_id=1,
                symbol="spDAI",
                protocol_name="SparkLend",
                exposure_usd=Decimal("1000"),
                applied=False,
                required_risk_capital_usd=None,
                crr_pct=None,
                model=None,
                unpriced_reason="share_data_missing",
            ),
        ],
    )
    service = _make_service(result=result)
    app.dependency_overrides[prime_risk_capital._get_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital")

        assert response.status_code == 200
        alloc = response.json()["per_allocation"][0]
        assert alloc["applied"] is False
        assert alloc["required_risk_capital_usd"] is None
        assert alloc["unpriced_reason"] == "share_data_missing"
    finally:
        app.dependency_overrides.pop(prime_risk_capital._get_service, None)


def test_get_prime_risk_capital_returns_422_for_invalid_prime_id():
    from app.api.v1 import prime_risk_capital

    service = _make_service()
    app.dependency_overrides[prime_risk_capital._get_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get("/v1/primes/0xdeadbeef/risk-capital")

        assert response.status_code == 422
        service.prime_exists.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(prime_risk_capital._get_service, None)
