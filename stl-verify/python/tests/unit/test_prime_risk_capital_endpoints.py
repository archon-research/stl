from decimal import Decimal
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime_risk_capital import AllocationRiskCapital, ChainRiskCapital, PrimeRiskCapital
from app.main import app
from app.services.prime_risk_capital_service import PrimeRiskCapitalService

_VALID_ADDR = "0x" + "ab" * 20
# A real axis-synome SubProxy: the gate resolves kind through the contract, so a
# placeholder address would classify as ALM and not exercise it.
_SPARK_SUB_PROXY = "0x3300f198988e4c9c63f75df86de36421f06af8c4"


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
        proxy_address=_VALID_ADDR,
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
        prime_name="spark",
        prime_exposure_usd=Decimal("1400"),
        prime_required_risk_capital_usd=Decimal("42"),
        prime_modeled_exposure_usd=Decimal("900"),
        prime_modeled_pct=Decimal("0.6429"),
        prime_encumbrance_ratio=Decimal("0.4200"),
        prime_proxies=(_VALID_ADDR,),
        prime_per_chain=(
            ChainRiskCapital(
                proxy_address=_VALID_ADDR,
                chain="mainnet",
                exposure_usd=Decimal("1000"),
                required_risk_capital_usd=Decimal("30"),
                allocation_count=2,
            ),
        ),
    )


def test_get_prime_risk_capital_serializes_large_usd_as_plain_string():
    # exposure_usd/total_risk_capital_usd come straight from DB NUMERIC, so
    # asyncpg can hand back positive-exponent Decimals here just like debt_wad.
    # They must serialize as plain strings, not scientific notation.
    from app.api.v1 import prime_risk_capital

    big_usd = Decimal((0, (2, 1, 9, 9), 4))  # 2.199E+7
    assert "E+" in str(big_usd)

    result = PrimeRiskCapital(
        proxy_address=_VALID_ADDR,
        model="gap_sweep",
        exposure_usd=big_usd,
        total_risk_capital_usd=big_usd,
        required_risk_capital_usd=Decimal("0"),
        encumbrance_ratio=None,
        modeled_exposure_usd=Decimal("0"),
        modeled_pct=None,
        per_allocation=[
            AllocationRiskCapital(
                receipt_token_id=1,
                symbol="aHorRwaRLUSD",
                protocol_name="aave-v3-rwa",
                exposure_usd=big_usd,
                applied=True,
                required_risk_capital_usd=Decimal("0"),
                crr_pct=Decimal("0"),
                model="gap_sweep",
            ),
        ],
    )
    service = _make_service(result=result)
    app.dependency_overrides[prime_risk_capital._get_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital")

        assert response.status_code == 200
        body = response.json()
        assert body["exposure_usd"] == "21990000"
        assert body["total_risk_capital_usd"] == "21990000"
        assert body["per_allocation"][0]["exposure_usd"] == "21990000"
    finally:
        app.dependency_overrides.pop(prime_risk_capital._get_service, None)


def test_get_prime_risk_capital_serializes_the_prime_scoped_figures_as_plain_strings():
    """The prime_ figures are sums of DB NUMERICs, so they carry the same exponent risk.

    ``prime_exposure_usd`` sums per-proxy exposure straight from ``allocation_position``,
    so asyncpg's positive-exponent Decimals propagate into the aggregate exactly as they
    do into the proxy-scoped fields. A consumer parsing with BigInt reads an exponential
    form as 0, which is what made prime debt render as zero before PlainDecimal landed.
    Asserted here because the sibling test above covers only the unprefixed fields.
    """
    from app.api.v1 import prime_risk_capital

    big_usd = Decimal((0, (2, 1, 9, 9), 4))  # 2.199E+7
    assert "E+" in str(big_usd)

    result = PrimeRiskCapital(
        proxy_address=_VALID_ADDR,
        model="gap_sweep",
        exposure_usd=big_usd,
        total_risk_capital_usd=big_usd,
        required_risk_capital_usd=Decimal("0"),
        encumbrance_ratio=None,
        modeled_exposure_usd=Decimal("0"),
        modeled_pct=None,
        per_allocation=[],
        prime_name="spark",
        prime_exposure_usd=big_usd,
        prime_required_risk_capital_usd=big_usd,
        prime_modeled_exposure_usd=big_usd,
        prime_per_chain=(
            ChainRiskCapital(
                proxy_address=_VALID_ADDR,
                chain="mainnet",
                exposure_usd=big_usd,
                required_risk_capital_usd=big_usd,
                allocation_count=1,
            ),
        ),
    )
    service = _make_service(result=result)
    app.dependency_overrides[prime_risk_capital._get_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital")

        assert response.status_code == 200
        body = response.json()
        assert body["prime_exposure_usd"] == "21990000"
        assert body["prime_required_risk_capital_usd"] == "21990000"
        assert body["prime_modeled_exposure_usd"] == "21990000"
        assert body["prime_per_chain"][0]["exposure_usd"] == "21990000"
        assert body["prime_per_chain"][0]["required_risk_capital_usd"] == "21990000"
    finally:
        app.dependency_overrides.pop(prime_risk_capital._get_service, None)


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


def test_get_prime_risk_capital_returns_404_for_a_subproxy_treasury_wallet():
    """A SubProxy holds the treasury, not allocations, so it has no prime aggregate.

    ``exists=True`` because a SubProxy does have ``allocation_position`` rows, so
    the not-found gate above does not catch it: without its own gate the request
    200s with the treasury folded into the prime-scoped fields, giving one extra
    ``prime_proxies`` entry and a ``chain: null`` row that no ALM proxy reports.
    """
    from app.api.v1 import prime_risk_capital

    service = _make_service(exists=True, result=_result())
    app.dependency_overrides[prime_risk_capital._get_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_SPARK_SUB_PROXY}/risk-capital")

        assert response.status_code == 404
        assert "SubProxy" in response.json()["detail"]
        service.compute.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(prime_risk_capital._get_service, None)


def test_get_prime_risk_capital_reports_share_missing_allocation_as_unpriced():
    """A backed allocation whose share lookup failed is surfaced as unpriced
    (200, applied=false + unpriced_reason), not a whole-response 503."""
    from app.api.v1 import prime_risk_capital

    result = PrimeRiskCapital(
        proxy_address=_VALID_ADDR,
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


def test_get_prime_risk_capital_exposes_the_prime_scoped_figures():
    from app.api.v1 import prime_risk_capital

    service = _make_service(result=_result())
    app.dependency_overrides[prime_risk_capital._get_service] = _override_service(service)
    try:
        body = TestClient(app).get(f"/v1/primes/{_VALID_ADDR}/risk-capital").json()

        assert body["prime_name"] == "spark"
        assert body["prime_required_risk_capital_usd"] == "42"
        assert body["prime_encumbrance_ratio"] == "0.4200"
    finally:
        app.dependency_overrides.clear()


def test_get_prime_risk_capital_still_returns_the_proxy_scoped_figures():
    from app.api.v1 import prime_risk_capital

    service = _make_service(result=_result())
    app.dependency_overrides[prime_risk_capital._get_service] = _override_service(service)
    try:
        body = TestClient(app).get(f"/v1/primes/{_VALID_ADDR}/risk-capital").json()

        assert body["required_risk_capital_usd"] == "30"
        assert body["encumbrance_ratio"] == "0.3000"
    finally:
        app.dependency_overrides.clear()


def test_get_prime_risk_capital_reports_the_per_chain_breakdown():
    from app.api.v1 import prime_risk_capital

    service = _make_service(result=_result())
    app.dependency_overrides[prime_risk_capital._get_service] = _override_service(service)
    try:
        body = TestClient(app).get(f"/v1/primes/{_VALID_ADDR}/risk-capital").json()

        assert body["prime_per_chain"] == [
            {
                "proxy_address": _VALID_ADDR,
                "chain": "mainnet",
                "exposure_usd": "1000",
                "required_risk_capital_usd": "30",
                "allocation_count": 2,
            }
        ]
    finally:
        app.dependency_overrides.clear()


def test_encumbrance_ratio_is_marked_deprecated_in_the_schema():
    properties = app.openapi()["components"]["schemas"]["PrimeRiskCapitalResponse"]["properties"]

    assert properties["encumbrance_ratio"]["deprecated"] is True


def test_prime_encumbrance_ratio_is_not_deprecated():
    properties = app.openapi()["components"]["schemas"]["PrimeRiskCapitalResponse"]["properties"]

    assert "deprecated" not in properties["prime_encumbrance_ratio"]


def test_get_prime_risk_capital_names_the_proxy_the_unprefixed_figures_belong_to():
    """`proxy_address` is what a client fanning out matches responses to requests by."""
    from app.api.v1 import prime_risk_capital

    service = _make_service(result=_result())
    app.dependency_overrides[prime_risk_capital._get_service] = _override_service(service)
    try:
        response = TestClient(app).get(f"/v1/primes/{_VALID_ADDR}/risk-capital")

        assert response.json()["proxy_address"] == _VALID_ADDR
    finally:
        app.dependency_overrides.pop(prime_risk_capital._get_service, None)


def test_get_prime_risk_capital_keeps_the_deprecated_prime_id_identical_to_proxy_address():
    # The value must not move for a consumer still reading the old key.
    from app.api.v1 import prime_risk_capital

    service = _make_service(result=_result())
    app.dependency_overrides[prime_risk_capital._get_service] = _override_service(service)
    try:
        body = TestClient(app).get(f"/v1/primes/{_VALID_ADDR}/risk-capital").json()

        assert body["prime_id"] == body["proxy_address"]
    finally:
        app.dependency_overrides.pop(prime_risk_capital._get_service, None)


def test_prime_risk_capital_marks_the_misnamed_prime_id_field_deprecated():
    schema = app.openapi()["components"]["schemas"]["PrimeRiskCapitalResponse"]["properties"]

    assert schema["prime_id"]["deprecated"] is True


def test_prime_risk_capital_does_not_deprecate_the_proxy_address_field():
    schema = app.openapi()["components"]["schemas"]["PrimeRiskCapitalResponse"]["properties"]

    assert "deprecated" not in schema["proxy_address"]
