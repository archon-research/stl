from decimal import Decimal
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from app.api.deps import PRIME_DENIED_DETAIL
from app.domain.entities.prime_risk_capital import AllocationRiskCapital, ChainRiskCapital, PrimeRiskCapital
from app.main import app
from app.services.prime_risk_capital_service import PrimeRiskCapitalService

_VALID_ADDR = "0x" + "ab" * 20
# A real axis-synome SubProxy: it resolves to its prime like any other of its
# identifiers, so the response is the prime's rather than a 404.
_SPARK_SUB_PROXY = "0x3300f198988e4c9c63f75df86de36421f06af8c4"


def _make_service(*, result: PrimeRiskCapital | None = None) -> AsyncMock:
    service = AsyncMock(spec=PrimeRiskCapitalService)
    service.compute.return_value = result
    return service


def _override_service(service: AsyncMock):
    async def _dep():
        yield service

    return _dep


def _result() -> PrimeRiskCapital:
    return PrimeRiskCapital(
        model="gap_sweep",
        exposure_usd=Decimal("1400"),
        total_risk_capital_usd=Decimal("100"),
        required_risk_capital_usd=Decimal("42"),
        encumbrance_ratio=Decimal("0.4200"),
        modeled_exposure_usd=Decimal("900"),
        modeled_pct=Decimal("0.6429"),
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
        prime_per_chain=(
            ChainRiskCapital(
                chain="mainnet",
                exposure_usd=Decimal("1400"),
                required_risk_capital_usd=Decimal("42"),
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
        prime_name="spark",
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


def test_get_prime_risk_capital_serializes_the_per_chain_figures_as_plain_strings():
    """The per-chain rows sum DB NUMERICs, so they carry the same exponent risk.

    A consumer parsing with BigInt reads an exponential form as 0, which is what
    made prime debt render as zero before PlainDecimal landed. Asserted here
    because the sibling test above covers only the top-level fields.
    """
    from app.api.v1 import prime_risk_capital

    big_usd = Decimal((0, (2, 1, 9, 9), 4))  # 2.199E+7
    assert "E+" in str(big_usd)

    result = PrimeRiskCapital(
        prime_name="spark",
        model="gap_sweep",
        exposure_usd=big_usd,
        total_risk_capital_usd=big_usd,
        required_risk_capital_usd=Decimal("0"),
        encumbrance_ratio=None,
        modeled_exposure_usd=Decimal("0"),
        modeled_pct=None,
        per_allocation=[],
        prime_per_chain=(
            ChainRiskCapital(
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
        body = TestClient(app).get(f"/v1/primes/{_VALID_ADDR}/risk-capital").json()

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
        assert body["exposure_usd"] == "1400"
        assert body["total_risk_capital_usd"] == "100"
        assert body["required_risk_capital_usd"] == "42"
        assert body["encumbrance_ratio"] == "0.4200"
        assert body["modeled_pct"] == "0.6429"
        assert len(body["per_allocation"]) == 2
        modeled = body["per_allocation"][0]
        assert modeled["applied"] is True
        assert modeled["required_risk_capital_usd"] == "30"
        assert modeled["unpriced_reason"] is None
        unmodeled = body["per_allocation"][1]
        assert unmodeled["applied"] is False
        assert unmodeled["required_risk_capital_usd"] is None
        assert unmodeled["unpriced_reason"] == "no_model"
    finally:
        app.dependency_overrides.pop(prime_risk_capital._get_service, None)


def test_get_prime_risk_capital_returns_404_when_prime_missing(prime_resolver):
    from app.api.v1 import prime_risk_capital

    service = _make_service()
    prime_resolver.identity = None
    app.dependency_overrides[prime_risk_capital._get_service] = _override_service(service)
    try:
        client = TestClient(app)

        response = client.get(f"/v1/primes/{_VALID_ADDR}/risk-capital")

        assert response.status_code == 404
        assert response.json()["detail"] == PRIME_DENIED_DETAIL
        service.compute.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(prime_risk_capital._get_service, None)


def test_a_subproxy_address_answers_the_prime_like_any_other_identifier():
    """It used to 404: the treasury wallet is not an ALM proxy, so answering for
    it folded the treasury into the aggregate. Now nothing is proxy-scoped and
    the treasury is read once, so a SubProxy is simply one of the prime's names.
    """
    from app.api.v1 import prime_risk_capital

    service = _make_service(result=_result())
    app.dependency_overrides[prime_risk_capital._get_service] = _override_service(service)
    try:
        by_subproxy = TestClient(app).get(f"/v1/primes/{_SPARK_SUB_PROXY}/risk-capital")
        by_alm = TestClient(app).get(f"/v1/primes/{_VALID_ADDR}/risk-capital")

        assert by_subproxy.status_code == 200
        assert by_subproxy.json() == by_alm.json()
    finally:
        app.dependency_overrides.pop(prime_risk_capital._get_service, None)


def test_get_prime_risk_capital_reports_share_missing_allocation_as_unpriced():
    """A backed allocation whose share lookup failed is surfaced as unpriced
    (200, applied=false + unpriced_reason), not a whole-response 503."""
    from app.api.v1 import prime_risk_capital

    result = PrimeRiskCapital(
        prime_name="spark",
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
        service.compute.assert_not_awaited()
    finally:
        app.dependency_overrides.pop(prime_risk_capital._get_service, None)


def test_get_prime_risk_capital_names_the_prime_the_figures_cover():
    from app.api.v1 import prime_risk_capital

    service = _make_service(result=_result())
    app.dependency_overrides[prime_risk_capital._get_service] = _override_service(service)
    try:
        body = TestClient(app).get(f"/v1/primes/{_VALID_ADDR}/risk-capital").json()

        assert body["prime_name"] == "spark"
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
                "chain": "mainnet",
                "exposure_usd": "1400",
                "required_risk_capital_usd": "42",
                "allocation_count": 2,
            }
        ]
    finally:
        app.dependency_overrides.clear()


def test_no_proxy_scoped_field_survives_on_the_response():
    """The success measure VEC-722 is written against: a caller cannot read a
    single proxy's figure out of a prime-scoped route by accident."""
    properties = app.openapi()["components"]["schemas"]["PrimeRiskCapitalResponse"]["properties"]

    assert {"prime_id", "proxy_address", "prime_proxies"}.isdisjoint(properties)


def test_no_response_field_is_deprecated():
    """`prime_id` and the scope-mixing `encumbrance_ratio` were the two, and both
    are gone rather than discouraged."""
    properties = app.openapi()["components"]["schemas"]["PrimeRiskCapitalResponse"]["properties"]

    assert [name for name, schema in properties.items() if schema.get("deprecated")] == []
