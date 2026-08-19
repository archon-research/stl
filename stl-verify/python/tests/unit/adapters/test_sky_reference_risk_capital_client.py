from decimal import Decimal

import httpx
import pytest

from app.adapters.sky.reference_risk_capital_client import SkyReferenceRiskCapitalClient
from app.domain.exceptions import ReferenceDataUnavailableError

_BASE = "https://monitor.test/star-monitoring/risk-capital"

_DETAIL = {
    "total_exposure": "2098090654.81",
    "total_rrc": "17837860.43",
    "total_rc": "48142491.08",
    "encumbrance_ratio": "0.3705",
    "total_exposure_share": "0.0084",
    "total_jrc": "48142491.08",
    "total_src": "0",
    "internal_jrc": "48142491.08",
    "external_jrc": "0",
    "tokenized_jrc": "0",
    "internal_src": "0",
    "external_src": "0",
    "epi_utilization": "0",
    "spj_utilization": "0",
}


def _allocation(symbol: str, exposure: str, rrc: str = "0", crr: str = "0", **overrides) -> dict:
    row = {
        "protocol": "sparklend",
        "network": "ethereum",
        "star": "spark",
        "token_address": "0x" + "ab" * 20,
        "symbol": symbol,
        "name": symbol,
        "loan_token_address": "0x" + "cd" * 20,
        "loan_token_symbol": "USDS",
        "exposure": exposure,
        "rrc": rrc,
        "crr": crr,
    }
    return row | overrides


def _wrap(data: dict) -> dict:
    return {"data": data, "status": 200, "success": True}


def _client(
    *,
    stars: list[str] | None = None,
    detail: dict | None = None,
    allocations: list[dict] | None = None,
    responses: dict[str, httpx.Response] | None = None,
) -> SkyReferenceRiskCapitalClient:
    """Build a client over a mock transport that routes on the request path."""
    listed = [{"star": star} for star in (stars if stars is not None else ["spark"])]
    default = {
        "/primes/": httpx.Response(200, json=_wrap({"results": listed})),
        "/primes/spark/": httpx.Response(200, json=_wrap(detail if detail is not None else _DETAIL)),
        "/primes/spark/allocations/": httpx.Response(
            200, json=_wrap({"results": allocations if allocations is not None else []})
        ),
    }
    routes = default | (responses or {})

    def handler(request: httpx.Request) -> httpx.Response:
        for suffix, response in routes.items():
            if request.url.path.endswith(suffix):
                return response
        raise AssertionError(f"unexpected request to {request.url}")

    return SkyReferenceRiskCapitalClient(_BASE, client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))


async def test_get_prime_rescales_upstream_crr_fraction_to_a_percentage():
    client = _client(allocations=[_allocation("spUSDT", "344187505.66", rrc="990048.94", crr="0.0028764051")])

    snapshot = await client.get_prime("spark")

    assert snapshot is not None
    assert snapshot.per_allocation[0].crr_pct == Decimal("0.28764051")


async def test_get_prime_parses_an_e_notation_crr():
    client = _client(allocations=[_allocation("spDAI", "296166747.05", crr="4.646E-15")])

    snapshot = await client.get_prime("spark")

    assert snapshot is not None
    assert snapshot.per_allocation[0].crr_pct == Decimal("4.646E-13")


async def test_get_prime_returns_none_for_a_star_the_monitor_does_not_list():
    # The detail route answers an unknown star with a 500 that is
    # indistinguishable from an outage, so the list route is the only safe gate.
    client = _client(stars=["grove"])

    assert await client.get_prime("spark") is None


async def test_get_prime_orders_the_breakdown_by_exposure_descending():
    client = _client(
        allocations=[
            _allocation("small", "100"),
            _allocation("large", "900"),
            _allocation("middle", "500"),
        ]
    )

    snapshot = await client.get_prime("spark")

    assert snapshot is not None
    assert [row.symbol for row in snapshot.per_allocation] == ["large", "middle", "small"]


async def test_get_prime_reads_the_junior_senior_split_the_self_model_cannot_produce():
    client = _client(detail=_DETAIL | {"total_jrc": "48142491.08", "total_src": "12.5"})

    snapshot = await client.get_prime("spark")

    assert snapshot is not None
    assert snapshot.junior_risk_capital_usd == Decimal("48142491.08")
    assert snapshot.senior_risk_capital_usd == Decimal("12.5")


@pytest.mark.parametrize(
    ("label", "response"),
    [
        ("server error", httpx.Response(500, text="boom")),
        ("invalid json", httpx.Response(200, text="not json")),
        ("upstream reported failure", httpx.Response(200, json={"data": {}, "success": False})),
        ("no data object", httpx.Response(200, json={"status": 200, "success": True})),
    ],
    ids=lambda value: value if isinstance(value, str) else "",
)
async def test_get_prime_raises_when_the_monitor_cannot_be_read(label, response):
    client = _client(responses={"/primes/spark/": response})

    with pytest.raises(ReferenceDataUnavailableError):
        await client.get_prime("spark")


async def test_get_prime_raises_when_a_required_total_is_absent():
    client = _client(detail={key: value for key, value in _DETAIL.items() if key != "total_rrc"})

    with pytest.raises(ReferenceDataUnavailableError, match="total_rrc"):
        await client.get_prime("spark")


async def test_get_prime_raises_when_a_figure_is_not_numeric():
    client = _client(detail=_DETAIL | {"total_rc": "n/a"})

    with pytest.raises(ReferenceDataUnavailableError, match="total_rc"):
        await client.get_prime("spark")


async def test_get_prime_raises_when_the_breakdown_has_no_results_array():
    client = _client(responses={"/primes/spark/allocations/": httpx.Response(200, json=_wrap({}))})

    with pytest.raises(ReferenceDataUnavailableError):
        await client.get_prime("spark")


async def test_get_prime_raises_when_the_monitor_is_unreachable():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("unreachable")

    client = SkyReferenceRiskCapitalClient(_BASE, client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))

    with pytest.raises(ReferenceDataUnavailableError):
        await client.get_prime("spark")
