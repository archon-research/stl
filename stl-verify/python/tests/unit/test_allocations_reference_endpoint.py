"""The ``reference=true`` branch of ``/v1/primes/{id}/allocations``."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from app.api.deps import get_reference_risk_capital_service_factory
from app.api.v1 import allocations
from app.domain.entities.allocation import AnchorageCustodyHolding, EthAddress
from app.domain.entities.reference_risk_capital import ReferenceAllocation, ReferencePrimeRiskCapital
from app.domain.exceptions import ReferenceDataUnavailableError
from app.main import app
from app.services.allocation_service import AllocationService

_VALID_ADDR = "0x" + "ab" * 20
_TOKEN = "0x" + "cd" * 20
_LOAN_TOKEN = "0x" + "12" * 20
_V4_POOL_ID = "0x" + "ef" * 32
_OTHER_PROXY = "0x" + "99" * 20


def _custody_holding() -> AnchorageCustodyHolding:
    return AnchorageCustodyHolding(
        symbol="BTC",
        custody_type="vault",
        balance=Decimal("1200"),
        amount_usd=Decimal("250000000"),
        collateral_usd=Decimal("310000000"),
        as_of=datetime(2026, 8, 21, tzinfo=UTC),
    )


def _reference_allocation(
    *,
    network: str = "ethereum",
    token_address: str = _TOKEN,
    receipt_token_id: int | None = 41,
    chain_id: int | None = 1,
    chain: str | None = "mainnet",
) -> ReferenceAllocation:
    return ReferenceAllocation(
        protocol_name="sparklend",
        network=network,
        symbol="spUSDT",
        name="Spark USDT",
        token_address=token_address,
        loan_token_address=_LOAN_TOKEN,
        loan_token_symbol="USDT",
        exposure_usd=Decimal("344187505.66"),
        required_risk_capital_usd=Decimal("990048.94"),
        crr_pct=Decimal("0.28764051"),
        receipt_token_id=receipt_token_id,
        chain_id=chain_id,
        chain=chain,
    )


def _snapshot(*rows: ReferenceAllocation) -> ReferencePrimeRiskCapital:
    zero = Decimal("0")
    return ReferencePrimeRiskCapital(
        star="spark",
        exposure_usd=zero,
        required_risk_capital_usd=zero,
        total_risk_capital_usd=zero,
        encumbrance_ratio=None,
        exposure_share=zero,
        junior_risk_capital_usd=zero,
        senior_risk_capital_usd=zero,
        internal_junior_risk_capital_usd=zero,
        external_junior_risk_capital_usd=zero,
        tokenized_junior_risk_capital_usd=zero,
        internal_senior_risk_capital_usd=zero,
        external_senior_risk_capital_usd=zero,
        epi_utilization=zero,
        spj_utilization=zero,
        per_allocation=rows or (_reference_allocation(),),
    )


@pytest.fixture
def reference_client(request):
    outcome = request.param
    reference_service = AsyncMock()
    if isinstance(outcome, Exception):
        reference_service.get.side_effect = outcome
    else:
        reference_service.get.return_value = outcome

    service = AsyncMock(spec=AllocationService)
    service.prime_exists.return_value = True
    service.list_anchorage_custody_holdings.return_value = []

    async def _service_dep():
        yield service

    app.dependency_overrides[allocations._get_service] = _service_dep
    app.dependency_overrides[get_reference_risk_capital_service_factory] = lambda: lambda: reference_service
    try:
        yield TestClient(app), service
    finally:
        app.dependency_overrides.clear()


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_reference_mode_serves_upstream_positions_in_the_allocation_shape(reference_client):
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true").json()

    (row,) = body
    assert row["symbol"] == "spUSDT"
    assert row["protocol_name"] == "sparklend"
    assert row["underlying_symbol"] == "USDT"
    assert row["amount_usd"] == "344187505.66"
    assert row["chain_id"] == 1


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_reference_mode_reports_no_balance_because_upstream_has_no_token_quantity(reference_client):
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true").json()

    assert body[0]["balance"] is None


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_reference_mode_marks_every_row_prime_scoped(reference_client):
    # Upstream reports per prime, so a client unioning a prime's proxies would
    # multiply the position count without this.
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true").json()

    assert body[0]["scope"] == "prime"


@pytest.mark.parametrize(
    "reference_client",
    [_snapshot(_reference_allocation(token_address=_V4_POOL_ID, receipt_token_id=None))],
    indirect=True,
)
def test_reference_mode_withholds_a_pool_id_from_the_address_field(reference_client):
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true").json()

    assert body[0]["receipt_token_address"] is None
    assert body[0]["receipt_token_id"] is None


@pytest.mark.parametrize("reference_client", [_snapshot()], indirect=True)
def test_reference_mode_never_reads_the_indexed_positions(reference_client):
    client, service = reference_client

    client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true")

    service.list_receipt_token_positions.assert_not_awaited()
    service.list_direct_asset_holdings.assert_not_awaited()


@pytest.mark.parametrize("reference_client", [None], indirect=True)
def test_reference_mode_returns_404_when_the_monitor_does_not_track_the_prime(reference_client):
    client, _ = reference_client

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true")

    assert response.status_code == 404


@pytest.mark.parametrize("reference_client", [ReferenceDataUnavailableError("boom")], indirect=True)
def test_reference_mode_returns_502_when_the_monitor_cannot_be_read(reference_client):
    client, _ = reference_client

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true")

    assert response.status_code == 502


@pytest.mark.parametrize(
    "reference_client",
    [_snapshot(_reference_allocation(network="plume", chain_id=None, chain=None))],
    indirect=True,
)
def test_reference_mode_serves_a_position_on_a_chain_it_has_no_id_for(reference_client):
    # Upstream adds chains before STL indexes them. Failing the list would cost
    # the prime every other position it holds -- grove loses 13 rows to 2.
    client, _ = reference_client

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true")

    assert response.status_code == 200
    [row] = response.json()
    assert row["chain_id"] is None
    assert row["network"] == "plume"


@pytest.mark.parametrize(
    "reference_client",
    [
        _snapshot(
            _reference_allocation(network="plume", chain_id=None, chain=None),
            _reference_allocation(network="ethereum", chain_id=1, chain="mainnet"),
        )
    ],
    indirect=True,
)
def test_reference_mode_keeps_the_mappable_rows_alongside_the_unmapped_one(reference_client):
    client, _ = reference_client

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true")

    assert response.status_code == 200
    assert [(row["chain_id"], row["network"]) for row in response.json()] == [
        (None, "plume"),
        (1, "ethereum"),
    ]


@pytest.mark.parametrize(
    "reference_client",
    [_snapshot(_reference_allocation(network="ethereum", chain_id=1, chain="mainnet"))],
    indirect=True,
)
def test_source_reference_lists_the_monitor_positions(reference_client):
    client, _ = reference_client

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?source=reference")

    assert response.status_code == 200
    assert [row["scope"] for row in response.json()] == ["prime"]


@pytest.mark.parametrize(
    "reference_client",
    [_snapshot(_reference_allocation(network="ethereum", chain_id=1, chain="mainnet"))],
    indirect=True,
)
def test_both_marks_a_position_only_sky_reports(reference_client):
    client, service = reference_client
    service.prime_proxy_addresses.return_value = [EthAddress(_VALID_ADDR)]
    service.list_receipt_token_positions.return_value = []
    service.list_direct_asset_holdings.return_value = []
    service.primary_proxy_address.return_value = None

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?source=both")

    assert response.status_code == 200
    assert [row["source"] for row in response.json()] == ["reference"]


@pytest.mark.parametrize(
    "reference_client",
    [_snapshot(_reference_allocation(network="ethereum", chain_id=1, chain="mainnet"))],
    indirect=True,
)
def test_both_serves_the_custody_leg_when_a_non_primary_proxy_is_queried(reference_client):
    # The merged view spans every proxy, so the prime-scoped leg belongs to it
    # whichever proxy was asked. Gating it on "is this the primary proxy" — right
    # for the proxy-scoped default — dropped it from the union entirely.
    client, service = reference_client
    service.prime_proxy_addresses.return_value = [EthAddress(_VALID_ADDR)]
    service.list_receipt_token_positions.return_value = []
    service.list_direct_asset_holdings.return_value = []
    service.primary_proxy_address.return_value = _OTHER_PROXY
    service.list_anchorage_custody_holdings.return_value = [_custody_holding()]

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?source=both")

    assert response.status_code == 200
    assert [row["symbol"] for row in response.json() if row["protocol_name"] == "anchorage"] == ["BTC"]


@pytest.mark.parametrize("reference_client", [ReferenceDataUnavailableError("boom")], indirect=True)
def test_both_serves_the_indexed_half_when_sky_cannot_be_read(reference_client):
    # Never a 502 for the merged view: the indexed rows are still true, and every
    # row carrying its own provenance is what says Sky contributed nothing.
    client, service = reference_client
    service.prime_proxy_addresses.return_value = [EthAddress(_VALID_ADDR)]
    service.list_receipt_token_positions.return_value = []
    service.list_direct_asset_holdings.return_value = []
    service.primary_proxy_address.return_value = None

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?source=both")

    assert response.status_code == 200
    assert response.json() == []
