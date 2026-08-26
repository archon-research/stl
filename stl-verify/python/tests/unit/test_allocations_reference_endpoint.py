"""The reference branch of ``/v1/primes/{id}/allocations``."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.api.deps import get_reference_positions_service_factory
from app.api.v1 import allocations
from app.domain.entities.allocation import AnchorageCustodyHolding, EthAddress
from app.domain.entities.reference_position import ReferencePosition, ReferencePositionSnapshot
from app.main import app
from app.services.allocation_service import AllocationService

_VALID_ADDR = "0x" + "ab" * 20
_TOKEN = "0x" + "cd" * 20
_V4_POOL_ID = "0x" + "ef" * 32
_OTHER_PROXY = "0x" + "99" * 20
_SYNCED_AT = datetime(2026, 8, 26, 9, 15, tzinfo=UTC)
_SYNCED_AT_ISO = "2026-08-26T09:15:00Z"


def _custody_holding() -> AnchorageCustodyHolding:
    return AnchorageCustodyHolding(
        symbol="BTC",
        custody_type="vault",
        balance=Decimal("1200"),
        amount_usd=Decimal("250000000"),
        collateral_usd=Decimal("310000000"),
        as_of=datetime(2026, 8, 21, tzinfo=UTC),
    )


def _reference_position(
    *,
    network: str = "ethereum",
    token_address: str = _TOKEN,
    receipt_token_id: int | None = 41,
    chain_id: int | None = 1,
    chain: str | None = "mainnet",
    underlying_token_id: int | None = None,
    underlying_token_address: str | None = None,
    underlying_symbol: str = "",
) -> ReferencePosition:
    return ReferencePosition(
        protocol_name="sparklend",
        network=network,
        symbol="spUSDT",
        name="Spark USDT",
        token_address=token_address,
        assets_usd=Decimal("344187505.66"),
        allocated_assets_usd=Decimal("344000000.00"),
        idle_assets_usd=Decimal("187505.66"),
        receipt_token_id=receipt_token_id,
        chain_id=chain_id,
        chain=chain,
        underlying_token_id=underlying_token_id,
        underlying_token_address=underlying_token_address,
        underlying_symbol=underlying_symbol,
    )


def _positions(*rows: ReferencePosition) -> ReferencePositionSnapshot:
    return ReferencePositionSnapshot(synced_at=_SYNCED_AT, positions=rows or (_reference_position(),))


@pytest.fixture
def reference_client(request):
    reference_service = AsyncMock()
    reference_service.get.return_value = request.param

    service = AsyncMock(spec=AllocationService)
    service.prime_exists.return_value = True
    service.list_anchorage_custody_holdings.return_value = []

    async def _service_dep():
        yield service

    app.dependency_overrides[allocations._get_service] = _service_dep
    app.dependency_overrides[get_reference_positions_service_factory] = lambda: lambda: reference_service
    try:
        yield TestClient(app), service
    finally:
        app.dependency_overrides.clear()


@pytest.mark.parametrize("reference_client", [_positions()], indirect=True)
def test_reference_mode_serves_upstream_positions_in_the_allocation_shape(reference_client):
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true").json()

    (row,) = body
    assert row["symbol"] == "spUSDT"
    assert row["protocol_name"] == "sparklend"
    assert row["underlying_symbol"] == ""
    assert row["amount_usd"] == "344187505.66"
    assert row["chain_id"] == 1


@pytest.mark.parametrize(
    "reference_client",
    [
        _positions(
            _reference_position(
                underlying_token_id=7,
                underlying_token_address="0x" + "77" * 20,
                underlying_symbol="USDT",
            )
        )
    ],
    indirect=True,
)
def test_reference_mode_serves_underlying_identity_when_the_position_resolves(reference_client):
    # Sky names no loan token of its own; this comes from the service resolving
    # the position against STL's receipt-token registry.
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true").json()

    assert body[0]["underlying_token_id"] == 7
    assert body[0]["underlying_token_address"] == "0x" + "77" * 20
    assert body[0]["underlying_symbol"] == "USDT"


@pytest.mark.parametrize("reference_client", [_positions()], indirect=True)
def test_reference_mode_leaves_underlying_null_when_the_position_is_unresolved(reference_client):
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true").json()

    assert body[0]["underlying_token_id"] is None
    assert body[0]["underlying_token_address"] is None
    assert body[0]["underlying_symbol"] == ""


@pytest.mark.parametrize("reference_client", [_positions()], indirect=True)
def test_reference_mode_stamps_each_row_with_the_cycle_it_was_observed_at(reference_client):
    # The rows are STL's record of the feed rather than a live read, so serving
    # them without a stamp would imply they are current.
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true").json()

    assert [row["reference_synced_at"] for row in body] == [_SYNCED_AT_ISO]


@pytest.mark.parametrize("reference_client", [_positions()], indirect=True)
def test_reference_mode_reports_no_balance_because_upstream_has_no_token_quantity(reference_client):
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true").json()

    assert body[0]["balance"] is None


@pytest.mark.parametrize("reference_client", [_positions()], indirect=True)
def test_reference_mode_stamps_reference_provenance_on_each_row(reference_client):
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true").json()

    assert body[0]["source"] == "reference"


@pytest.mark.parametrize("reference_client", [_positions()], indirect=True)
def test_reference_mode_marks_every_row_prime_scoped(reference_client):
    # Upstream reports per prime, so a client unioning a prime's proxies would
    # multiply the position count without this.
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true").json()

    assert body[0]["scope"] == "prime"


@pytest.mark.parametrize(
    "reference_client",
    [_positions(_reference_position(token_address=_V4_POOL_ID, receipt_token_id=None))],
    indirect=True,
)
def test_reference_mode_withholds_a_pool_id_from_the_address_field(reference_client):
    client, _ = reference_client

    body = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true").json()

    assert body[0]["receipt_token_address"] is None
    assert body[0]["receipt_token_id"] is None


@pytest.mark.parametrize("reference_client", [_positions()], indirect=True)
def test_reference_mode_never_reads_the_indexed_positions(reference_client):
    client, service = reference_client

    client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true")

    service.list_receipt_token_positions.assert_not_awaited()
    service.list_direct_asset_holdings.assert_not_awaited()


@pytest.mark.parametrize("reference_client", [None], indirect=True)
def test_reference_mode_returns_404_when_no_cycle_has_reported_on_the_prime(reference_client):
    client, _ = reference_client

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?reference=true")

    assert response.status_code == 404


@pytest.mark.parametrize("reference_client", [_positions()], indirect=True)
def test_reference_mode_propagates_a_read_failure_rather_than_reporting_no_data(reference_client):
    # A read that failed says nothing about coverage, so it must not be served
    # as "Sky reports nothing here", which reads identically to a real answer.
    client, _ = reference_client
    app.dependency_overrides[get_reference_positions_service_factory] = lambda: lambda: _failing_reader()

    with pytest.raises(ValueError, match="boom"):
        client.get(f"/v1/primes/{_VALID_ADDR}/allocations?source=reference")


@pytest.mark.parametrize("reference_client", [_positions()], indirect=True)
def test_both_propagates_a_read_failure_rather_than_degrading_to_indexed(reference_client):
    # The merged view swallows a 404 by design. A failure is not a 404, and
    # degrading on one would publish the indexed half as the whole answer.
    client, service = reference_client
    service.prime_proxy_addresses.return_value = [EthAddress(_VALID_ADDR)]
    service.list_receipt_token_positions.return_value = []
    service.list_direct_asset_holdings.return_value = []
    service.primary_proxy_address.return_value = None
    app.dependency_overrides[get_reference_positions_service_factory] = lambda: lambda: _failing_reader()

    with pytest.raises(ValueError, match="boom"):
        client.get(f"/v1/primes/{_VALID_ADDR}/allocations?source=both")


@pytest.mark.parametrize("reference_client", [_positions()], indirect=True)
def test_both_does_not_degrade_on_a_non_404_http_error(reference_client):
    # The guard that re-raises anything but a 404 exists for this; without a
    # test it is unreachable code that a refactor could widen unnoticed.
    client, service = reference_client
    service.prime_proxy_addresses.return_value = [EthAddress(_VALID_ADDR)]
    service.list_receipt_token_positions.return_value = []
    service.list_direct_asset_holdings.return_value = []
    service.primary_proxy_address.return_value = None
    reader = AsyncMock()
    reader.get.side_effect = HTTPException(status_code=503, detail="warming up")
    app.dependency_overrides[get_reference_positions_service_factory] = lambda: lambda: reader

    assert client.get(f"/v1/primes/{_VALID_ADDR}/allocations?source=both").status_code == 503


def _failing_reader():
    reader = AsyncMock()
    reader.get.side_effect = ValueError("Database query failed: boom")
    return reader


@pytest.mark.parametrize(
    "reference_client",
    [_positions(_reference_position(network="plume", chain_id=None, chain=None))],
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
        _positions(
            _reference_position(network="plume", chain_id=None, chain=None),
            _reference_position(network="ethereum", chain_id=1, chain="mainnet"),
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
    [_positions(_reference_position(network="ethereum", chain_id=1, chain="mainnet"))],
    indirect=True,
)
def test_source_reference_lists_the_monitor_positions(reference_client):
    client, _ = reference_client

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?source=reference")

    assert response.status_code == 200
    assert [row["scope"] for row in response.json()] == ["prime"]


@pytest.mark.parametrize(
    "reference_client",
    [_positions(_reference_position(network="ethereum", chain_id=1, chain="mainnet"))],
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
    [_positions(_reference_position(network="ethereum", chain_id=1, chain="mainnet"))],
    indirect=True,
)
def test_both_keeps_skys_value_beside_stls_on_a_matched_row(reference_client):
    """A merged row carries both provenances' figures, not just STL's.

    STL prices only the chains it indexes, so a position it holds on an unserved
    chain has a real balance and a null `amount_usd`. Dropping Sky's figure on
    the match left nothing for a total to fall back to — six of spark's rows,
    $423M, priced by Sky alone.
    """
    from app.domain.entities.allocation import ReceiptTokenPosition

    client, service = reference_client
    service.prime_proxy_addresses.return_value = [EthAddress(_VALID_ADDR)]
    service.list_direct_asset_holdings.return_value = []
    service.primary_proxy_address.return_value = None
    service.list_receipt_token_positions.return_value = [
        ReceiptTokenPosition(
            chain_id=1,
            receipt_token_id=41,
            receipt_token_address=_TOKEN,
            underlying_token_id=7,
            underlying_token_address="0x" + "77" * 20,
            symbol="spUSDT",
            underlying_symbol="USDT",
            protocol_name="sparklend",
            balance=Decimal("1"),
            amount_usd=None,
            latest_activity_at=None,
            latest_activity_action=None,
            latest_activity_amount=None,
        )
    ]

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?source=both")

    assert response.status_code == 200
    (row,) = response.json()
    assert row["source"] == "both"
    # STL priced none of it; Sky's figure is the only one there is.
    assert row["amount_usd"] is None
    assert row["reference_amount_usd"] == "344187505.66"
    assert row["reference_synced_at"] == _SYNCED_AT_ISO


@pytest.mark.parametrize(
    "reference_client",
    [_positions(_reference_position(network="ethereum", chain_id=1, chain="mainnet"))],
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


@pytest.mark.parametrize("reference_client", [None], indirect=True)
def test_both_serves_the_indexed_half_for_a_prime_with_no_reference_data(reference_client):
    # The indexed rows are still true, and every row carrying its own provenance
    # is what says Sky contributed nothing.
    client, service = reference_client
    service.prime_proxy_addresses.return_value = [EthAddress(_VALID_ADDR)]
    service.list_receipt_token_positions.return_value = []
    service.list_direct_asset_holdings.return_value = []
    service.primary_proxy_address.return_value = None

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations?source=both")

    assert response.status_code == 200
    assert response.json() == []
