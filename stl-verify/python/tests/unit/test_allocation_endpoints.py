from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.domain.entities.allocation import ChainMetadata, EthAddress, Prime, ProtocolMetadata
from app.domain.entities.allocation_activity import AllocationActivityEvent
from app.domain.entities.time_series_bucket import AllocationActivityBucket
from app.main import app
from app.services.allocation_service import AllocationService
from tests.conftest import make_direct_asset_holding, make_receipt_token_position

_VALID_ADDR = "0x" + "ab" * 20


@pytest.fixture(autouse=True)
def _clear_dependency_overrides():
    yield
    app.dependency_overrides.clear()


def _make_service(primes=None, positions=None, direct_holdings=None, *, exists: bool = True) -> AsyncMock:
    service = AsyncMock(spec=AllocationService)
    service.list_primes.return_value = primes or []
    service.list_receipt_token_positions.return_value = positions or []
    service.list_direct_asset_holdings.return_value = direct_holdings or []
    service.prime_exists.return_value = exists
    service.list_activity_buckets.return_value = []
    return service


def _override_service(service: AsyncMock):
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


def test_list_allocations_returns_200_with_enriched_holdings():
    from app.api.v1 import allocations

    position = make_receipt_token_position()
    service = _make_service(positions=[position])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    data = response.json()
    assert data == [
        {
            "chain_id": 1,
            "receipt_token_id": 1,
            "receipt_token_address": "0x" + "a" * 40,
            "underlying_token_id": 10,
            "underlying_token_address": "0x" + "b" * 40,
            "symbol": "aUSDC",
            "underlying_symbol": "USDC",
            "protocol_name": "aave_v3",
            "balance": "100.0",
            "amount_usd": None,
            "latest_activity_at": None,
            "category": "allocation",
        }
    ]
    service.list_receipt_token_positions.assert_awaited_once_with(EthAddress(_VALID_ADDR))


def test_list_allocations_returns_direct_asset_rows_with_null_receipt_fields():
    """Direct holdings (e.g. raw PYUSD in a proxy) surface as their own rows.
    receipt_token_id / receipt_token_address / protocol_name / amount_usd
    are null; symbol and underlying_symbol both name the held asset; category
    defaults to ASSET.
    """
    from app.api.v1 import allocations

    holding = make_direct_asset_holding()
    service = _make_service(direct_holdings=[holding])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    assert response.json() == [
        {
            "chain_id": 1,
            "receipt_token_id": None,
            "receipt_token_address": None,
            "underlying_token_id": 99,
            "underlying_token_address": "0x" + "c" * 40,
            "symbol": "PYUSD",
            "underlying_symbol": "PYUSD",
            "protocol_name": None,
            "balance": "250.0",
            "amount_usd": None,
            "latest_activity_at": None,
            "category": "asset",
        }
    ]
    service.list_direct_asset_holdings.assert_awaited_once_with(EthAddress(_VALID_ADDR))


def test_list_allocations_combines_receipt_and_direct_rows():
    from app.api.v1 import allocations

    service = _make_service(
        positions=[make_receipt_token_position()],
        direct_holdings=[make_direct_asset_holding()],
    )
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    rows = response.json()
    assert len(rows) == 2
    by_symbol = {row["symbol"]: row for row in rows}
    assert by_symbol["aUSDC"]["receipt_token_id"] == 1
    assert by_symbol["PYUSD"]["receipt_token_id"] is None


def test_list_allocations_returns_empty_when_prime_exists_with_no_holdings():
    """A registered prime that has fully exited all positions returns 200+[],
    not 404 — only unknown primes (no history at all) trigger 404.
    """
    from app.api.v1 import allocations

    service = _make_service(positions=[], exists=True)
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 200
    assert response.json() == []
    service.prime_exists.assert_awaited_once_with(EthAddress(_VALID_ADDR))


def test_list_allocations_returns_404_when_prime_missing():
    from app.api.v1 import allocations

    service = _make_service(exists=False)
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(f"/v1/primes/{_VALID_ADDR}/allocations")

    assert response.status_code == 404
    assert response.json()["detail"] == "Prime not found"
    service.prime_exists.assert_awaited_once_with(EthAddress(_VALID_ADDR))
    service.list_receipt_token_positions.assert_not_awaited()
    service.list_direct_asset_holdings.assert_not_awaited()


def test_list_allocations_returns_422_for_invalid_prime_id():
    from app.api.v1 import allocations

    service = _make_service(positions=[])
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/primes/0xdeadbeef/allocations")

    assert response.status_code == 422
    service.prime_exists.assert_not_awaited()
    service.list_receipt_token_positions.assert_not_awaited()


def test_list_chains_returns_200_with_chain_rows():
    from app.api.v1 import allocations

    service = _make_service()
    service.list_chains.return_value = [
        ChainMetadata(chain_id=1, name="Ethereum"),
        ChainMetadata(chain_id=10, name="Optimism"),
    ]
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/chains")

    assert response.status_code == 200
    assert response.json() == [
        {"chain_id": 1, "name": "Ethereum"},
        {"chain_id": 10, "name": "Optimism"},
    ]
    service.list_chains.assert_awaited_once()


def test_list_protocols_returns_200_with_protocol_rows():
    from app.api.v1 import allocations

    service = _make_service()
    service.list_protocols.return_value = [
        ProtocolMetadata(id=1, chain_id=1, encode="aave_v3", name="Aave V3"),
        ProtocolMetadata(id=2, chain_id=1, encode="spark", name="SparkLend"),
    ]
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/protocols")

    assert response.status_code == 200
    assert response.json() == [
        {"id": 1, "chain_id": 1, "encode": "aave_v3", "name": "Aave V3"},
        {"id": 2, "chain_id": 1, "encode": "spark", "name": "SparkLend"},
    ]
    service.list_protocols.assert_awaited_once()


def test_list_allocation_activity_returns_rows_and_forwards_filters():
    from app.api.v1 import allocations

    from_ts = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    to_ts = datetime(2026, 1, 2, 0, 0, tzinfo=UTC)

    service = _make_service()
    service.list_allocation_activity.return_value = [
        AllocationActivityEvent(
            chain_id=1,
            prime_address=_VALID_ADDR,
            prime_name="spark",
            protocol_name="Aave V3",
            token_id=1,
            token_symbol="USDC",
            action_type="in",
            tx_amount=Decimal("100.0"),
            balance=Decimal("200.0"),
            tx_hash="0x" + "ab" * 32,
            log_index=1,
            block_number=100,
            block_version=0,
            created_at=from_ts,
        )
    ]
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(
        "/v1/allocations/activity",
        params={
            "prime_id": _VALID_ADDR,
            "chain_id": 1,
            "protocol_name": "aave",
            "action_type": "in",
            "token_symbol": "usdc",
            "tx_hash": "0x" + "ab" * 32,
            "from_timestamp": from_ts.isoformat().replace("+00:00", "Z"),
            "to_timestamp": to_ts.isoformat().replace("+00:00", "Z"),
            "limit": 50,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "raw"
    assert len(payload["data"]) == 1
    assert payload["data"][0]["token_symbol"] == "USDC"

    kwargs = service.list_allocation_activity.await_args.kwargs
    assert kwargs["prime_id"] == EthAddress(_VALID_ADDR)
    assert kwargs["chain_id"] == 1
    assert kwargs["protocol_name"] == "aave"
    assert kwargs["action_type"] == "in"
    assert kwargs["token_symbol"] == "usdc"
    assert kwargs["tx_hash"] == "0x" + "ab" * 32
    assert kwargs["from_timestamp"] == from_ts
    assert kwargs["to_timestamp"] == to_ts
    assert kwargs["limit"] == 50


def test_list_allocation_activity_returns_aggregated_buckets():
    from app.api.v1 import allocations

    service = _make_service()
    service.list_activity_buckets.return_value = [
        AllocationActivityBucket(
            bucket_start=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
            event_count=3,
            total_tx_amount=Decimal("450.5"),
        )
    ]
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(
        "/v1/allocations/activity",
        params={
            "from_timestamp": "2026-01-01T00:00:00Z",
            "to_timestamp": "2026-01-02T00:00:00Z",
            "aggregate": "true",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "aggregated"
    assert payload["data"] == [{"bucket_start": "2026-01-01T12:00:00Z", "event_count": 3, "total_tx_amount": "450.5"}]
    kwargs = service.list_activity_buckets.await_args.kwargs
    assert kwargs["bucket_seconds"] == 5 * 60  # 24h window -> PT5M default
    service.list_allocation_activity.assert_not_awaited()


def test_list_allocation_activity_returns_422_for_invalid_prime_id():
    from app.api.v1 import allocations

    service = _make_service()
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get(
        "/v1/allocations/activity",
        params={"prime_id": "0xdeadbeef"},
    )

    assert response.status_code == 422
    service.list_allocation_activity.assert_not_awaited()


def test_list_allocation_activity_hides_synthetic_sweep_tx_hash():
    from app.api.v1 import allocations

    created_at = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    service = _make_service()
    service.list_allocation_activity.return_value = [
        AllocationActivityEvent(
            chain_id=1,
            prime_address=_VALID_ADDR,
            prime_name="spark",
            protocol_name="SparkLend",
            token_id=1,
            token_symbol="spUSDC",
            action_type="sweep",
            tx_amount=Decimal("0"),
            balance=Decimal("200.0"),
            tx_hash="0x" + "cd" * 32,
            log_index=0,
            block_number=100,
            block_version=0,
            created_at=created_at,
        )
    ]
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    response = client.get("/v1/allocations/activity", params={"action_type": "sweep"})

    assert response.status_code == 200
    assert response.json()["data"][0]["tx_hash"] is None


def test_list_allocation_activity_returns_200_empty_for_unknown_valid_prime_id():
    """Valid-format prime_id with no rows is a filter miss, not a missing resource → 200 []."""
    from app.api.v1 import allocations

    service = _make_service()
    service.list_allocation_activity.return_value = []
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    unknown_addr = "0x" + "ee" * 20
    response = client.get(
        "/v1/allocations/activity",
        params={"prime_id": unknown_addr},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["mode"] == "raw"
    assert body["data"] == []
    service.list_allocation_activity.assert_awaited_once()
    assert service.list_allocation_activity.await_args.kwargs["prime_id"] == EthAddress(unknown_addr)


def test_list_allocation_activity_returns_422_for_limit_out_of_range():
    from app.api.v1 import allocations

    service = _make_service()
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app)

    too_small = client.get("/v1/allocations/activity", params={"limit": 0})
    too_large = client.get("/v1/allocations/activity", params={"limit": 1001})

    assert too_small.status_code == 422
    assert too_large.status_code == 422
    service.list_allocation_activity.assert_not_awaited()


def test_list_allocation_activity_returns_500_when_service_raises_value_error():
    from app.api.v1 import allocations

    service = _make_service()
    service.list_allocation_activity.side_effect = ValueError("query failed")
    app.dependency_overrides[allocations._get_service] = _override_service(service)
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/v1/allocations/activity")

    assert response.status_code == 500
    assert response.json() == {"detail": "Failed to retrieve allocation activity"}


# --- capital-metrics endpoint ---


def test_list_capital_metrics_maps_star_risk_capital_data(monkeypatch):
    """Metrics are sourced from Star risk capital upstream, matched by prime name."""
    from app.api.v1 import allocations

    grove_addr = _VALID_ADDR
    spark_addr = "0x" + "cd" * 20

    service = _make_service(
        primes=[
            Prime(id=grove_addr, name="grove", address=grove_addr),
            Prime(id=spark_addr, name="spark", address=spark_addr),
        ]
    )
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    async def _fake_payload():
        return allocations.StarRiskCapitalResponse.model_validate(
            {
                "status": 200,
                "success": True,
                "data": {
                    "results": [
                        {
                            "star": "grove",
                            "exposure": "500.00",
                            "total_rc": "100.00",
                            "financial_rrc": "40.00",
                            "exposure_share": "50.00%",
                            "risk_tolerance_ratio": "5.00",
                        },
                        {
                            "star": "spark",
                            "exposure": "200.00",
                            "total_rc": "80.00",
                            "financial_rrc": "30.00",
                            "exposure_share": "20.00%",
                            "risk_tolerance_ratio": "2.50",
                        },
                    ]
                },
            }
        )

    monkeypatch.setattr(allocations, "_fetch_star_risk_capital_payload", _fake_payload)
    client = TestClient(app)

    response = client.get("/v1/capital-metrics")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2

    grove = next(m for m in data if m["prime_id"] == grove_addr)
    assert grove["risk_capital"] == "500.00"
    assert grove["total_capital"] == "100.00"
    assert grove["first_loss_capital"] == "40.00"
    assert grove["capital_buffer"] == "60.00"
    assert grove["risk_to_capital_ratio"] == "5.00"

    spark = next(m for m in data if m["prime_id"] == spark_addr)
    assert spark["risk_capital"] == "200.00"
    assert spark["risk_to_capital_ratio"] == "2.50"


def test_list_capital_metrics_returns_defaults_for_primes_with_no_star_row(monkeypatch):
    """Primes not present in Star risk capital data are returned with default metric values."""
    from app.api.v1 import allocations

    grove_addr = _VALID_ADDR
    service = _make_service(
        primes=[
            Prime(id=grove_addr, name="grove", address=grove_addr),
            Prime(id="0x" + "ee" * 20, name="unknown-prime", address="0x" + "ee" * 20),
        ]
    )
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    async def _fake_payload():
        return allocations.StarRiskCapitalResponse.model_validate(
            {
                "status": 200,
                "success": True,
                "data": {
                    "results": [
                        {
                            "star": "grove",
                            "exposure": "100.00",
                            "total_rc": "50.00",
                            "financial_rrc": "20.00",
                            "exposure_share": "10.00%",
                            "risk_tolerance_ratio": "2.00",
                        }
                    ]
                },
            }
        )

    monkeypatch.setattr(allocations, "_fetch_star_risk_capital_payload", _fake_payload)
    client = TestClient(app)

    response = client.get("/v1/capital-metrics")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 2
    grove = next(item for item in data if item["prime_name"] == "grove")
    assert grove["risk_capital"] == "100.00"

    missing = next(item for item in data if item["prime_name"] == "unknown-prime")
    assert missing["risk_capital"] == "0"
    assert missing["capital_buffer"] == "0"
    assert missing["first_loss_capital"] == "0"
    assert missing["total_capital"] == "0"
    assert missing["risk_to_capital_ratio"] is None
    assert missing["validation_note"] == "No upstream Star risk-capital row matched this prime."


def test_list_capital_metrics_returns_502_for_invalid_numeric_payload(monkeypatch):
    from app.api.v1 import allocations

    grove_addr = _VALID_ADDR
    service = _make_service(primes=[Prime(id=grove_addr, name="grove", address=grove_addr)])
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    async def _fake_payload():
        return allocations.StarRiskCapitalResponse.model_validate(
            {
                "status": 200,
                "success": True,
                "data": {
                    "results": [
                        {
                            "star": "grove",
                            "exposure": "not-a-number",
                            "total_rc": "50.00",
                            "financial_rrc": "20.00",
                            "exposure_share": "10.00%",
                            "risk_tolerance_ratio": "2.00",
                        }
                    ]
                },
            }
        )

    monkeypatch.setattr(allocations, "_fetch_star_risk_capital_payload", _fake_payload)
    client = TestClient(app)

    response = client.get("/v1/capital-metrics")

    assert response.status_code == 502
    assert "invalid numeric value" in response.json()["detail"]


def test_list_capital_metrics_returns_502_when_upstream_fetch_fails(monkeypatch):
    from app.api.v1 import allocations

    service = _make_service(primes=[Prime(id=_VALID_ADDR, name="grove", address=_VALID_ADDR)])
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    async def _raise_fetch_error():
        raise HTTPException(status_code=502, detail="Risk capital upstream request failed")

    monkeypatch.setattr(allocations, "_fetch_star_risk_capital_payload", _raise_fetch_error)
    client = TestClient(app)

    response = client.get("/v1/capital-metrics")

    assert response.status_code == 502
    assert response.json() == {"detail": "Risk capital upstream request failed"}


def test_list_capital_metrics_returns_empty_when_payload_has_no_data(monkeypatch):
    from app.api.v1 import allocations

    service = _make_service(primes=[Prime(id=_VALID_ADDR, name="grove", address=_VALID_ADDR)])
    app.dependency_overrides[allocations._get_service] = _override_service(service)

    async def _fake_payload_without_data():
        return allocations.StarRiskCapitalResponse.model_validate(
            {
                "status": 200,
                "success": True,
                "data": None,
            }
        )

    monkeypatch.setattr(allocations, "_fetch_star_risk_capital_payload", _fake_payload_without_data)
    client = TestClient(app)

    response = client.get("/v1/capital-metrics")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["prime_id"] == _VALID_ADDR
    assert payload[0]["risk_capital"] == "0"
    assert payload[0]["is_validated"] is False


# --- data-sources endpoint ---


def test_get_data_sources_returns_200_with_sources():
    client = TestClient(app)

    response = client.get("/v1/data-sources")

    assert response.status_code == 200
    data = response.json()
    assert "sources" in data
    assert isinstance(data["sources"], list)
    assert len(data["sources"]) > 0
