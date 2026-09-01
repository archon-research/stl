"""Integration tests for the risk API warm-up and pass-through behavior.

Warm-up: a ``receipt_token`` row exists before the prime-allocation-indexer
has created the matching ``token`` row for the receipt token's own address.
The API should treat that as "data not indexed yet" (HTTP 503), not "unknown
receipt token" (HTTP 404).

Pass-through: a directly-held allocated asset that is not a registered
receipt token still gets a breakdown — a single self-backed item built from
``allocation_position_current`` — instead of a 404.
"""

import asyncio
from decimal import Decimal
from pathlib import Path

import asyncpg
import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.config import Settings
from app.main import create_app
from tests.integration.seed import (
    PT_POSITIONLESS_TOKEN_HEX,
    PT_PROXY_A_HEX,
    PT_SELF_TOKEN_HEX,
    PT_UNPRICED_TOKEN_HEX,
    PT_WRAPPER_TOKEN_HEX,
    seed_pass_through_positions,
)

_RECEIPT_TOKEN_ADDRESS_HEX = "59cd1c87501baa753d0b5b5ab5d8416a45cd71dc"
_PRIME_ID = "0x" + "ab" * 20


async def _seed(db_url: str) -> int:
    """Insert a SparkLend receipt_token row without creating its token row."""
    conn = await asyncpg.connect(db_url)
    try:
        protocol_id = await conn.fetchval("SELECT id FROM protocol WHERE name = 'SparkLend' AND chain_id = 1")
        if protocol_id is None:
            raise RuntimeError("no SparkLend protocol seed found for chain_id=1")
        underlying_token_id = await conn.fetchval("SELECT id FROM token WHERE symbol = 'WETH' AND chain_id = 1")
        if underlying_token_id is None:
            raise RuntimeError("no WETH token seed found for chain_id=1")
        receipt_token_id = await conn.fetchval(
            """
            INSERT INTO receipt_token
                (protocol_id, underlying_token_id, receipt_token_address, symbol,
                 created_at_block, chain_id)
            VALUES ($1, $2, $3, 'spWETH-warmup', 16776402, 1)
            ON CONFLICT ON CONSTRAINT receipt_token_chain_address_unique
                DO UPDATE SET symbol = EXCLUDED.symbol
            RETURNING id
            """,
            protocol_id,
            underlying_token_id,
            bytes.fromhex(_RECEIPT_TOKEN_ADDRESS_HEX),
        )
        return int(receipt_token_id)
    finally:
        await conn.close()


@pytest.fixture(scope="module")
def seeded_receipt_token_id(db_url: str) -> int:
    """A receipt_token that exists before its own token row has been indexed."""
    return asyncio.run(_seed(db_url))


@pytest.fixture()
def client(async_db_url: str, tmp_path: Path):
    """Return a TestClient wired to the module's isolated database."""
    empty_mapping = tmp_path / "empty_mapping.json"
    empty_mapping.write_text("{}")
    test_app = create_app(
        Settings.model_validate(
            {
                "database_url": SecretStr(async_db_url),
                "suraf_mappings_file": empty_mapping,
                "core_model_mappings_file": empty_mapping,
            }
        )
    )
    with TestClient(test_app) as c:
        yield c


def test_risk_breakdown_returns_503_when_receipt_token_token_row_is_missing(
    client: TestClient,
    seeded_receipt_token_id: int,
) -> None:
    response = client.get(f"/v1/risk/{seeded_receipt_token_id}/breakdown")

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "share_data_missing"


def test_risk_bad_debt_returns_503_when_receipt_token_token_row_is_missing(
    client: TestClient,
    seeded_receipt_token_id: int,
) -> None:
    response = client.get(f"/v1/risk/{seeded_receipt_token_id}/bad-debt?gap_pct=0.1")

    assert response.status_code == 503
    body = response.json()
    assert body["detail"]["code"] == "share_data_missing"


def test_risk_breakdown_by_address_resolves_chain_and_address(
    client: TestClient,
    seeded_receipt_token_id: int,
) -> None:
    """The address-based breakdown route reaches the same service path as the legacy form."""
    response = client.get(f"/v1/risk/1/0x{_RECEIPT_TOKEN_ADDRESS_HEX}/breakdown")

    # Same warm-up state as the legacy test — receipt_token row exists but
    # its token row does not, so share lookup signals "data not indexed".
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "share_data_missing"


def test_risk_breakdown_by_address_returns_404_for_unknown_address(
    client: TestClient,
) -> None:
    response = client.get("/v1/risk/1/0x" + "ff" * 20 + "/breakdown")

    assert response.status_code == 404


def test_risk_bad_debt_by_address_resolves_chain_and_address(
    client: TestClient,
    seeded_receipt_token_id: int,
) -> None:
    """Bad-debt address route reaches the same service path as the legacy form."""
    response = client.get(f"/v1/risk/1/0x{_RECEIPT_TOKEN_ADDRESS_HEX}/bad-debt?gap_pct=0.1")

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "share_data_missing"


def test_risk_bad_debt_by_address_returns_404_for_unknown_address(
    client: TestClient,
) -> None:
    response = client.get("/v1/risk/1/0x" + "ff" * 20 + "/bad-debt?gap_pct=0.1")

    assert response.status_code == 404


def test_risk_by_address_accepts_mixed_case_address(
    client: TestClient,
    seeded_receipt_token_id: int,
) -> None:
    """Address path is matched case-insensitively (compared as bytes after hex decode)."""
    mixed = "0x59cD1C87501baa753d0B5B5Ab5D8416A45cD71DC"
    response = client.get(f"/v1/risk/1/{mixed}/breakdown")

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "share_data_missing"


def test_risk_bad_debt_by_address_returns_422_for_malformed_address(
    client: TestClient,
) -> None:
    response = client.get("/v1/risk/1/0xbadaddr/bad-debt?gap_pct=0.1")

    assert response.status_code == 422


def test_risk_bad_debt_by_address_returns_422_for_out_of_range_gap_pct(
    client: TestClient,
    seeded_receipt_token_id: int,
) -> None:
    response = client.get(f"/v1/risk/1/0x{_RECEIPT_TOKEN_ADDRESS_HEX}/bad-debt?gap_pct=1.5")

    assert response.status_code == 422


# ---------------------------------------------------------------------------
# /v1/risk/rrc{,/scenario} — asset-identity validation + 404 unknown address
# ---------------------------------------------------------------------------


def test_get_rrc_returns_422_when_both_identities_supplied(client: TestClient) -> None:
    response = client.get(
        "/v1/risk/rrc",
        params={
            "asset_id": 1,
            "chain_id": 1,
            "token_address": f"0x{_RECEIPT_TOKEN_ADDRESS_HEX}",
            "prime_id": _PRIME_ID,
        },
    )

    assert response.status_code == 422
    assert "got both" in response.json()["detail"]


def test_get_rrc_returns_422_when_neither_identity_supplied(client: TestClient) -> None:
    response = client.get("/v1/risk/rrc", params={"prime_id": _PRIME_ID})

    assert response.status_code == 422
    assert "neither" in response.json()["detail"]


def test_get_rrc_returns_422_when_only_chain_id_supplied(client: TestClient) -> None:
    response = client.get(
        "/v1/risk/rrc",
        params={"chain_id": 1, "prime_id": _PRIME_ID},
    )

    assert response.status_code == 422


def test_get_rrc_returns_404_when_address_unknown(client: TestClient) -> None:
    response = client.get(
        "/v1/risk/rrc",
        params={
            "chain_id": 1,
            "token_address": "0x" + "ff" * 20,
            "prime_id": _PRIME_ID,
        },
    )

    assert response.status_code == 404


def test_post_scenario_returns_422_when_both_identities_supplied(client: TestClient) -> None:
    response = client.post(
        "/v1/risk/rrc/scenario",
        json={
            "asset_id": 1,
            "chain_id": 1,
            "token_address": f"0x{_RECEIPT_TOKEN_ADDRESS_HEX}",
            "prime_id": _PRIME_ID,
        },
    )

    assert response.status_code == 422


def test_post_scenario_returns_422_when_neither_identity_supplied(client: TestClient) -> None:
    response = client.post(
        "/v1/risk/rrc/scenario",
        json={"prime_id": _PRIME_ID},
    )

    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Pass-through breakdown fallback for directly-held allocated assets
# ---------------------------------------------------------------------------

# syrupUSDG: migration-seeded receipt token with no indexed pool, so the
# receipt path (not the fallback) must keep serving it.
_SYRUP_USDG_HEX = "87b65c4aaffa76881f9e96f3e7ed945ddfc3cd7a"


@pytest.fixture(scope="module")
def pass_through_seed(db_url: str) -> None:
    asyncio.run(seed_pass_through_positions(db_url))


def test_breakdown_falls_back_to_pass_through_for_directly_held_asset(
    client: TestClient, pass_through_seed: None
) -> None:
    response = client.get(f"/v1/risk/1/0x{PT_SELF_TOKEN_HEX}/breakdown")

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["receipt_token_id"] is None
    assert len(body["items"]) == 1
    item = body["items"][0]
    assert item["symbol"] == "PTSELF"
    assert Decimal(item["amount"]) == Decimal("157.5")
    assert Decimal(item["backing_pct"]) == Decimal("100")
    assert Decimal(item["price_usd"]) == Decimal("1.0002")
    assert Decimal(item["amount_usd"]) == Decimal("157.5") * Decimal("1.0002")
    assert item["liquidation_threshold"] is None
    assert item["liquidation_bonus"] is None


def test_pass_through_breakdown_reports_the_underlying_when_it_differs(
    client: TestClient, pass_through_seed: None
) -> None:
    response = client.get(f"/v1/risk/1/0x{PT_WRAPPER_TOKEN_HEX}/breakdown")

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["receipt_token_id"] is None
    (item,) = body["items"]
    assert item["symbol"] == "PTUSDC"
    assert Decimal(item["amount"]) == Decimal("20320203.5")
    assert Decimal(item["price_usd"]) == Decimal("0.9999")


def test_pass_through_breakdown_serves_unpriced_asset_with_null_price(
    client: TestClient, pass_through_seed: None
) -> None:
    response = client.get(f"/v1/risk/1/0x{PT_UNPRICED_TOKEN_HEX}/breakdown")

    assert response.status_code == 200, response.text
    (item,) = response.json()["items"]
    assert Decimal(item["amount"]) == Decimal("42")
    assert item["price_usd"] is None
    assert Decimal(item["amount_usd"]) == Decimal("0")


def test_pass_through_breakdown_narrows_to_the_primes_proxies(client: TestClient, pass_through_seed: None) -> None:
    # PT_PROXY_A resolves to pt_prime_a, whose proxies hold 100.5 + 50; the
    # other prime's 7 must drop out.
    response = client.get(f"/v1/risk/1/0x{PT_SELF_TOKEN_HEX}/breakdown?prime_id=0x{PT_PROXY_A_HEX}")

    assert response.status_code == 200, response.text
    (item,) = response.json()["items"]
    assert Decimal(item["amount"]) == Decimal("150.5")


def test_pass_through_breakdown_returns_404_for_unknown_prime(client: TestClient, pass_through_seed: None) -> None:
    # An unknown prime narrows to itself (list_prime_proxy_addresses returns
    # [prime_address]), which holds no positions.
    response = client.get(f"/v1/risk/1/0x{PT_SELF_TOKEN_HEX}/breakdown?prime_id=0x" + "cd" * 20)

    assert response.status_code == 404


def test_pass_through_breakdown_returns_404_without_allocation_position(
    client: TestClient, pass_through_seed: None
) -> None:
    response = client.get(f"/v1/risk/1/0x{PT_POSITIONLESS_TOKEN_HEX}/breakdown")

    assert response.status_code == 404


def test_breakdown_keeps_receipt_path_with_integer_receipt_token_id(
    client: TestClient, pass_through_seed: None
) -> None:
    response = client.get(f"/v1/risk/1/0x{_SYRUP_USDG_HEX}/breakdown")

    assert response.status_code == 200, response.text
    assert isinstance(response.json()["receipt_token_id"], int)
