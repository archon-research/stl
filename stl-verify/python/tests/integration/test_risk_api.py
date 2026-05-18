"""Integration tests for the risk API warm-up behavior.

These tests cover the case where a ``receipt_token`` row exists before the
prime-allocation-indexer has created the matching ``token`` row for the
receipt token's own address. The API should treat that as "data not indexed
yet" (HTTP 503), not "unknown receipt token" (HTTP 404).
"""

import asyncio
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import Settings
from app.main import create_app

_RECEIPT_TOKEN_ADDRESS_HEX = "59cd1c87501baa753d0b5b5ab5d8416a45cd71dc"
_PRIME_ID = "0x" + "ab" * 20


async def _seed(async_url: str) -> int:
    """Insert a SparkLend receipt_token row without creating its token row."""
    engine = create_async_engine(async_url)
    try:
        async with engine.begin() as conn:
            protocol_id = (
                await conn.execute(text("SELECT id FROM protocol WHERE name = 'SparkLend' AND chain_id = 1"))
            ).scalar_one()
            underlying_token_id = (
                await conn.execute(text("SELECT id FROM token WHERE symbol = 'WETH' AND chain_id = 1"))
            ).scalar_one()

            receipt_token_id = (
                await conn.execute(
                    text(
                        """
                        INSERT INTO receipt_token
                            (protocol_id, underlying_token_id, receipt_token_address, symbol,
                             created_at_block, chain_id)
                        VALUES (:protocol_id, :underlying_token_id, decode(:addr, 'hex'), 'spWETH-warmup', 16776402, 1)
                        ON CONFLICT ON CONSTRAINT receipt_token_chain_address_unique
                            DO UPDATE SET symbol = EXCLUDED.symbol
                        RETURNING id
                        """
                    ),
                    {
                        "protocol_id": protocol_id,
                        "underlying_token_id": underlying_token_id,
                        "addr": _RECEIPT_TOKEN_ADDRESS_HEX,
                    },
                )
            ).scalar_one()
            return int(receipt_token_id)
    finally:
        await engine.dispose()


@pytest.fixture(scope="module")
def seeded_receipt_token_id(async_db_url: str) -> int:
    """A receipt_token that exists before its own token row has been indexed."""
    return asyncio.run(_seed(async_db_url))


@pytest.fixture()
def client(async_db_url: str, tmp_path: Path):
    """Return a TestClient wired to the module's isolated database."""
    empty_mapping = tmp_path / "empty_mapping.json"
    empty_mapping.write_text("{}")
    test_app = create_app(
        Settings.model_validate({"database_url": SecretStr(async_db_url), "suraf_mappings_file": empty_mapping})
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


def test_risk_by_address_returns_404_with_hint_when_address_is_underlying(
    client: TestClient,
    seeded_receipt_token_id: int,
) -> None:
    """Passing the underlying ERC-20 address yields 404 with a hint listing the wrapping receipt tokens."""
    weth_address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    response = client.get(f"/v1/risk/1/{weth_address}/breakdown")

    assert response.status_code == 404
    detail = response.json()["detail"]
    assert isinstance(detail, dict)
    assert "underlying" in detail["message"]
    assert len(detail["suggestions"]) >= 1
    assert any(s["receipt_token_address"].lower() == f"0x{_RECEIPT_TOKEN_ADDRESS_HEX}" for s in detail["suggestions"])


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
