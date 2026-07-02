"""Integration tests for the Maple Syrup risk API slice.

Proves the breakdown endpoint surfaces Maple collateral (symbol-keyed, null
liquidation params) via the migration-seeded ``syrupUSDC`` receipt token, and
that RRC degrades gracefully (no applicable model => 404) because Maple has no
quantitative risk model.
"""

import asyncio
import datetime as dt
from pathlib import Path

import asyncpg
import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.config import Settings
from app.main import create_app
from tests.integration.seed import (
    insert_maple_loan,
    insert_maple_loan_collateral,
    insert_maple_loan_state,
    insert_maple_pool,
    insert_maple_pool_state,
    insert_user,
    maple_seed_ids,
)

SYRUP_USDC_HEX = "80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"
_PRIME_ID = "0x" + "01" * 20
_BORROWER = bytes.fromhex("4444444444444444444444444444444444444444")
_EXTERNAL_LOAN = bytes.fromhex("1111111111111111111111111111111111111111")
_SYNCED = dt.datetime(2026, 6, 18, 12, 0, tzinfo=dt.timezone.utc)


async def _seed(db_url: str) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        protocol_id, usdc_id = await maple_seed_ids(conn)
        pool_id = await insert_maple_pool(
            conn,
            protocol_id=protocol_id,
            address=bytes.fromhex(SYRUP_USDC_HEX),
            asset_token_id=usdc_id,
            synced_at=_SYNCED,
        )
        await insert_maple_pool_state(conn, pool_id=pool_id, synced_at=_SYNCED, liquid_assets=1000000000000)
        borrower_id = await insert_user(conn, _BORROWER)
        loan_id = await insert_maple_loan(
            conn,
            protocol_id=protocol_id,
            pool_id=pool_id,
            borrower_user_id=borrower_id,
            address=_EXTERNAL_LOAN,
            synced_at=_SYNCED,
        )
        await insert_maple_loan_state(
            conn, loan_id=loan_id, synced_at=_SYNCED, state="Active", principal_owed=120000000000, acm_ratio=1656007
        )
        await insert_maple_loan_collateral(
            conn,
            loan_id=loan_id,
            synced_at=_SYNCED,
            symbol="BTC",
            amount=200000000,
            decimals=8,
            value_usd=6500000000000,
        )
    finally:
        await conn.close()


@pytest.fixture(scope="module")
def maple_seed(db_url: str) -> None:
    asyncio.run(_seed(db_url))


@pytest.fixture
def client(async_db_url: str, tmp_path: Path):
    empty_mapping = tmp_path / "empty_mapping.json"
    empty_mapping.write_text("{}")
    test_app = create_app(
        Settings.model_validate({"database_url": SecretStr(async_db_url), "suraf_mappings_file": empty_mapping})
    )
    with TestClient(test_app) as c:
        yield c


def test_breakdown_endpoint_returns_maple_collateral(client: TestClient, maple_seed: None) -> None:
    response = client.get(f"/v1/risk/1/0x{SYRUP_USDC_HEX}/breakdown")

    assert response.status_code == 200, response.text
    body = response.json()
    by_symbol = {i["symbol"]: i for i in body["items"]}
    assert {"BTC", "USDC"} <= set(by_symbol)
    btc = by_symbol["BTC"]
    assert btc["token_id"] is None
    assert btc["liquidation_threshold"] is None
    assert btc["liquidation_bonus"] is None
    assert btc["amount_usd"] == "130000.00"
    assert by_symbol["USDC"]["amount_usd"] == "1000000.00"


# syrupUSDG: receipt_token is migration-seeded but no test seeds its maple_pool,
# so the "registered but not yet indexed" path holds regardless of test order.
_SYRUP_USDG_HEX = "87b65c4aaffa76881f9e96f3e7ed945ddfc3cd7a"


def test_breakdown_endpoint_empty_when_pool_unindexed(client: TestClient) -> None:
    # The syrupUSDG receipt_token is migration-seeded but its maple_pool has not
    # been indexed. The endpoint must degrade to 200 with an empty breakdown,
    # not 4xx/5xx.
    response = client.get(f"/v1/risk/1/0x{_SYRUP_USDG_HEX}/breakdown")

    assert response.status_code == 200, response.text
    assert response.json()["items"] == []


def test_rrc_returns_404_no_applicable_model(client: TestClient, maple_seed: None) -> None:
    # Maple is excluded from the gap-sweep RRC set and has no SURAF mapping, so
    # no model applies. The endpoint must degrade to 404, never 500.
    response = client.get(
        "/v1/risk/rrc",
        params={"chain_id": 1, "token_address": f"0x{SYRUP_USDC_HEX}", "prime_id": _PRIME_ID},
    )

    assert response.status_code == 404
    assert "no risk models apply" in response.json()["detail"]
