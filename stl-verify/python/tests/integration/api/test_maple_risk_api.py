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

SYRUP_USDC_HEX = "80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"
_PRIME_ID = "0x" + "01" * 20
_BORROWER = bytes.fromhex("4444444444444444444444444444444444444444")
_EXTERNAL_LOAN = bytes.fromhex("1111111111111111111111111111111111111111")
_SYNCED = dt.datetime(2026, 6, 18, 12, 0, tzinfo=dt.timezone.utc)


async def _seed(db_url: str) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        protocol_id = await conn.fetchval("SELECT id FROM protocol WHERE chain_id = 1 AND name = 'maple'")
        usdc_id = await conn.fetchval("SELECT id FROM token WHERE chain_id = 1 AND symbol = 'USDC'")
        pool_id = await conn.fetchval(
            "INSERT INTO maple_pool (chain_id, protocol_id, address, name, asset_token_id, is_syrup) "
            "VALUES (1, $1, $2, 'Syrup USDC', $3, true) RETURNING id",
            protocol_id,
            bytes.fromhex(SYRUP_USDC_HEX),
            usdc_id,
        )
        await conn.execute(
            "INSERT INTO maple_pool_state "
            "(maple_pool_id, synced_at, liquid_assets, principal_out, utilization, monthly_apy, spot_apy) "
            "VALUES ($1, $2, 1000000000000, 0, 0, 0, 0)",
            pool_id,
            _SYNCED,
        )
        borrower_id = await conn.fetchval(
            'INSERT INTO "user" (chain_id, address) VALUES (1, $1) ON CONFLICT DO NOTHING RETURNING id',
            _BORROWER,
        )
        loan_id = await conn.fetchval(
            "INSERT INTO maple_loan (chain_id, protocol_id, loan_address, maple_pool_id, borrower_user_id) "
            "VALUES (1, $1, $2, $3, $4) RETURNING id",
            protocol_id,
            _EXTERNAL_LOAN,
            pool_id,
            borrower_id,
        )
        await conn.execute(
            "INSERT INTO maple_loan_state (maple_loan_id, synced_at, state, principal_owed, acm_ratio) "
            "VALUES ($1, $2, 'Active', 120000000000, 1656007)",
            loan_id,
            _SYNCED,
        )
        await conn.execute(
            "INSERT INTO maple_loan_collateral "
            "(maple_loan_id, synced_at, asset_symbol, asset_amount, asset_decimals, asset_value_usd, state) "
            "VALUES ($1, $2, 'BTC', 200000000, 8, 6500000000000, 'Deposited')",
            loan_id,
            _SYNCED,
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


def test_rrc_returns_404_no_applicable_model(client: TestClient, maple_seed: None) -> None:
    # Maple is excluded from the gap-sweep RRC set and has no SURAF mapping, so
    # no model applies. The endpoint must degrade to 404, never 500.
    response = client.get(
        "/v1/risk/rrc",
        params={"chain_id": 1, "token_address": f"0x{SYRUP_USDC_HEX}", "prime_id": _PRIME_ID},
    )

    assert response.status_code == 404
    assert "no risk models apply" in response.json()["detail"]
