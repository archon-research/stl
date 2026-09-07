"""Integration tests for /v1/risk/rrc with the packaged CORE mapping and an empty results table.

Every other API fixture wires ``core_model_mappings_file`` to an empty mapping,
so nothing exercised the real deployment shape: the packaged mapping resolves
against the migration-seeded receipt tokens while ``core_model_results`` has no
rows (the runner cronjob lands in PR 2). These tests pin the degradation
contract for that window: the CORE model is skipped, it never turns a working
endpoint into a 500, and the other models' results are still served.
"""

import asyncpg
import pytest_asyncio
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.config import Settings
from app.main import create_app
from tests.integration.seed import insert_oracle_asset, insert_token, insert_user

# Migration-seeded SparkLend receipt tokens on chain 1, as mapped by the
# packaged mapping files. spUSDT is in both the SURAF and CORE mappings;
# spDAI is in the CORE mapping only (gap_sweep applies to both regardless).
_SPUSDT = "0xe7df13b8e3d6740fe17cbe928c7334243d86c92f"
_SPDAI = "0x4dedf26112b3ec8ec46e7e31ea5e123490b05b8b"
_PRIME_ID = "0x" + "ab" * 20


def _scenario_body(token_address: str) -> dict:
    # usd_exposure overrides keep the test independent of allocation-position
    # seeding: exposure resolution succeeds for every model that accepts the
    # override, so the only degradation under test is the empty results table.
    return {
        "chain_id": 1,
        "token_address": token_address,
        "prime_id": _PRIME_ID,
        "overrides": {
            "suraf": {"usd_exposure": "1000"},
            "core_model": {"usd_exposure": "1000"},
        },
    }


def _client(async_db_url: str) -> TestClient:
    """App with the packaged (real) mapping files, unlike every other API fixture."""
    return TestClient(create_app(Settings.model_validate({"database_url": SecretStr(async_db_url)})))


def test_spusdt_rrc_still_serves_suraf_when_core_results_table_is_empty(async_db_url: str) -> None:
    """The asset SURAF serves in prod today keeps returning 200 with CORE mapped but unpopulated."""
    with _client(async_db_url) as client:
        response = client.post("/v1/risk/rrc/scenario", json=_scenario_body(_SPUSDT))

    assert response.status_code == 200
    models = [r["risk_model"] for r in response.json()["results"]]
    assert "suraf" in models
    assert "core_model" not in models


def test_spdai_rrc_skips_core_and_serves_remaining_models(async_db_url: str) -> None:
    """A CORE-mapped asset with no result row degrades to the other applicable models."""
    with _client(async_db_url) as client:
        response = client.post("/v1/risk/rrc/scenario", json=_scenario_body(_SPDAI))

    assert response.status_code == 200
    models = [r["risk_model"] for r in response.json()["results"]]
    assert "core_model" not in models
    assert models  # the envelope is degraded, never empty-yet-200


def test_spdai_rrc_without_overrides_skips_core_via_missing_position(async_db_url: str) -> None:
    """No override → CORE hits the no-position path; that is a skip, not a 500."""
    with _client(async_db_url) as client:
        response = client.get(f"/v1/risk/rrc?chain_id=1&token_address={_SPDAI}&prime_id={_PRIME_ID}")

    assert response.status_code == 200
    models = [r["risk_model"] for r in response.json()["results"]]
    assert "core_model" not in models


# ---------------------------------------------------------------------------
# Morpho vault-share serving through the real app (VEC-654)
#
# Two vaults behind real receipt_token rows, so the startup snapshot picks
# them up and the whole pipe runs: applies_to → vault allocations →
# per-market results → aggregation, next to gap_sweep in the same envelope.
#
#   Full vault:  1M USDC, 400K in cbBTC/USDC (computed, CRR 2.5%), 600K idle
#                → coverage 100%, CORE served alongside gap_sweep.
#   Data-less vault: registered but never snapshotted (no state, no
#                positions) → CORE skips, gap_sweep still answers.
# ---------------------------------------------------------------------------

_MORPHO_BLOCK = 21_000_000
_FULL_VAULT_ADDRESS = b"\x0f" * 20
_DATALESS_VAULT_ADDRESS = b"\x1f" * 20
_FULL_VAULT = "0x" + _FULL_VAULT_ADDRESS.hex()
_DATALESS_VAULT = "0x" + _DATALESS_VAULT_ADDRESS.hex()
_MORPHO_USDC_ADDRESS = b"\xa0\xb8\x69\x91\xc6\x21\x8b\x36\xc1\xd1\x9d\x4a\x2e\x9e\xb0\xce\x36\x06\xeb\x48"
_CBBTC_ADDRESS = b"\xcb\xb7\xc0\x00\x00\xab\x88\xb4\x73\xb1\xf5\xaf\xd9\xef\x80\x84\x40\xee\xd3\x3b"


async def _seed_morpho_vault_scenarios(db_url: str) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        protocol_id = await conn.fetchval(
            """
            INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at)
            VALUES (1, $1, 'Morpho Blue', 'morpho_blue', 18883124, NOW())
            ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            b"\xbb" * 19 + b"\x02",
        )
        usdc_id = await insert_token(conn, "USDC", 6, _MORPHO_USDC_ADDRESS)
        cbbtc_id = await insert_token(conn, "cbBTC", 8, _CBBTC_ADDRESS)
        full_share_id = await insert_token(conn, "mcoreUSDC", 18, _FULL_VAULT_ADDRESS)
        await insert_token(conn, "mthinUSDC", 18, _DATALESS_VAULT_ADDRESS)

        # gap_sweep prices the breakdown through the protocol's chainlink binding;
        # an unpriced loan token would degrade the whole envelope to 503.
        chainlink_id = await conn.fetchval("SELECT id FROM oracle WHERE name = 'chainlink'")
        await conn.execute(
            """
            INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
            VALUES ($1, $2, $3) ON CONFLICT (protocol_id, oracle_id, from_block) DO NOTHING
            """,
            protocol_id,
            chainlink_id,
            _MORPHO_BLOCK,
        )
        await conn.execute(
            """
            INSERT INTO onchain_token_price
                (token_id, oracle_id, block_number, block_version, timestamp, price_usd)
            VALUES ($1, $2, $3, 0, NOW(), 1.0)
            """,
            usdc_id,
            chainlink_id,
            _MORPHO_BLOCK,
        )
        await insert_oracle_asset(conn, chainlink_id, usdc_id, enabled=True)

        for address, symbol, name in (
            (_FULL_VAULT_ADDRESS, "mcoreUSDC", "Covered USDC Vault"),
            (_DATALESS_VAULT_ADDRESS, "mthinUSDC", "Data-less USDC Vault"),
        ):
            await insert_user(conn, address)
            await conn.execute(
                """
                INSERT INTO receipt_token
                    (chain_id, protocol_id, underlying_token_id, receipt_token_address,
                     symbol, created_at_block, metadata, updated_at)
                VALUES (1, $1, $2, $3, $4, $5, '{}', NOW())
                ON CONFLICT (chain_id, receipt_token_address) DO NOTHING
                """,
                protocol_id,
                usdc_id,
                address,
                symbol,
                _MORPHO_BLOCK,
            )

        full_vault_id = await conn.fetchval(
            """
            INSERT INTO morpho_vault
                (chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
            VALUES (1, $1, $2, 'Covered USDC Vault', 'mcoreUSDC', $3, 1, $4) RETURNING id
            """,
            protocol_id,
            _FULL_VAULT_ADDRESS,
            usdc_id,
            _MORPHO_BLOCK,
        )
        await conn.execute(
            """
            INSERT INTO morpho_vault
                (chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
            VALUES (1, $1, $2, 'Data-less USDC Vault', 'mthinUSDC', $3, 1, $4)
            """,
            protocol_id,
            _DATALESS_VAULT_ADDRESS,
            usdc_id,
            _MORPHO_BLOCK,
        )
        await conn.execute(
            """
            INSERT INTO morpho_vault_state
                (morpho_vault_id, block_number, block_version, timestamp, total_assets, total_shares)
            VALUES ($1, $2, 0, NOW(), '1000000000000', '1000000000000')
            """,
            full_vault_id,
            _MORPHO_BLOCK,
        )
        cbbtc_market_id = await conn.fetchval(
            """
            INSERT INTO morpho_market
                (chain_id, protocol_id, market_id, loan_token_id, collateral_token_id,
                 oracle_address, irm_address, lltv, created_at_block)
            VALUES (1, $1, $2, $3, $4, $5, $5, 0.86, $6) RETURNING id
            """,
            protocol_id,
            b"\x0a" * 32,
            usdc_id,
            cbbtc_id,
            b"\x00" * 20,
            _MORPHO_BLOCK,
        )
        full_vault_user_id = await conn.fetchval(
            'SELECT id FROM "user" WHERE address = $1 AND chain_id = 1', _FULL_VAULT_ADDRESS
        )
        await conn.execute(
            """
            INSERT INTO morpho_market_position
                (user_id, morpho_market_id, block_number, block_version, timestamp,
                 supply_shares, borrow_shares, collateral, supply_assets, borrow_assets)
            VALUES ($1, $2, $3, 0, NOW(), '400000000000', 0, 0, '400000000000', 0)
            """,
            full_vault_user_id,
            cbbtc_market_id,
            _MORPHO_BLOCK,
        )

        await conn.execute(
            """
            INSERT INTO core_model_results
                (market_key, crr_el_pct, crr_es_pct, crr_var_pct, hhi, protocol,
                 forecast_step, n_mc, copula_type, computed_at, params)
            VALUES ('morpho_cbbtc-usdc', 2.5, 3.5, 3.0, NULL, 'MORPHO', 14, 100, 'T-COPULA', NOW(), '{}')
            ON CONFLICT DO NOTHING
            """
        )

        # gap_sweep's prime share for the full vault: the prime holds 25% of
        # the vault's shares. The position row sits one block before the
        # supply snapshot, as the share lookup's LATERAL join requires.
        prime_id = await conn.fetchval(
            """
            INSERT INTO prime (name, vault_address) VALUES ('core-degradation-prime', $1)
            ON CONFLICT (name) DO UPDATE SET vault_address = EXCLUDED.vault_address RETURNING id
            """,
            bytes.fromhex(_PRIME_ID[2:]),
        )
        await conn.execute(
            """
            INSERT INTO allocation_position
                (chain_id, token_id, prime_id, proxy_address, balance, scaled_balance,
                 block_number, block_version, tx_hash, log_index, tx_amount, direction, created_at)
            VALUES (1, $1, $2, $3, '250000000000000000000000000000', '250000000000000000000000000000',
                    $4, 0, $5, 0, 0, 'in', NOW())
            """,
            full_share_id,
            prime_id,
            bytes.fromhex(_PRIME_ID[2:]),
            _MORPHO_BLOCK - 1,
            b"\x00" * 32,
        )
        await conn.execute(
            """
            INSERT INTO token_total_supply
                (chain_id, token_id, total_supply, block_number, block_version, block_timestamp, source)
            VALUES (1, $1, '1000000000000000000000000000000', $2, 0, NOW(), 'sweep')
            """,
            full_share_id,
            _MORPHO_BLOCK,
        )
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _morpho_scenarios(db_url: str) -> None:
    await _seed_morpho_vault_scenarios(db_url)


def _morpho_scenario_body(token_address: str) -> dict:
    return {
        "chain_id": 1,
        "token_address": token_address,
        "prime_id": _PRIME_ID,
        "overrides": {"core_model": {"usd_exposure": "1000"}},
    }


def test_morpho_vault_with_covered_market_serves_the_core_aggregate(async_db_url: str, _morpho_scenarios: None) -> None:
    with _client(async_db_url) as client:
        response = client.post("/v1/risk/rrc/scenario", json=_morpho_scenario_body(_FULL_VAULT))

    assert response.status_code == 200, response.text
    results = {r["risk_model"]: r for r in response.json()["results"]}
    assert "gap_sweep" in results
    core = results["core_model"]
    # 40% of the vault at CRR 2.5% + 60% idle at 0 → aggregate 1.0%.
    assert core["comparable_crr_pct"] == "1.0000"
    assert core["rrc_usd"] == "10.00"  # 1000 * 1.0%
    assert core["details"]["coverage_pct"] == "100.00"
    assert [(m["market_key"], m["allocation_pct"]) for m in core["details"]["markets"]] == [
        ("morpho_cbbtc-usdc", "40.00"),
    ]


def test_morpho_vault_without_indexed_data_degrades_to_gap_sweep(async_db_url: str, _morpho_scenarios: None) -> None:
    with _client(async_db_url) as client:
        response = client.post("/v1/risk/rrc/scenario", json=_morpho_scenario_body(_DATALESS_VAULT))

    assert response.status_code == 200, response.text
    models = [r["risk_model"] for r in response.json()["results"]]
    assert "core_model" not in models
    assert models == ["gap_sweep"]
