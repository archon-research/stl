"""Integration tests for /v1/risk/rrc with the packaged CORE mapping and an empty results table.

Every other API fixture wires ``core_model_mappings_file`` to an empty mapping,
so nothing exercised the real deployment shape: the packaged mapping resolves
against the migration-seeded receipt tokens while ``core_model_results`` has no
rows (the runner cronjob lands in PR 2). These tests pin the degradation
contract for that window: the CORE model is skipped, it never turns a working
endpoint into a 500, and the other models' results are still served.
"""

from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.config import Settings
from app.main import create_app

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
