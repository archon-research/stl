"""Integration tests for prime-level aggregation across per-chain ALM proxies.

Its own module — and so its own isolated database — because it seeds the *real*
axis-synome spark proxy addresses. ``test_allocation_api.py`` seeds spark under a
placeholder address the registry does not recognise, so the fan-out these tests
exercise cannot exist there.

Every test names whether it guards backwards compatibility (an existing field's
value must not have moved) or the new prime-scoped behaviour.
"""

import asyncio
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.config import Settings
from app.main import create_app
from tests.integration.seed import (
    FAN_OUT_PRIME_EXPOSURE_USD,
    SPARK_AVALANCHE_ALM_HEX,
    SPARK_MAINNET_ALM_HEX,
    seed_prime_fan_out,
)

_SPARK_MAINNET_ALM = f"0x{SPARK_MAINNET_ALM_HEX}"
_SPARK_AVALANCHE_ALM = f"0x{SPARK_AVALANCHE_ALM_HEX}"
_SPARK_VAULT = "0x691a6c29e9e96dd897718305427ad5d534db16ba"


@pytest.fixture(scope="module")
def async_db_url(module_db):
    asyncio.run(seed_prime_fan_out(module_db["db_url"]))
    return module_db["async_url"]


@pytest.fixture()
def client(async_db_url: str, tmp_path: Path):
    empty_mapping = tmp_path / "empty_mapping.json"
    empty_mapping.write_text("{}")
    test_app = create_app(
        Settings.model_validate({"database_url": SecretStr(async_db_url), "suraf_mappings_file": empty_mapping})
    )
    with TestClient(test_app) as c:
        yield c


def _spark_rows(client: TestClient) -> list[dict]:
    return [row for row in client.get("/v1/primes").json() if row["name"] == "spark"]


def _stub_star_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stand in for the third-party Star risk-capital fetch with a fixed spark row.

    ``/v1/capital-metrics`` awaits a real ``httpx`` call to Blockanalitica
    (``_fetch_star_risk_capital_payload``); AGENTS.md only allows integration
    tests to mock data sources we do not control, and a third party is exactly
    that. Stubbing only this half keeps the rest of the request (list_primes,
    the real seeded database) exercising the genuine path.
    """
    from app.api.v1 import allocations

    async def _fake_payload() -> allocations.StarRiskCapitalResponse:
        return allocations.StarRiskCapitalResponse.model_validate(
            {
                "status": 200,
                "success": True,
                "data": {
                    "results": [
                        {
                            "star": "spark",
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


def test_primes_lists_both_of_sparks_alm_proxies(client: TestClient) -> None:
    assert len(_spark_rows(client)) == 2


def test_primes_returns_one_row_per_proxy_and_chain(client: TestClient) -> None:
    """Guards the cardinality the `DISTINCT ON` key promises.

    Keyed on proxy_address alone, a proxy holding positions on two chains would
    collapse to one row carrying whichever chain had the higher block_number —
    a number that is not comparable across chains.
    """
    rows = _spark_rows(client)

    assert len({(row["address"], row["chain_id"]) for row in rows}) == len(rows)


def test_primes_labels_each_proxy_with_its_chain_id(client: TestClient) -> None:
    assert {row["chain_id"] for row in _spark_rows(client)} == {1, 43114}


def test_primes_derives_the_chain_name_from_the_chain_id(client: TestClient) -> None:
    rows = {row["address"]: row["chain"] for row in _spark_rows(client)}

    assert rows[_SPARK_MAINNET_ALM] == "mainnet"
    assert rows[_SPARK_AVALANCHE_ALM] == "avalanche-c"


def test_primes_shares_one_vault_address_across_a_primes_proxies(client: TestClient) -> None:
    assert {row["prime_vault_address"] for row in _spark_rows(client)} == {_SPARK_VAULT}


def test_primes_excludes_the_subproxy_treasury_wallet(client: TestClient) -> None:
    addresses = {row["address"] for row in _spark_rows(client)}

    assert addresses == {_SPARK_MAINNET_ALM, _SPARK_AVALANCHE_ALM}


def test_backwards_compat_capital_metrics_still_returns_one_row_per_proxy(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    _stub_star_payload(monkeypatch)

    rows = [row for row in client.get("/v1/capital-metrics").json() if row["prime_name"] == "spark"]

    assert len(rows) == 2


def test_capital_metrics_rows_carry_a_shared_dedupe_key(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_star_payload(monkeypatch)

    rows = [row for row in client.get("/v1/capital-metrics").json() if row["prime_name"] == "spark"]

    assert {row["prime_vault_address"] for row in rows} == {_SPARK_VAULT}


def test_backwards_compat_risk_capital_exposure_stays_proxy_scoped(client: TestClient) -> None:
    # The priced position sits on mainnet only, so the avalanche proxy's own
    # exposure must still read zero — this is the field consumers read today.
    body = client.get(f"/v1/primes/{_SPARK_AVALANCHE_ALM}/risk-capital").json()

    assert body["exposure_usd"] == "0"


def test_risk_capital_reports_the_same_prime_exposure_from_every_proxy(client: TestClient) -> None:
    # Compared as Decimal, not raw strings: SQL arithmetic widens the scale
    # (e.g. "1000.000000000000000000"), matching test_allocation_api.py's
    # amount_usd assertions rather than the money-as-string serialization rule,
    # which is about type (str, not float) and does not pin an exact scale.
    exposures = {
        Decimal(client.get(f"/v1/primes/{row['address']}/risk-capital").json()["prime_exposure_usd"])
        for row in _spark_rows(client)
    }

    assert exposures == {Decimal(FAN_OUT_PRIME_EXPOSURE_USD)}


def test_risk_capital_prime_exposure_is_non_zero(client: TestClient) -> None:
    # Guards the test above from passing vacuously on an all-zero fixture.
    body = client.get(f"/v1/primes/{_SPARK_AVALANCHE_ALM}/risk-capital").json()

    assert body["prime_exposure_usd"] != "0"


def test_risk_capital_reports_prime_identity_from_a_non_mainnet_proxy(client: TestClient) -> None:
    body = client.get(f"/v1/primes/{_SPARK_AVALANCHE_ALM}/risk-capital").json()

    assert body["prime_name"] == "spark"


def test_risk_capital_lists_the_sibling_proxies_it_aggregated(client: TestClient) -> None:
    body = client.get(f"/v1/primes/{_SPARK_AVALANCHE_ALM}/risk-capital").json()

    assert _SPARK_MAINNET_ALM in [address.lower() for address in body["prime_proxies"]]


def test_custody_leg_is_served_under_the_mainnet_proxy_only(client: TestClient) -> None:
    carrying = [
        row["address"]
        for row in _spark_rows(client)
        if any(alloc["scope"] == "prime" for alloc in client.get(f"/v1/primes/{row['address']}/allocations").json())
    ]

    assert carrying == [_SPARK_MAINNET_ALM]


def test_risk_capital_reports_the_same_prime_encumbrance_ratio_from_every_proxy(client: TestClient) -> None:
    ratios = {
        client.get(f"/v1/primes/{row['address']}/risk-capital").json()["prime_encumbrance_ratio"]
        for row in _spark_rows(client)
    }

    assert len(ratios) == 1


def test_risk_capital_prime_encumbrance_ratio_is_non_null(client: TestClient) -> None:
    # Guards the test above from passing vacuously on a fixture where every
    # proxy resolves to a null ratio (e.g. a missing SubProxy treasury).
    body = client.get(f"/v1/primes/{_SPARK_AVALANCHE_ALM}/risk-capital").json()

    assert body["prime_encumbrance_ratio"] is not None


def test_risk_capital_reports_null_for_a_chain_no_tracker_serves(client: TestClient) -> None:
    """Spark's contract proxies on unserved chains have no rows at all.

    Reported as ``"0"`` they would be indistinguishable from a served chain the
    prime genuinely holds nothing on, which understates encumbrance.
    """
    body = client.get(f"/v1/primes/{_SPARK_MAINNET_ALM}/risk-capital").json()

    unserved = [row for row in body["prime_per_chain"] if row["chain"] in body["prime_unserved_chains"]]
    assert unserved != []
    assert all(row["exposure_usd"] is None and row["required_risk_capital_usd"] is None for row in unserved)


def test_risk_capital_names_the_unserved_chains_identically_from_every_proxy(client: TestClient) -> None:
    reported = {
        tuple(client.get(f"/v1/primes/{row['address']}/risk-capital").json()["prime_unserved_chains"])
        for row in _spark_rows(client)
    }

    assert reported == {("arbitrum", "optimism", "unichain")}


def test_risk_capital_prime_per_chain_sums_to_the_prime_total(client: TestClient) -> None:
    """The auditability `prime_per_chain` is sold on, over the real seeded data."""
    body = client.get(f"/v1/primes/{_SPARK_AVALANCHE_ALM}/risk-capital").json()

    per_chain_total = sum(
        Decimal(row["exposure_usd"]) for row in body["prime_per_chain"] if row["exposure_usd"] is not None
    )
    assert per_chain_total == Decimal(body["prime_exposure_usd"])


def test_the_whole_prime_scoped_projection_is_identical_from_every_proxy(client: TestClient) -> None:
    """One assertion over every `prime_*` field, so a new field is covered by default.

    The per-field tests above pin the values; this pins the invariant the fields
    are sold on — that a consumer can dedupe on them — across the prime's proxies.
    """
    projections = {
        tuple(
            sorted(
                (key, str(value))
                for key, value in client.get(f"/v1/primes/{row['address']}/risk-capital").json().items()
                if key.startswith("prime_") and key != "prime_id"
            )
        )
        for row in _spark_rows(client)
    }

    assert len(projections) == 1


def test_custody_leg_moves_with_the_data_not_the_contract_pin(client: TestClient) -> None:
    """Attribution is resolved from `allocation_position`, so it cannot double-serve.

    The seeded prime's mainnet proxy carries the leg; asking as any other proxy of
    the same prime — contract-known or not — must not produce a second copy.
    """
    unknown_to_contract = "0x" + "cd" * 20
    served = [
        row["address"]
        for row in _spark_rows(client)
        if any(alloc["scope"] == "prime" for alloc in client.get(f"/v1/primes/{row['address']}/allocations").json())
    ]

    assert served == [_SPARK_MAINNET_ALM]
    assert client.get(f"/v1/primes/{unknown_to_contract}/allocations").status_code == 404


def test_non_primary_proxys_on_chain_holdings_survive_the_custody_drop(client: TestClient) -> None:
    # Only the Anchorage custody row is scoped away from a non-primary proxy;
    # avalanche's own on-chain holding (a direct, non-receipt-token asset) must
    # still surface, proving the drop targets the custody row alone.
    rows = client.get(f"/v1/primes/{_SPARK_AVALANCHE_ALM}/allocations").json()

    assert [row["symbol"] for row in rows] == ["JAAA"]
