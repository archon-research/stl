"""Integration tests for a proxy the pinned axis-synome contract does not know.

Its own module — and so its own isolated database — because it seeds a third spark
proxy that ``test_prime_fan_out_api.py`` asserts the absence of.

The state under test is real and reachable: Go loads the committed contract JSON at
runtime while Python recomputes from its pinned ``axis-synome`` package, so a newly
deployed tracker can be writing ``allocation_position`` rows for a proxy this
service has not been told about. Nothing gates the two against each other. What
must not happen then is a second copy of a prime-scoped row: a consumer unioning a
prime's proxies would count the $250M Anchorage leg twice.
"""

import asyncio
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.config import Settings
from app.main import create_app
from tests.integration.seed import (
    SPARK_MAINNET_ALM_HEX,
    SPARK_OFF_CONTRACT_ALM_HEX,
    seed_prime_fan_out,
)

_SPARK_MAINNET_ALM = f"0x{SPARK_MAINNET_ALM_HEX}"
_SPARK_OFF_CONTRACT_ALM = f"0x{SPARK_OFF_CONTRACT_ALM_HEX}"


@pytest.fixture(scope="module")
def async_db_url(module_db):
    asyncio.run(seed_prime_fan_out(module_db["db_url"], with_off_contract_proxy=True))
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


def _custody_rows(client: TestClient, proxy: str) -> list[dict]:
    return [row for row in client.get(f"/v1/primes/{proxy}/allocations").json() if row["scope"] == "prime"]


def test_the_off_contract_proxy_is_listed_and_seeded(client: TestClient) -> None:
    # Guards every assertion below from passing because the fixture is inert:
    # classify_proxy treats an unknown address as ALM, so it must reach /v1/primes.
    addresses = {row["address"] for row in client.get("/v1/primes").json() if row["name"] == "spark"}

    assert _SPARK_OFF_CONTRACT_ALM in addresses


def test_the_custody_leg_is_served_under_exactly_one_proxy(client: TestClient) -> None:
    spark = [row["address"] for row in client.get("/v1/primes").json() if row["name"] == "spark"]

    carrying = [address for address in spark if _custody_rows(client, address)]

    assert carrying == [_SPARK_MAINNET_ALM]


def test_the_off_contract_proxy_does_not_serve_a_second_copy(client: TestClient) -> None:
    assert _custody_rows(client, _SPARK_OFF_CONTRACT_ALM) == []


def test_the_off_contract_proxys_own_holdings_still_surface(client: TestClient) -> None:
    # Only the prime-scoped row is withheld; the proxy's own positions are its own.
    rows = client.get(f"/v1/primes/{_SPARK_OFF_CONTRACT_ALM}/allocations").json()

    assert [row["symbol"] for row in rows] == ["JAAA"]


def test_risk_capital_answers_for_the_off_contract_proxy_over_itself_alone(client: TestClient) -> None:
    """No siblings are discoverable for it, which the endpoint documents.

    ``prime_name`` is null and the aggregation covers the queried proxy only, so a
    consumer can tell this figure apart from a prime-wide one rather than reading
    it as authoritative for spark.
    """
    body = client.get(f"/v1/primes/{_SPARK_OFF_CONTRACT_ALM}/risk-capital").json()

    assert body["prime_name"] is None
    assert [address.lower() for address in body["prime_proxies"]] == [_SPARK_OFF_CONTRACT_ALM]
