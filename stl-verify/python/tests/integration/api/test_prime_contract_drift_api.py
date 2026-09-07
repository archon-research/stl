"""Integration tests for a proxy that has positions but is not declared.

Its own module — and so its own isolated database — because it seeds a third spark
proxy that ``test_prime_fan_out_api.py`` asserts the absence of.

The state under test is real and reachable: a newly deployed tracker writes
``allocation_position`` rows for a proxy before the migration that adds it to
``prime_proxy`` has landed. ``prime_proxy`` is the declared source of truth, so
until that row exists the proxy is not a prime address at all — it is not listed,
and every prime-scoped endpoint 404s for it.

That is a deliberate trade, and it cuts both ways. It makes a second copy of a
prime-scoped row impossible (a consumer unioning a prime's proxies cannot count the
$250M Anchorage leg twice), and it makes the undeclared proxy's own positions
unreachable until the declaration lands. Onboarding a proxy is therefore one PR
that updates the axis-synome contract and appends the ``prime_proxy`` row together.
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


def _custody_rows(client: TestClient, proxy: str) -> list[dict]:
    return [row for row in client.get(f"/v1/primes/{proxy}/allocations").json() if row["scope"] == "prime"]


def test_the_undeclared_proxy_is_not_listed(client: TestClient) -> None:
    """It holds rows under spark's prime_id, but /v1/primes follows prime_proxy."""
    addresses = {row["address"] for row in client.get("/v1/primes").json() if row["name"] == "spark"}

    assert _SPARK_OFF_CONTRACT_ALM not in addresses


def test_the_custody_leg_is_served_under_exactly_one_proxy(client: TestClient) -> None:
    spark = [row["address"] for row in client.get("/v1/primes").json() if row["name"] == "spark"]

    carrying = [address for address in spark if _custody_rows(client, address)]

    assert carrying == [_SPARK_MAINNET_ALM]


def test_the_undeclared_proxy_cannot_serve_a_second_copy(client: TestClient) -> None:
    """The prime-scoped leg stays on exactly one proxy: this one is unreachable."""
    assert client.get(f"/v1/primes/{_SPARK_OFF_CONTRACT_ALM}/allocations").status_code == 404


def test_the_undeclared_proxys_own_holdings_are_unreachable(client: TestClient) -> None:
    """Its JAAA position is indexed and real, and still cannot be read.

    This is the cost of resolving from the declared list rather than from ingest,
    and it is why the declaration has to ship with the tracker that writes the rows.
    """
    assert client.get(f"/v1/primes/{_SPARK_OFF_CONTRACT_ALM}/allocations").status_code == 404


def test_risk_capital_does_not_answer_for_the_undeclared_proxy(client: TestClient) -> None:
    """Better a 404 than a figure a consumer could read as authoritative for spark."""
    assert client.get(f"/v1/primes/{_SPARK_OFF_CONTRACT_ALM}/risk-capital").status_code == 404
