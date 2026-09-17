"""Every accepted identifier answers the same whole prime (VEC-722).

Its own module — and so its own isolated database — because it seeds the *real*
axis-synome spark and grove proxies, which is what makes name, vault, ALM proxy
and SubProxy four names for one prime rather than four addresses.

The routes are adopted one slice at a time, so ``WHOLE_PRIME_ROUTES`` grows as
each takes the resolved scope. A route listed here must answer byte-identically
from every form; one not listed has not been moved yet.
"""

import asyncio
from datetime import timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.config import Settings
from app.main import create_app
from tests.integration.seed import (
    FAN_OUT_TREASURY,
    FAN_OUT_TREASURY_OBSERVED_AT,
    GROVE_MAINNET_ALM_HEX,
    SPARK_AVALANCHE_ALM_HEX,
    SPARK_MAINNET_ALM_HEX,
    SPARK_SUB_PROXY_HEX,
    seed_prime_fan_out,
)

_SPARK_VAULT = "0x691a6c29e9e96dd897718305427ad5d534db16ba"

#: Four forms of one prime: name, vault, an ALM proxy on each of two chains, and
#: the SubProxy treasury. Every one must produce the same response.
SPARK_IDENTIFIERS = [
    "spark",
    _SPARK_VAULT,
    f"0x{SPARK_MAINNET_ALM_HEX}",
    f"0x{SPARK_AVALANCHE_ALM_HEX}",
    f"0x{SPARK_SUB_PROXY_HEX}",
]

#: Routes already carrying whole-prime semantics. VEC-722 adds one per slice.
WHOLE_PRIME_ROUTES = ["total-capital"]

#: Pinned around the seeded observation: a defaulted window is now-relative, so
#: two requests a millisecond apart would differ in `window` alone.
_PINNED_WINDOW = {
    "from_timestamp": (FAN_OUT_TREASURY_OBSERVED_AT - timedelta(hours=6)).isoformat(),
    "to_timestamp": (FAN_OUT_TREASURY_OBSERVED_AT + timedelta(hours=6)).isoformat(),
    "frequency": "PT1H",
}


@pytest.fixture(scope="module")
def async_db_url(module_db):
    asyncio.run(seed_prime_fan_out(module_db["db_url"]))
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


@pytest.mark.parametrize("route", WHOLE_PRIME_ROUTES)
def test_every_identifier_form_returns_the_same_body(client: TestClient, route: str) -> None:
    bodies = {
        client.get(f"/v1/primes/{identifier}/{route}", params=_PINNED_WINDOW).text for identifier in SPARK_IDENTIFIERS
    }

    assert len(bodies) == 1


@pytest.mark.parametrize("route", WHOLE_PRIME_ROUTES)
@pytest.mark.parametrize("identifier", SPARK_IDENTIFIERS)
def test_every_identifier_form_is_accepted(client: TestClient, route: str, identifier: str) -> None:
    assert client.get(f"/v1/primes/{identifier}/{route}", params=_PINNED_WINDOW).status_code == 200


@pytest.mark.parametrize("route", WHOLE_PRIME_ROUTES)
def test_the_treasury_is_counted_once_rather_than_per_proxy(client: TestClient, route: str) -> None:
    """Spark has six ALM proxies. A treasury summed over them would be six times
    the figure the SubProxy holds, and would read as entirely plausible."""
    body = client.get(f"/v1/primes/spark/{route}", params=_PINNED_WINDOW).json()
    observed = [bucket["total_capital_usd"] for bucket in body["data"] if bucket["total_capital_usd"] is not None]

    assert observed
    assert all(float(value) == float(FAN_OUT_TREASURY) for value in observed)


@pytest.mark.parametrize("route", WHOLE_PRIME_ROUTES)
def test_a_prime_with_no_proxies_answers_a_series_not_a_404(client: TestClient, route: str) -> None:
    """obex is a vault with no declared proxies. Whole-prime aggregation over an
    empty wallet set is an answer, not an error — and the same shape as a prime
    whose wallets have nothing indexed, so a chart gets a window either way."""
    response = client.get(f"/v1/primes/obex/{route}", params=_PINNED_WINDOW)

    assert response.status_code == 200
    buckets = response.json()["data"]
    # Guards the null assertion below from passing on an empty list.
    assert buckets, "an empty wallet set must still gapfill the window"
    observed = [value for bucket in buckets for key, value in bucket.items() if key != "bucket_start"]
    assert all(value is None for value in observed)


@pytest.mark.parametrize("route", WHOLE_PRIME_ROUTES)
def test_an_unknown_identifier_is_404_not_an_existence_oracle(client: TestClient, route: str) -> None:
    response = client.get(f"/v1/primes/not-a-prime/{route}")

    assert (response.status_code, response.json()["detail"]) == (404, "prime not found")


@pytest.mark.parametrize("route", WHOLE_PRIME_ROUTES)
def test_a_malformed_address_is_422(client: TestClient, route: str) -> None:
    assert client.get(f"/v1/primes/0xdeadbeef/{route}").status_code == 422


def test_another_primes_proxy_resolves_to_that_prime_not_this_one(client: TestClient) -> None:
    """Guards the identity tests above from passing on a resolver that answers
    the same prime for everything."""
    spark = client.get("/v1/primes/spark/total-capital", params=_PINNED_WINDOW).text
    grove = client.get(f"/v1/primes/0x{GROVE_MAINNET_ALM_HEX}/total-capital", params=_PINNED_WINDOW).json()

    assert client.get("/v1/primes/grove/total-capital", params=_PINNED_WINDOW).json() == grove
    assert spark != grove
