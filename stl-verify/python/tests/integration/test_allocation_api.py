"""Integration tests for the allocation API endpoints.

Uses a shared TimescaleDB container (session-scoped) with an isolated database
per test module.  Migrations are applied by the ``module_db`` fixture from
``conftest.py``; this file only seeds test-specific data.
"""

import asyncio

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import Settings
from app.main import create_app

# Hex bytes used in assertions (20-byte proxy, 20-byte token addr, 32-byte tx hash).
_PROXY_HEX = "1234567890abcdef1234567890abcdef12345678"
_GROVE_PROXY_HEX = "abcdef1234567890abcdef1234567890abcdef12"
_UNKNOWN_PROXY_HEX = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
_TOKEN_HEX = "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
_TX1_HEX = "aa" * 32
_TX2_HEX = "bb" * 32


async def _seed(async_url: str) -> None:
    """Insert fixture rows. Chain data is already present from the migrations."""
    engine = create_async_engine(async_url)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(
                text("SELECT id FROM token WHERE chain_id = 1 AND address = decode(:addr, 'hex')"),
                {"addr": _TOKEN_HEX},
            )
            token_id = result.scalar_one()

            result = await conn.execute(text("SELECT id FROM prime WHERE name = 'spark'"))
            spark_id = result.scalar_one()

            result = await conn.execute(text("SELECT id FROM prime WHERE name = 'grove'"))
            grove_id = result.scalar_one()

            # Two rows for 'spark' at different block numbers (proxy = _PROXY_HEX).
            for block, tx_hex in [(1000, _TX1_HEX), (2000, _TX2_HEX)]:
                await conn.execute(
                    text(
                        "INSERT INTO allocation_position "
                        "(chain_id, token_id, prime_id, proxy_address, balance, "
                        "block_number, tx_hash, log_index, tx_amount, direction) "
                        "VALUES (1, :tid, :prime_id, decode(:proxy, 'hex'), 500, :bn, decode(:tx, 'hex'), 0, 500, 'in')"
                    ),
                    {"tid": token_id, "prime_id": spark_id, "proxy": _PROXY_HEX, "bn": block, "tx": tx_hex},
                )

            # Reorg simulation: a second block_version (1) for the block-1000 event with
            # corrected balance. The query must return only this version, not version 0.
            await conn.execute(
                text(
                    "INSERT INTO allocation_position "
                    "(chain_id, token_id, prime_id, proxy_address, balance, "
                    "block_number, block_version, tx_hash, log_index, tx_amount, direction) "
                    "VALUES (1, :tid, :prime_id, decode(:proxy, 'hex'), 999, 1000, 1, decode(:tx, 'hex'), 0, 999, 'in')"
                ),
                {"tid": token_id, "prime_id": spark_id, "proxy": _PROXY_HEX, "tx": _TX1_HEX},
            )

            # One row for a second prime ('grove') at block 1000 with its own proxy address.
            await conn.execute(
                text(
                    "INSERT INTO allocation_position "
                    "(chain_id, token_id, prime_id, proxy_address, balance, "
                    "block_number, tx_hash, log_index, tx_amount, direction) "
                    "VALUES (1, :tid, :prime_id, decode(:proxy, 'hex'), 100, 1000, decode(:tx, 'hex'), 0, 100, 'in')"
                ),
                {"tid": token_id, "prime_id": grove_id, "proxy": _GROVE_PROXY_HEX, "tx": "cc" * 32},
            )
    finally:
        await engine.dispose()


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed test data into the module's isolated database and yield the async URL."""
    asyncio.run(_seed(module_db["async_url"]))
    return module_db["async_url"]


@pytest.fixture()
def client(async_db_url: str):
    """Return a TestClient wired to the testcontainer database."""
    test_app = create_app(Settings(database_url=SecretStr(async_db_url)))
    with TestClient(test_app) as c:
        yield c


def test_list_primes_returns_both_primes(client: TestClient) -> None:
    response = client.get("/v1/primes")

    assert response.status_code == 200
    data = response.json()
    by_name = {item["name"]: item for item in data}
    assert set(by_name.keys()) == {"spark", "grove"}
    # Each prime exposes id and address as its proxy address.
    assert by_name["spark"]["id"] == f"0x{_PROXY_HEX}"
    assert by_name["spark"]["address"] == f"0x{_PROXY_HEX}"
    assert by_name["grove"]["id"] == f"0x{_GROVE_PROXY_HEX}"
    assert by_name["grove"]["address"] == f"0x{_GROVE_PROXY_HEX}"


def test_list_allocations_without_block_number_returns_latest(client: TestClient) -> None:
    response = client.get(f"/v1/primes/0x{_PROXY_HEX}/allocations")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["block_number"] == 2000
    assert data[0]["name"] == "spark"
    assert data[0]["token_symbol"] == "USDC"
    assert data[0]["token_decimals"] == 6
    assert data[0]["proxy_address"] == f"0x{_PROXY_HEX}"
    assert data[0]["token_address"] == f"0x{_TOKEN_HEX}"


def test_list_allocations_with_block_number_returns_that_block(client: TestClient) -> None:
    response = client.get(f"/v1/primes/0x{_PROXY_HEX}/allocations?block_number=1000")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["block_number"] == 1000


def test_list_allocations_with_unknown_block_number_returns_empty(client: TestClient) -> None:
    response = client.get(f"/v1/primes/0x{_PROXY_HEX}/allocations?block_number=9999")

    assert response.status_code == 200
    assert response.json() == []


def test_list_allocations_for_unknown_prime_returns_empty(client: TestClient) -> None:
    response = client.get(f"/v1/primes/0x{_UNKNOWN_PROXY_HEX}/allocations")

    assert response.status_code == 200
    assert response.json() == []


def test_list_allocations_returns_latest_block_version_after_reorg(client: TestClient) -> None:
    """When two block_version rows exist for the same event the latest version wins."""
    response = client.get(f"/v1/primes/0x{_PROXY_HEX}/allocations?block_number=1000")

    assert response.status_code == 200
    data = response.json()
    # The reorg fixture inserted block_version=0 and block_version=1; only version 1 is canon.
    assert len(data) == 1
    assert data[0]["block_version"] == 1
    assert data[0]["balance"] == "999"


def test_get_prime_via_receipt_tokens(client: TestClient) -> None:
    """Regression: get_prime must not use SELECT DISTINCT with ORDER BY on a non-selected column.

    The original query used ``SELECT DISTINCT ... ORDER BY block_number DESC``
    which PostgreSQL rejects because block_number is not in the select list.
    This test exercises that code path (receipt-tokens calls get_prime internally).
    """
    response = client.get(f"/v1/primes/0x{_PROXY_HEX}/receipt-tokens")

    # The prime exists so we should not get a 500 (SQL error) or 404.
    # An empty list is fine — the important thing is the query executes.
    assert response.status_code == 200


def test_get_prime_returns_404_for_unknown_prime(client: TestClient) -> None:
    """receipt-tokens endpoint returns 404 when the prime doesn't exist."""
    response = client.get(f"/v1/primes/0x{_UNKNOWN_PROXY_HEX}/receipt-tokens")

    assert response.status_code == 404


def test_list_allocations_returns_422_for_malformed_prime_id(client: TestClient) -> None:
    """A prime_id that is not a valid Ethereum address should be rejected at the API boundary."""
    response = client.get("/v1/primes/0xdeadbeef/allocations")

    assert response.status_code == 422
