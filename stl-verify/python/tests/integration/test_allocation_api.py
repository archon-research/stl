"""Integration tests for the allocation API endpoints.

Spins up a real TimescaleDB container, applies all shared migration files to get
the real production schema, seeds fixture data, then hits the FastAPI app via
TestClient to verify end-to-end behaviour.
"""

import asyncio
import pathlib

import asyncpg
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.postgres import PostgresContainer

from app.adapters.postgres.allocation_repository import PostgresAllocationRepository
from app.api.v1 import allocations
from app.main import app
from app.services.allocation_service import AllocationService

MIGRATIONS_DIR = pathlib.Path(__file__).parents[4] / "db" / "migrations"

# Hex bytes used in assertions (20-byte proxy, 20-byte token addr, 32-byte tx hash).
_PROXY_HEX = "1234567890abcdef1234567890abcdef12345678"
_GROVE_PROXY_HEX = "abcdef1234567890abcdef1234567890abcdef12"
_UNKNOWN_PROXY_HEX = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
_TOKEN_HEX = "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
_TX1_HEX = "aa" * 32
_TX2_HEX = "bb" * 32


async def _run_migrations(dsn: str) -> None:
    """Execute every migration file in filename order using a native asyncpg
    connection, which supports multi-statement SQL strings."""
    conn = await asyncpg.connect(dsn)
    try:
        for sql_file in sorted(MIGRATIONS_DIR.glob("*.sql")):
            await conn.execute(sql_file.read_text())
    finally:
        await conn.close()


async def _seed(async_url: str) -> None:
    """Insert fixture rows. Chain data is already present from the migrations."""
    engine = create_async_engine(async_url)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(
                text(
                    "INSERT INTO token (chain_id, address, symbol, decimals) "
                    "VALUES (1, decode(:addr, 'hex'), 'USDC', 6) RETURNING id"
                ),
                {"addr": _TOKEN_HEX},
            )
            token_id = result.scalar_one()

            # Two rows for 'spark' at different block numbers (proxy = _PROXY_HEX).
            for block, tx_hex in [(1000, _TX1_HEX), (2000, _TX2_HEX)]:
                await conn.execute(
                    text(
                        "INSERT INTO allocation_position "
                        "(chain_id, token_id, star, proxy_address, balance, "
                        "block_number, tx_hash, log_index, tx_amount, direction) "
                        "VALUES (1, :tid, 'spark', decode(:proxy, 'hex'), 500, :bn, decode(:tx, 'hex'), 0, 500, 'in')"
                    ),
                    {"tid": token_id, "proxy": _PROXY_HEX, "bn": block, "tx": tx_hex},
                )

            # One row for a second star ('grove') at block 1000 with its own proxy address.
            await conn.execute(
                text(
                    "INSERT INTO allocation_position "
                    "(chain_id, token_id, star, proxy_address, balance, "
                    "block_number, tx_hash, log_index, tx_amount, direction) "
                    "VALUES (1, :tid, 'grove', decode(:proxy, 'hex'), 100, 1000, decode(:tx, 'hex'), 0, 100, 'in')"
                ),
                {"tid": token_id, "proxy": _GROVE_PROXY_HEX, "tx": "cc" * 32},
            )
    finally:
        await engine.dispose()


@pytest.fixture(scope="module")
def async_db_url():
    """Start a TimescaleDB container, run all migrations, seed data, yield the asyncpg URL."""
    with PostgresContainer("timescale/timescaledb:latest-pg16") as container:
        async_url = container.get_connection_url(driver="asyncpg")
        dsn = container.get_connection_url()
        asyncio.run(_run_migrations(dsn))
        asyncio.run(_seed(async_url))
        yield async_url


@pytest.fixture()
def client(async_db_url: str):
    """Return a TestClient wired to the test database via a dependency override."""

    async def _service_override():
        engine = create_async_engine(async_db_url)
        async with engine.connect() as conn:
            repo = PostgresAllocationRepository(conn)
            yield AllocationService(repo)
        await engine.dispose()

    app.dependency_overrides[allocations._get_service] = _service_override
    with TestClient(app) as c:
        yield c
    # clear_dependency_overrides (autouse in tests/conftest.py) clears overrides after each test


def test_list_stars_returns_both_stars(client: TestClient) -> None:
    response = client.get("/v1/stars")

    assert response.status_code == 200
    data = response.json()
    by_name = {item["name"]: item for item in data}
    assert set(by_name.keys()) == {"spark", "grove"}
    # Each star exposes id and address as its proxy address.
    assert by_name["spark"]["id"] == f"0x{_PROXY_HEX}"
    assert by_name["spark"]["address"] == f"0x{_PROXY_HEX}"
    assert by_name["grove"]["id"] == f"0x{_GROVE_PROXY_HEX}"
    assert by_name["grove"]["address"] == f"0x{_GROVE_PROXY_HEX}"


def test_list_allocations_without_block_number_returns_latest(client: TestClient) -> None:
    response = client.get(f"/v1/stars/0x{_PROXY_HEX}/allocations")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["block_number"] == 2000
    assert data[0]["star"] == "spark"
    assert data[0]["token_symbol"] == "USDC"
    assert data[0]["token_decimals"] == 6
    assert data[0]["proxy_address"] == f"0x{_PROXY_HEX}"
    assert data[0]["token_address"] == f"0x{_TOKEN_HEX}"


def test_list_allocations_with_block_number_returns_that_block(client: TestClient) -> None:
    response = client.get(f"/v1/stars/0x{_PROXY_HEX}/allocations?block_number=1000")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["block_number"] == 1000


def test_list_allocations_with_unknown_block_number_returns_empty(client: TestClient) -> None:
    response = client.get(f"/v1/stars/0x{_PROXY_HEX}/allocations?block_number=9999")

    assert response.status_code == 200
    assert response.json() == []


def test_list_allocations_for_unknown_star_returns_empty(client: TestClient) -> None:
    response = client.get(f"/v1/stars/0x{_UNKNOWN_PROXY_HEX}/allocations")

    assert response.status_code == 200
    assert response.json() == []
