"""Integration tests for the allocation API endpoints.

Uses a shared TimescaleDB container (session-scoped) with an isolated database
per test module.  Migrations are applied by the ``module_db`` fixture from
``conftest.py``; this file only seeds test-specific data.
"""

import asyncio
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import Settings
from app.main import create_app

# Prime proxy addresses (20 bytes).
_SPARK_PROXY_HEX = "1234567890abcdef1234567890abcdef12345678"
_GROVE_PROXY_HEX = "abcdef1234567890abcdef1234567890abcdef12"
_OBEX_PROXY_HEX = "fedcba9876543210fedcba9876543210fedcba98"
_UNKNOWN_PROXY_HEX = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"

# Underlying tokens seeded by the sparklend migration (all chain_id=1).
_USDC_HEX = "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
_WETH_HEX = "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
_GNO_HEX = "6810e776880c02933d47db1b9fc05908e5386b96"

# Receipt-token addresses (Aave V3) — arbitrary bytes for test purposes.
_AUSDC_HEX = "98c23e9d8f34fefb1b7bd6a91b7ff122f4e16f5c"
_AWETH_HEX = "4d5f47fa6a74757f35c14fd3a6ef8e3c9bc514e8"

# Transaction hashes (32 bytes).
_TX1_HEX = "aa" * 32
_TX2_HEX = "bb" * 32
_TX3_HEX = "cc" * 32
_TX4_HEX = "dd" * 32
_TX5_HEX = "ee" * 32


async def _seed(async_url: str) -> None:
    """Insert fixture rows. Protocol/token/prime data is already present from the migrations."""
    engine = create_async_engine(async_url)
    try:
        async with engine.begin() as conn:
            # Look up the IDs that the migrations populated.
            usdc_id = (
                await conn.execute(
                    text("SELECT id FROM token WHERE chain_id = 1 AND address = decode(:addr, 'hex')"),
                    {"addr": _USDC_HEX},
                )
            ).scalar_one()
            weth_id = (
                await conn.execute(
                    text("SELECT id FROM token WHERE chain_id = 1 AND address = decode(:addr, 'hex')"),
                    {"addr": _WETH_HEX},
                )
            ).scalar_one()
            gno_id = (
                await conn.execute(
                    text("SELECT id FROM token WHERE chain_id = 1 AND address = decode(:addr, 'hex')"),
                    {"addr": _GNO_HEX},
                )
            ).scalar_one()
            aave_id = (await conn.execute(text("SELECT id FROM protocol WHERE name = 'Aave V3'"))).scalar_one()
            spark_id = (await conn.execute(text("SELECT id FROM prime WHERE name = 'spark'"))).scalar_one()
            grove_id = (await conn.execute(text("SELECT id FROM prime WHERE name = 'grove'"))).scalar_one()
            obex_id = (await conn.execute(text("SELECT id FROM prime WHERE name = 'obex'"))).scalar_one()

            # Register aUSDC / aWETH as tokens so allocation_position can reference them.
            await conn.execute(
                text(
                    "INSERT INTO token (chain_id, address, symbol, decimals) "
                    "VALUES (1, decode(:addr, 'hex'), 'aUSDC', 6)"
                ),
                {"addr": _AUSDC_HEX},
            )
            ausdc_token_id = (
                await conn.execute(
                    text("SELECT id FROM token WHERE chain_id = 1 AND address = decode(:addr, 'hex')"),
                    {"addr": _AUSDC_HEX},
                )
            ).scalar_one()

            await conn.execute(
                text(
                    "INSERT INTO token (chain_id, address, symbol, decimals) "
                    "VALUES (1, decode(:addr, 'hex'), 'aWETH', 18)"
                ),
                {"addr": _AWETH_HEX},
            )
            aweth_token_id = (
                await conn.execute(
                    text("SELECT id FROM token WHERE chain_id = 1 AND address = decode(:addr, 'hex')"),
                    {"addr": _AWETH_HEX},
                )
            ).scalar_one()

            # receipt_token rows map (underlying, protocol) -> receipt token.
            await conn.execute(
                text(
                    "INSERT INTO receipt_token "
                    "(chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol) "
                    "VALUES (1, :pid, :uid, decode(:addr, 'hex'), 'aUSDC')"
                ),
                {"pid": aave_id, "uid": usdc_id, "addr": _AUSDC_HEX},
            )
            await conn.execute(
                text(
                    "INSERT INTO receipt_token "
                    "(chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol) "
                    "VALUES (1, :pid, :uid, decode(:addr, 'hex'), 'aWETH')"
                ),
                {"pid": aave_id, "uid": weth_id, "addr": _AWETH_HEX},
            )

            # spark holds aUSDC: an earlier balance (block 1000) and the latest
            # (block 2000). A block_version=1 row at block 1000 simulates a
            # reorg correction — superseded by block 2000 which wins.
            for bn, bv, bal, tx in [
                (1000, 0, 300, _TX1_HEX),
                (1000, 1, 999, _TX1_HEX),
                (2000, 0, 750, _TX2_HEX),
            ]:
                await conn.execute(
                    text(
                        "INSERT INTO allocation_position "
                        "(chain_id, token_id, prime_id, proxy_address, balance, "
                        "block_number, block_version, tx_hash, log_index, tx_amount, direction) "
                        "VALUES (1, :tid, :pid, decode(:proxy, 'hex'), :bal, :bn, :bv, "
                        "decode(:tx, 'hex'), 0, :bal, 'in')"
                    ),
                    {
                        "tid": ausdc_token_id,
                        "pid": spark_id,
                        "proxy": _SPARK_PROXY_HEX,
                        "bal": bal,
                        "bn": bn,
                        "bv": bv,
                        "tx": tx,
                    },
                )

            # spark also holds aWETH at block 2000.
            await conn.execute(
                text(
                    "INSERT INTO allocation_position "
                    "(chain_id, token_id, prime_id, proxy_address, balance, "
                    "block_number, tx_hash, log_index, tx_amount, direction) "
                    "VALUES (1, :tid, :pid, decode(:proxy, 'hex'), 100, 2000, decode(:tx, 'hex'), 0, 100, 'in')"
                ),
                {"tid": aweth_token_id, "pid": spark_id, "proxy": _SPARK_PROXY_HEX, "tx": _TX3_HEX},
            )

            # grove holds raw GNO. GNO has no receipt_token row, so the query
            # returns no receipt-token holdings for grove.
            await conn.execute(
                text(
                    "INSERT INTO allocation_position "
                    "(chain_id, token_id, prime_id, proxy_address, balance, "
                    "block_number, tx_hash, log_index, tx_amount, direction) "
                    "VALUES (1, :tid, :pid, decode(:proxy, 'hex'), 50, 1000, decode(:tx, 'hex'), 0, 50, 'in')"
                ),
                {"tid": gno_id, "pid": grove_id, "proxy": _GROVE_PROXY_HEX, "tx": _TX4_HEX},
            )

            # obex holds raw USDC. This exercises branch 1 of the receipt-token
            # SQL (``rt.underlying_token_id = t.id``): a position recorded in
            # the underlying token is mapped onto every receipt token whose
            # underlying is that token. Today that surfaces aUSDC — a single
            # holding, which also covers the single-holding acceptance case.
            # If a second USDC-underlying receipt_token is ever seeded, this
            # will fan out (see T2 review issue #2).
            await conn.execute(
                text(
                    "INSERT INTO allocation_position "
                    "(chain_id, token_id, prime_id, proxy_address, balance, "
                    "block_number, tx_hash, log_index, tx_amount, direction) "
                    "VALUES (1, :tid, :pid, decode(:proxy, 'hex'), 250, 1500, decode(:tx, 'hex'), 0, 250, 'in')"
                ),
                {"tid": usdc_id, "pid": obex_id, "proxy": _OBEX_PROXY_HEX, "tx": _TX5_HEX},
            )
    finally:
        await engine.dispose()


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed test data into the module's isolated database and yield the async URL."""
    asyncio.run(_seed(module_db["async_url"]))
    return module_db["async_url"]


@pytest.fixture()
def client(async_db_url: str, tmp_path: Path):
    """Return a TestClient wired to the testcontainer database."""
    empty_mapping = tmp_path / "empty_mapping.json"
    empty_mapping.write_text("{}")
    test_app = create_app(Settings(database_url=SecretStr(async_db_url), suraf_mappings_file=empty_mapping))
    with TestClient(test_app) as c:
        yield c


def test_list_primes_returns_seeded_primes(client: TestClient) -> None:
    response = client.get("/v1/primes")

    assert response.status_code == 200
    data = response.json()
    by_name = {item["name"]: item for item in data}
    assert set(by_name.keys()) == {"spark", "grove", "obex"}
    assert by_name["spark"]["id"] == f"0x{_SPARK_PROXY_HEX}"
    assert by_name["spark"]["address"] == f"0x{_SPARK_PROXY_HEX}"
    assert by_name["grove"]["id"] == f"0x{_GROVE_PROXY_HEX}"
    assert by_name["grove"]["address"] == f"0x{_GROVE_PROXY_HEX}"
    assert by_name["obex"]["id"] == f"0x{_OBEX_PROXY_HEX}"
    assert by_name["obex"]["address"] == f"0x{_OBEX_PROXY_HEX}"


def test_list_allocations_returns_multiple_holdings_for_prime(client: TestClient) -> None:
    """spark holds both aUSDC and aWETH, enriched with underlying token info."""
    response = client.get(f"/v1/primes/0x{_SPARK_PROXY_HEX}/allocations")

    assert response.status_code == 200
    data = response.json()
    by_symbol = {item["symbol"]: item for item in data}
    assert set(by_symbol.keys()) == {"aUSDC", "aWETH"}

    ausdc = by_symbol["aUSDC"]
    # block 2000 is the latest event for aUSDC — its balance wins over the
    # earlier (block 1000) rows, including the block_version=1 reorg fixture.
    assert ausdc["chain_id"] == 1
    assert ausdc["balance"] == "750"
    assert ausdc["receipt_token_address"] == f"0x{_AUSDC_HEX}"
    assert ausdc["underlying_token_address"] == f"0x{_USDC_HEX}"
    assert ausdc["underlying_symbol"] == "USDC"
    assert ausdc["protocol_name"] == "Aave V3"
    assert isinstance(ausdc["receipt_token_id"], int)
    assert isinstance(ausdc["underlying_token_id"], int)

    aweth = by_symbol["aWETH"]
    assert aweth["chain_id"] == 1
    assert aweth["balance"] == "100"
    assert aweth["receipt_token_address"] == f"0x{_AWETH_HEX}"
    assert aweth["underlying_token_address"] == f"0x{_WETH_HEX}"
    assert aweth["underlying_symbol"] == "WETH"
    assert aweth["protocol_name"] == "Aave V3"


def test_list_allocations_returns_single_holding_via_underlying_branch(
    client: TestClient,
) -> None:
    """obex holds raw USDC — exercises the ``rt.underlying_token_id = t.id``
    branch of the receipt-token SQL, which maps a position in the underlying
    onto every receipt token whose underlying is that token. Today this
    surfaces aUSDC (one row). Doubles as the single-holding case required
    by T2's acceptance criteria.
    """
    response = client.get(f"/v1/primes/0x{_OBEX_PROXY_HEX}/allocations")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1

    ausdc = data[0]
    assert ausdc["chain_id"] == 1
    assert ausdc["symbol"] == "aUSDC"
    assert ausdc["balance"] == "250"
    assert ausdc["receipt_token_address"] == f"0x{_AUSDC_HEX}"
    assert ausdc["underlying_token_address"] == f"0x{_USDC_HEX}"
    assert ausdc["underlying_symbol"] == "USDC"
    assert ausdc["protocol_name"] == "Aave V3"


def test_list_allocations_returns_empty_when_prime_has_no_receipt_token_holdings(
    client: TestClient,
) -> None:
    """grove holds only GNO, which has no receipt_token mapping."""
    response = client.get(f"/v1/primes/0x{_GROVE_PROXY_HEX}/allocations")

    assert response.status_code == 200
    assert response.json() == []


def test_list_allocations_returns_empty_for_unknown_prime(client: TestClient) -> None:
    response = client.get(f"/v1/primes/0x{_UNKNOWN_PROXY_HEX}/allocations")

    assert response.status_code == 200
    assert response.json() == []


def test_list_allocations_returns_422_for_malformed_prime_id(client: TestClient) -> None:
    """A prime_id that is not a valid Ethereum address should be rejected at the API boundary."""
    response = client.get("/v1/primes/0xdeadbeef/allocations")

    assert response.status_code == 422
