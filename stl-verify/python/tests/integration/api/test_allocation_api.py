"""Integration tests for the allocation API endpoints.

Uses a shared TimescaleDB container (session-scoped) with an isolated database
per test module.  Migrations are applied by the ``module_db`` fixture from
``conftest.py``; this file only seeds test-specific data.
"""

import asyncio
from pathlib import Path

import asyncpg
import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.config import Settings
from app.main import create_app
from tests.integration.seed import (
    GHOST_CLOSED_PROXY_HEX,
    GHOST_MIXED_PROXY_HEX,
    GHOST_OPEN_PROXY_HEX,
    GHOST_SWEEP_PROXY_HEX,
    GHOST_TIEBREAK_PROXY_HEX,
    insert_allocation_position,
    insert_token,
    seed_ghost_balance,
)

# Prime proxy addresses (20 bytes).
_SPARK_PROXY_HEX = "1234567890abcdef1234567890abcdef12345678"
_GROVE_PROXY_HEX = "abcdef1234567890abcdef1234567890abcdef12"
_OBEX_PROXY_HEX = "fedcba9876543210fedcba9876543210fedcba98"
_UNKNOWN_PROXY_HEX = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
# Real Spark SubProxy address — used to exercise the ALM-only filter in
# /v1/primes. Must match app.domain.proxy_kind._SUB_PROXY_HEX.
_SPARK_SUB_PROXY_HEX = "3300f198988e4c9c63f75df86de36421f06af8c4"

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


async def _token_id_by_address(conn: asyncpg.Connection, addr_hex: str) -> int:
    """Return the id of the chain-1 token at *addr_hex* (seeded by the migrations)."""
    return await conn.fetchval(
        "SELECT id FROM token WHERE chain_id = 1 AND address = $1",
        bytes.fromhex(addr_hex),
    )


async def _seed(db_url: str) -> None:
    """Insert fixture rows. Protocol/token/prime data is already present from the migrations."""
    conn = await asyncpg.connect(db_url)
    try:
        async with conn.transaction():
            # Look up the IDs that the migrations populated.
            usdc_id = await _token_id_by_address(conn, _USDC_HEX)
            weth_id = await _token_id_by_address(conn, _WETH_HEX)
            gno_id = await _token_id_by_address(conn, _GNO_HEX)
            aave_id = await conn.fetchval("SELECT id FROM protocol WHERE name = 'Aave V3'")
            spark_id = await conn.fetchval("SELECT id FROM prime WHERE name = 'spark'")
            grove_id = await conn.fetchval("SELECT id FROM prime WHERE name = 'grove'")
            obex_id = await conn.fetchval("SELECT id FROM prime WHERE name = 'obex'")

            # Register aUSDC / aWETH as tokens so allocation_position can reference them.
            ausdc_token_id = await insert_token(conn, "aUSDC", 6, bytes.fromhex(_AUSDC_HEX))
            aweth_token_id = await insert_token(conn, "aWETH", 18, bytes.fromhex(_AWETH_HEX))

            # receipt_token rows map (underlying, protocol) -> receipt token.
            await conn.execute(
                "INSERT INTO receipt_token "
                "(chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol) "
                "VALUES (1, $1, $2, $3, 'aUSDC')",
                aave_id,
                usdc_id,
                bytes.fromhex(_AUSDC_HEX),
            )
            await conn.execute(
                "INSERT INTO receipt_token "
                "(chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol) "
                "VALUES (1, $1, $2, $3, 'aWETH')",
                aave_id,
                weth_id,
                bytes.fromhex(_AWETH_HEX),
            )

            # spark holds aUSDC: an earlier balance (block 1000) and the latest
            # (block 2000). A block_version=1 row at block 1000 simulates a
            # reorg correction — superseded by block 2000 which wins.
            for bn, bv, bal, tx in [
                (1000, 0, 300, _TX1_HEX),
                (1000, 1, 999, _TX1_HEX),
                (2000, 0, 750, _TX2_HEX),
            ]:
                await insert_allocation_position(
                    conn,
                    token_id=ausdc_token_id,
                    prime_id=spark_id,
                    proxy_hex=_SPARK_PROXY_HEX,
                    balance=bal,
                    block=bn,
                    tx=tx,
                    direction="in",
                    block_version=bv,
                )

            # spark also holds aWETH at block 2000.
            await insert_allocation_position(
                conn,
                token_id=aweth_token_id,
                prime_id=spark_id,
                proxy_hex=_SPARK_PROXY_HEX,
                balance=100,
                block=2000,
                tx=_TX3_HEX,
                direction="in",
            )

            # grove holds raw GNO. GNO has no receipt_token row, so the query
            # returns no receipt-token holdings for grove.
            await insert_allocation_position(
                conn,
                token_id=gno_id,
                prime_id=grove_id,
                proxy_hex=_GROVE_PROXY_HEX,
                balance=50,
                block=1000,
                tx=_TX4_HEX,
                direction="in",
            )

            # Spark also has an entry under its SubProxy wallet (risk capital).
            # /v1/primes must NOT surface this as a separate prime — it shares
            # spark_id with the ALM proxy above.
            await insert_allocation_position(
                conn,
                token_id=ausdc_token_id,
                prime_id=spark_id,
                proxy_hex=_SPARK_SUB_PROXY_HEX,
                balance=42,
                block=2000,
                tx=_TX2_HEX,
                direction="in",
                log_index=1,
            )

            # obex holds raw USDC directly (not wrapped in any receipt token).
            # The endpoint should not surface this under aUSDC or any other
            # receipt token whose underlying is USDC: a direct underlying
            # holding is its own asset, not a position in a wrapper.
            await insert_allocation_position(
                conn,
                token_id=usdc_id,
                prime_id=obex_id,
                proxy_hex=_OBEX_PROXY_HEX,
                balance=250,
                block=1500,
                tx=_TX5_HEX,
                direction="in",
            )
    finally:
        await conn.close()


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed test data into the module's isolated database and yield the async URL.

    Loads both this module's allocation fixtures and the shared ghost-balance
    regression rows (their own ``ghost_balance`` prime, disjoint addresses).
    """
    asyncio.run(_seed(module_db["db_url"]))
    asyncio.run(seed_ghost_balance(module_db["db_url"]))
    return module_db["async_url"]


@pytest.fixture()
def client(async_db_url: str, tmp_path: Path):
    """Return a TestClient wired to the testcontainer database."""
    empty_mapping = tmp_path / "empty_mapping.json"
    empty_mapping.write_text("{}")
    test_app = create_app(
        Settings.model_validate({"database_url": SecretStr(async_db_url), "suraf_mappings_file": empty_mapping})
    )
    with TestClient(test_app) as c:
        yield c


def test_list_primes_returns_seeded_primes(client: TestClient) -> None:
    response = client.get("/v1/primes")

    assert response.status_code == 200
    data = response.json()
    # Assert presence of the primes this test seeds rather than an exact global
    # count: other scenarios in this module (ghost-balance) add their own ALM
    # proxies to the same database, and a "list everything" endpoint returns
    # them all.
    by_name = {item["name"]: item for item in data}
    assert by_name["spark"]["id"] == f"0x{_SPARK_PROXY_HEX}"
    assert by_name["spark"]["address"] == f"0x{_SPARK_PROXY_HEX}"
    assert by_name["grove"]["id"] == f"0x{_GROVE_PROXY_HEX}"
    assert by_name["grove"]["address"] == f"0x{_GROVE_PROXY_HEX}"
    assert by_name["obex"]["id"] == f"0x{_OBEX_PROXY_HEX}"
    assert by_name["obex"]["address"] == f"0x{_OBEX_PROXY_HEX}"
    # SubProxy rows (e.g. _SPARK_SUB_PROXY_HEX) share spark_id and must be
    # filtered out — only the ALM proxy per prime should appear.
    addresses = {item["address"] for item in data}
    assert f"0x{_SPARK_SUB_PROXY_HEX}" not in addresses


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


def test_direct_underlying_holdings_surface_as_their_own_rows(
    client: TestClient,
) -> None:
    """A prime holds raw USDC directly. It must not be attributed to any
    USDC-wrapping receipt token (that would double-count and fan out), but
    it should appear as a direct-asset row with null receipt_token fields
    and ASSET category.
    """
    response = client.get(f"/v1/primes/0x{_OBEX_PROXY_HEX}/allocations")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    row = data[0]
    assert row["symbol"] == "USDC"
    assert row["underlying_symbol"] == "USDC"
    assert row["receipt_token_id"] is None
    assert row["receipt_token_address"] is None
    assert row["protocol_name"] is None
    assert row["underlying_token_address"] == f"0x{_USDC_HEX}"
    assert row["balance"] == "250"
    assert row["category"] == "asset"


def test_list_allocations_returns_only_direct_row_when_no_receipt_tokens(
    client: TestClient,
) -> None:
    """A prime holds only GNO. There is no receipt_token wrapping GNO, so the
    response contains exactly one direct-asset row and no receipt-token rows.
    """
    response = client.get(f"/v1/primes/0x{_GROVE_PROXY_HEX}/allocations")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    row = data[0]
    assert row["symbol"] == "GNO"
    assert row["receipt_token_id"] is None
    assert row["protocol_name"] is None
    assert row["category"] == "asset"


def test_list_allocations_returns_404_for_unknown_prime(client: TestClient) -> None:
    """A well-formed address with no allocation_position history is not a
    registered prime: the endpoint signals this with 404 rather than an
    ambiguous empty list.
    """
    response = client.get(f"/v1/primes/0x{_UNKNOWN_PROXY_HEX}/allocations")

    assert response.status_code == 404
    assert response.json()["detail"] == "Prime not found"


def test_list_allocations_returns_422_for_malformed_prime_id(client: TestClient) -> None:
    """A prime_id that is not a valid Ethereum address should be rejected at the API boundary."""
    response = client.get("/v1/primes/0xdeadbeef/allocations")

    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Ghost-balance regression: positions whose latest balance is zero must not
# resurface their last non-zero balance.  Seed shape lives in
# ``conftest.seed_ghost_balance``.
# ---------------------------------------------------------------------------


def test_closed_receipt_token_absent_from_allocations_endpoint(client: TestClient) -> None:
    """A position closed to 0 (latest balance=0) must not appear in /allocations."""
    response = client.get(f"/v1/primes/0x{GHOST_CLOSED_PROXY_HEX}/allocations")

    assert response.status_code == 200
    symbols = {row["symbol"] for row in response.json()}
    assert "aSyrupUSDT" not in symbols, "ghost receipt-token position surfaced"
    assert "USDS" not in symbols, "ghost direct-asset position surfaced"


def test_closed_direct_asset_absent_from_allocations_endpoint(client: TestClient) -> None:
    """A direct asset swept to 0 must not appear."""
    response = client.get(f"/v1/primes/0x{GHOST_CLOSED_PROXY_HEX}/allocations")

    assert response.status_code == 200
    rows = response.json()
    direct_symbols = {r["symbol"] for r in rows if r.get("category") == "asset"}
    assert "USDS" not in direct_symbols, "ghost direct-asset holding surfaced"


def test_swept_position_absent_from_allocations_endpoint(client: TestClient) -> None:
    """Zero-balance sweep rows newer than the last non-zero row close the position."""
    response = client.get(f"/v1/primes/0x{GHOST_SWEEP_PROXY_HEX}/allocations")

    assert response.status_code == 200
    symbols = {row["symbol"] for row in response.json()}
    assert "aSyrupUSDT" not in symbols, "ghost balance surfaced for swept position"


def test_open_position_with_older_zero_rows_still_returned(client: TestClient) -> None:
    """An open position whose history contains earlier zero rows must still appear."""
    response = client.get(f"/v1/primes/0x{GHOST_OPEN_PROXY_HEX}/allocations")

    assert response.status_code == 200
    by_symbol = {row["symbol"]: row for row in response.json()}
    assert "aSyrupUSDT" in by_symbol, "open position incorrectly filtered out"
    assert by_symbol["aSyrupUSDT"]["balance"] == "500"


def test_mixed_proxy_drops_only_swept_token(client: TestClient) -> None:
    """Filtering is per token: the swept holding disappears, the open one stays."""
    response = client.get(f"/v1/primes/0x{GHOST_MIXED_PROXY_HEX}/allocations")

    assert response.status_code == 200
    by_symbol = {row["symbol"]: row for row in response.json()}
    assert "aSyrupUSDT" not in by_symbol, "swept receipt-token position surfaced on mixed proxy"
    assert "USDS" in by_symbol, "open direct holding incorrectly filtered out on mixed proxy"
    assert by_symbol["USDS"]["balance"] == "1000"


def test_same_block_log_index_tiebreak_decides_latest_row(client: TestClient) -> None:
    """Within a single block, log_index ordering decides which row is latest."""
    response = client.get(f"/v1/primes/0x{GHOST_TIEBREAK_PROXY_HEX}/allocations")

    assert response.status_code == 200
    by_symbol = {row["symbol"]: row for row in response.json()}
    assert "aSyrupUSDT" not in by_symbol, "position zeroed within its final block surfaced"
    assert "USDS" in by_symbol, "position opened within the same block incorrectly filtered out"
    assert by_symbol["USDS"]["balance"] == "400"
