"""Integration tests for the allocation API endpoints.

Uses a shared TimescaleDB container (session-scoped) with an isolated database
per test module.  Migrations are applied by the ``module_db`` fixture from
``conftest.py``; this file only seeds test-specific data.
"""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
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

# Flow-reconstruction (net_flow_usd) fixture: a dedicated prime whose aUSDC
# activity exercises the signed-flow valuation. USDC is priced at 1 USD (seeded
# in ``_seed``), so net_flow_usd equals the signed token magnitude.
_FLOW_PRIME_VAULT_HEX = "f1" * 20
_FLOW_PROXY_HEX = "f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0"
_FLOW_TX_IN = "1a" * 32
_FLOW_TX_OUT = "1b" * 32
_FLOW_TX_SWEEP = "1c" * 32
# All three events fall in the single 00:00 PT1H bucket.
_FLOW_BUCKET_TS = datetime(2026, 1, 1, 0, 30, tzinfo=UTC)

# Direct-asset flow fixture: a prime that holds raw USDC (no receipt-token
# wrapper). Its flow must NOT contribute to net_flow_usd: direct underlying
# tokens record outflows mostly as sweeps (excluded) while inflows are 'in', so
# valuing them overstates net flow as gross inflow throughput. Only receipt-token
# flows drive the reconstructed balance series.
_DIRECT_FLOW_PRIME_VAULT_HEX = "f2" * 20
_DIRECT_FLOW_PROXY_HEX = "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1"
_DIRECT_FLOW_TX_IN = "2a" * 32
_DIRECT_FLOW_TX_OUT = "2b" * 32

# Total-capital LOCF fixture: the Spark SubProxy holds treasury USDS, observed
# at two timestamps two hours apart, so gap-fill carry-forward and the
# leading-gap null are observable.
_USDS_HEX = "dc035d45d973e3ec169d2276ddab16f1e407384f"
_CAP_SNAP1_TS = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
_CAP_SNAP2_TS = datetime(2026, 1, 1, 2, 0, tzinfo=UTC)


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

            # USDC has an oracle price, so obex's direct holding is valued in USD.
            # GNO (grove's direct holding) is deliberately left unpriced to assert
            # the null-amount_usd path for tokens with no oracle feed.
            oracle_id = await conn.fetchval("SELECT id FROM oracle WHERE name = 'aave_v3'")
            await conn.execute(
                "INSERT INTO onchain_token_price "
                "(token_id, oracle_id, block_number, block_version, timestamp, price_usd) "
                "VALUES ($1, $2, 1500, 0, NOW(), $3)",
                usdc_id,
                oracle_id,
                Decimal(1),
            )

            # net_flow_usd flow-reconstruction fixture: three aUSDC events in one
            # bucket — +100 in, -40 out, and a 1000 sweep that must net to zero.
            # aUSDC's underlying USDC is priced at 1 USD, so net_flow_usd == 60.
            flow_prime_id = await conn.fetchval(
                "INSERT INTO prime (name, vault_address) VALUES ('flow_test', $1) RETURNING id",
                bytes.fromhex(_FLOW_PRIME_VAULT_HEX),
            )
            for offset, (tx, direction, amount) in enumerate(
                [
                    (_FLOW_TX_IN, "in", 100),
                    (_FLOW_TX_OUT, "out", 40),
                    (_FLOW_TX_SWEEP, "sweep", 1000),
                ]
            ):
                await conn.execute(
                    "INSERT INTO allocation_position "
                    "(chain_id, token_id, prime_id, proxy_address, balance, "
                    "block_number, block_version, tx_hash, log_index, tx_amount, direction, created_at) "
                    "VALUES (1, $1, $2, $3, $4, $5, 0, $6, 0, $4, $7, $8)",
                    ausdc_token_id,
                    flow_prime_id,
                    bytes.fromhex(_FLOW_PROXY_HEX),
                    Decimal(amount),
                    5000 + offset,
                    bytes.fromhex(tx),
                    direction,
                    _FLOW_BUCKET_TS,
                )

            # Direct-asset flow fixture: a prime holding raw USDC (no receipt
            # token). +250 in, -50 out in one bucket. USDC has no receipt-token
            # wrapper, so the flow is excluded from net_flow_usd (== 0) even
            # though the events are still counted.
            direct_flow_prime_id = await conn.fetchval(
                "INSERT INTO prime (name, vault_address) VALUES ('direct_flow_test', $1) RETURNING id",
                bytes.fromhex(_DIRECT_FLOW_PRIME_VAULT_HEX),
            )
            for offset, (tx, direction, amount) in enumerate(
                [
                    (_DIRECT_FLOW_TX_IN, "in", 250),
                    (_DIRECT_FLOW_TX_OUT, "out", 50),
                ]
            ):
                await conn.execute(
                    "INSERT INTO allocation_position "
                    "(chain_id, token_id, prime_id, proxy_address, balance, "
                    "block_number, block_version, tx_hash, log_index, tx_amount, direction, created_at) "
                    "VALUES (1, $1, $2, $3, $4, $5, 0, $6, 0, $4, $7, $8)",
                    usdc_id,
                    direct_flow_prime_id,
                    bytes.fromhex(_DIRECT_FLOW_PROXY_HEX),
                    Decimal(amount),
                    7000 + offset,
                    bytes.fromhex(tx),
                    direction,
                    _FLOW_BUCKET_TS,
                )

            # Total-capital LOCF fixture: the Spark SubProxy holds treasury USDS,
            # observed at two timestamps two hours apart (and sharing spark's
            # prime_id), so the gap bucket carries the earlier value forward and a
            # bucket before the first observation gap-fills to null.
            usds_id = await insert_token(conn, "USDS", 18, bytes.fromhex(_USDS_HEX))
            for created_at, balance, tx, block in [
                (_CAP_SNAP1_TS, 2_000_000, "a1" * 32, 6000),
                (_CAP_SNAP2_TS, 2_100_000, "a2" * 32, 6001),
            ]:
                await conn.execute(
                    "INSERT INTO allocation_position "
                    "(chain_id, token_id, prime_id, proxy_address, balance, "
                    "block_number, block_version, tx_hash, log_index, tx_amount, direction, created_at) "
                    "VALUES (1, $1, $2, $3, $4, $5, 0, $6, 0, $4, 'in', $7)",
                    usds_id,
                    spark_id,
                    bytes.fromhex(_SPARK_SUB_PROXY_HEX),
                    Decimal(balance),
                    block,
                    bytes.fromhex(tx),
                    created_at,
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
    and ASSET category. USDC has an oracle price, so amount_usd is valued
    (balance 250 × 1 USD).
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
    assert Decimal(row["amount_usd"]) == Decimal("250")
    assert row["category"] == "asset"


def test_list_allocations_returns_only_direct_row_when_no_receipt_tokens(
    client: TestClient,
) -> None:
    """A prime holds only GNO. There is no receipt_token wrapping GNO, so the
    response contains exactly one direct-asset row and no receipt-token rows.
    GNO has no oracle price, so amount_usd is null rather than the row being
    dropped.
    """
    response = client.get(f"/v1/primes/0x{_GROVE_PROXY_HEX}/allocations")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    row = data[0]
    assert row["symbol"] == "GNO"
    assert row["receipt_token_id"] is None
    assert row["protocol_name"] is None
    assert row["amount_usd"] is None
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


# ---------------------------------------------------------------------------
# net_flow_usd: the signed flow valuation that drives the UI's reconstructed
# total-allocation balance series. Inflows add, outflows subtract, and sweeps
# (internal position moves) net to zero — a flipped sign here would silently
# invert every reconstructed balance chart.
# ---------------------------------------------------------------------------


def test_activity_buckets_net_flow_is_signed_and_excludes_sweeps(client: TestClient) -> None:
    response = client.get(
        "/v1/allocations/activity",
        params={
            "prime_id": f"0x{_FLOW_PROXY_HEX}",
            "from_timestamp": "2026-01-01T00:00:00Z",
            "to_timestamp": "2026-01-01T01:00:00Z",
            "resolution": "PT1H",
            "aggregate": "true",
        },
    )

    assert response.status_code == 200
    envelope = response.json()
    assert envelope["mode"] == "aggregated"
    buckets = envelope["data"]
    assert len(buckets) == 1
    bucket = buckets[0]
    assert bucket["event_count"] == 3
    # +100 (in) − 40 (out) + 0 (the 1000 sweep is excluded), valued at USDC = 1 USD.
    assert Decimal(str(bucket["net_flow_usd"])) == Decimal("60")
    # total_tx_amount is the unsigned magnitude sum and DOES include the sweep.
    assert Decimal(str(bucket["total_tx_amount"])) == Decimal("1140")


def test_activity_buckets_exclude_direct_asset_flows(client: TestClient) -> None:
    """A flow whose token is held directly (no receipt-token wrapper) contributes
    0 to net_flow_usd. Direct underlying tokens record outflows mostly as sweeps
    (excluded) while inflows are 'in', so pricing them overstates net flow as
    gross inflow throughput and makes the reconstructed balance ramp from zero;
    only receipt-token flows drive the series. The events are still counted."""
    response = client.get(
        "/v1/allocations/activity",
        params={
            "prime_id": f"0x{_DIRECT_FLOW_PROXY_HEX}",
            "from_timestamp": "2026-01-01T00:00:00Z",
            "to_timestamp": "2026-01-01T01:00:00Z",
            "resolution": "PT1H",
            "aggregate": "true",
        },
    )

    assert response.status_code == 200
    envelope = response.json()
    assert envelope["mode"] == "aggregated"
    buckets = envelope["data"]
    assert len(buckets) == 1
    # +250 (in) / -50 (out) on USDC, which is held directly (no receipt-token
    # wrapper): not valued, so net_flow_usd is 0 though both events are counted.
    assert buckets[0]["event_count"] == 2
    assert Decimal(str(buckets[0]["net_flow_usd"])) == Decimal("0")


# ---------------------------------------------------------------------------
# Total-capital aggregation: time_bucket_gapfill + locf over the SubProxy
# treasury USDS balance. A bucket with no observation carries the previous
# value forward; buckets before the first observation gap-fill to null. The
# prime is addressed by its ALM proxy; the SubProxy is matched by shared
# prime_id.
# ---------------------------------------------------------------------------


def test_total_capital_buckets_locf_carry_forward_and_leading_gap(client: TestClient) -> None:
    response = client.get(
        f"/v1/primes/0x{_SPARK_PROXY_HEX}/total-capital",
        params={
            "from_timestamp": "2025-12-31T23:00:00Z",
            "to_timestamp": "2026-01-01T03:30:00Z",
            "resolution": "PT1H",
            "aggregate": "true",
        },
    )

    assert response.status_code == 200
    envelope = response.json()
    assert envelope["mode"] == "aggregated"
    by_start = {datetime.fromisoformat(b["bucket_start"]): b for b in envelope["data"]}

    leading = by_start[datetime(2025, 12, 31, 23, 0, tzinfo=UTC)]
    assert leading["total_capital_usd"] is None, "bucket before the first observation must gap-fill to null"

    first = by_start[datetime(2026, 1, 1, 0, 0, tzinfo=UTC)]
    assert Decimal(first["total_capital_usd"]) == Decimal("2000000")

    carried = by_start[datetime(2026, 1, 1, 1, 0, tzinfo=UTC)]
    assert Decimal(carried["total_capital_usd"]) == Decimal("2000000"), "LOCF must carry the prior value into the gap"

    second = by_start[datetime(2026, 1, 1, 2, 0, tzinfo=UTC)]
    assert Decimal(second["total_capital_usd"]) == Decimal("2100000")

    trailing = by_start[datetime(2026, 1, 1, 3, 0, tzinfo=UTC)]
    assert Decimal(trailing["total_capital_usd"]) == Decimal("2100000")


def test_total_capital_returns_all_null_when_prime_has_no_treasury(client: TestClient) -> None:
    """Grove is a known prime (it holds GNO) but has no seeded SubProxy USDS, so
    its total-capital series is a 200 with every bucket gap-filled to null —
    not a 404 and not an error.
    """
    response = client.get(
        f"/v1/primes/0x{_GROVE_PROXY_HEX}/total-capital",
        params={
            "from_timestamp": "2026-01-01T00:00:00Z",
            "to_timestamp": "2026-01-01T03:00:00Z",
            "resolution": "PT1H",
            "aggregate": "true",
        },
    )

    assert response.status_code == 200
    envelope = response.json()
    assert envelope["mode"] == "aggregated"
    # No treasury observations -> every bucket is null (gap-filled) or the series
    # is empty; either way no real value is surfaced, and it is not a 404/500.
    assert all(b["total_capital_usd"] is None for b in envelope["data"])


def test_risk_capital_self_computed_total_is_latest_treasury(client: TestClient) -> None:
    """The self-computed risk-capital endpoint reports Total Risk Capital from the
    latest on-chain SubProxy USDS balance (the 2.1M observation wins over 2.0M),
    independent of the Star feed. The default model (gap_sweep) is reported and a
    per-allocation breakdown is present; required RRC depends on model coverage
    which the fixture does not seed, so it is not asserted here.
    """
    response = client.get(f"/v1/primes/0x{_SPARK_PROXY_HEX}/risk-capital")

    assert response.status_code == 200
    body = response.json()
    assert body["model"] == "gap_sweep"
    assert Decimal(body["total_risk_capital_usd"]) == Decimal("2100000")
    assert isinstance(body["per_allocation"], list)


def test_risk_capital_returns_404_for_unknown_prime(client: TestClient) -> None:
    response = client.get(f"/v1/primes/0x{_UNKNOWN_PROXY_HEX}/risk-capital")

    assert response.status_code == 404
