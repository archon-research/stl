"""Integration tests for the PSM3 reserves endpoints (``/v1/psm3/reserves{,/history}``).

Exercises the FastAPI handlers through ``Psm3Service`` to
``PostgresPsm3Repository`` against a real Postgres, including the
latest-per-chain version ordering and the price join.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.config import Settings
from app.main import create_app

_PSM3_ADDRESS = bytes.fromhex("ab" * 20)
_BLOCK_TS = datetime(2026, 6, 12, 12, 0, tzinfo=UTC)

# Contract DDL for psm3_snapshot, copied from the sibling Go indexer PR's
# migration (db/migrations/). That migration is not on this branch, so the
# table is created here; keep this in sync with it. The DROP keeps this
# fixture working once that migration lands and creates the table first.
_PSM3_SNAPSHOT_DDL = """
DROP TABLE IF EXISTS psm3_snapshot;
CREATE TABLE psm3_snapshot (
    chain_id           INT         NOT NULL REFERENCES chain (chain_id),
    address            BYTEA       NOT NULL,
    usds_balance       NUMERIC     NOT NULL,
    susds_balance      NUMERIC     NOT NULL,
    usdc_balance       NUMERIC     NOT NULL,
    total_assets       NUMERIC     NOT NULL,
    conversion_rate    NUMERIC     NOT NULL,
    block_number       BIGINT      NOT NULL,
    block_version      INT         NOT NULL DEFAULT 0,
    block_timestamp    TIMESTAMPTZ NOT NULL,
    source             TEXT        NOT NULL CHECK (source = 'sweep'),
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, block_number, block_version, processing_version, block_timestamp)
)
"""


async def _insert_snapshot(
    conn: asyncpg.Connection,
    *,
    chain_id: int,
    usds: int,
    block_number: int,
    block_version: int = 0,
    processing_version: int = 0,
    block_timestamp: datetime = _BLOCK_TS,
) -> None:
    await conn.execute(
        """
        INSERT INTO psm3_snapshot
            (chain_id, address, usds_balance, susds_balance, usdc_balance, total_assets,
             conversion_rate, block_number, block_version, block_timestamp, source, processing_version)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'sweep', $11)
        """,
        chain_id,
        _PSM3_ADDRESS,
        Decimal(usds * 10**18),
        Decimal(4 * 10**18),
        Decimal(5 * 10**6),
        Decimal(10 * 10**18),
        Decimal(105 * 10**25),  # conversion rate 1.05 in 1e27
        block_number,
        block_version,
        block_timestamp,
        processing_version,
    )


async def _insert_price(conn: asyncpg.Connection, source_asset_id: str, price: Decimal, timestamp: datetime) -> None:
    await conn.execute(
        """
        INSERT INTO offchain_token_price (token_id, source_id, timestamp, price_usd)
        SELECT opa.token_id, opa.source_id, $2, $3
        FROM offchain_price_asset opa
        JOIN offchain_price_source ops ON ops.id = opa.source_id
        WHERE ops.name = 'coingecko' AND opa.source_asset_id = $1
        """,
        source_asset_id,
        timestamp,
        price,
    )


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _seed_psm3(db_url: str) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        await conn.execute(_PSM3_SNAPSHOT_DDL)

        # Base: four rows at descending precedence. The (block 100, bv 1, pv 1)
        # row must win over the re-emitted (bv 1, pv 0), the original (bv 0),
        # and the older block. Every losing row gets a NEWER block_timestamp
        # than the winner (and the winner is inserted last) so any ordering by
        # block_timestamp — or by insertion order — fails these tests.
        def ts(minutes: int) -> datetime:
            return _BLOCK_TS + timedelta(minutes=minutes)

        await _insert_snapshot(conn, chain_id=8453, usds=9, block_number=99, block_timestamp=ts(60))
        await _insert_snapshot(conn, chain_id=8453, usds=8, block_number=100, block_version=0, block_timestamp=ts(30))
        await _insert_snapshot(conn, chain_id=8453, usds=7, block_number=100, block_version=1, block_timestamp=ts(10))
        await _insert_snapshot(conn, chain_id=8453, usds=2, block_number=100, block_version=1, processing_version=1)
        # Optimism: a single row.
        await _insert_snapshot(conn, chain_id=10, usds=3, block_number=200)

        now = datetime.now(UTC)
        await _insert_price(conn, "usds", Decimal("0.99"), now)
        await _insert_price(conn, "usd-coin", Decimal("1.01"), now)
        # A stale older usds price that must lose to the fresh one above.
        await _insert_price(conn, "usds", Decimal("0.5"), now - timedelta(days=1))
    finally:
        await conn.close()


@pytest.fixture(scope="module")
def client(async_db_url: str, tmp_path_factory, _seed_psm3: None):
    empty_mapping = tmp_path_factory.mktemp("cfg") / "empty_mapping.json"
    empty_mapping.write_text("{}")
    test_app = create_app(
        Settings.model_validate({"database_url": SecretStr(async_db_url), "suraf_mappings_file": empty_mapping})
    )
    with TestClient(test_app) as c:
        yield c


def test_list_reserves_returns_latest_row_per_chain(client: TestClient) -> None:
    response = client.get("/v1/psm3/reserves")

    assert response.status_code == 200
    payload = response.json()
    assert [(row["network"], row["block_number"]) for row in payload] == [("optimism", 200), ("base", 100)]

    base = payload[1]
    # The highest (block_version, processing_version) row wins: usds=2.
    assert base["block_version"] == 1
    assert Decimal(base["usds_balance"]) == Decimal("2")
    assert base["address"] == "0x" + "ab" * 20
    assert base["block_timestamp"] == "2026-06-12T12:00:00Z"


def test_list_reserves_joins_latest_fresh_prices(client: TestClient) -> None:
    response = client.get("/v1/psm3/reserves?network=base")

    assert response.status_code == 200
    [row] = response.json()
    assert Decimal(row["usds_price"]) == Decimal("0.99")
    assert Decimal(row["usdc_price"]) == Decimal("1.01")
    assert Decimal(row["susds_price"]) == Decimal("1.0395")  # 1.05 conversion rate x usds_price
    assert Decimal(row["usds_balance"]) == Decimal("2")
    assert Decimal(row["susds_balance"]) == Decimal("4")
    assert Decimal(row["usdc_balance"]) == Decimal("5")
    assert Decimal(row["usds_balance_usd"]) == Decimal("1.98")
    assert Decimal(row["susds_balance_usd"]) == Decimal("4.158")
    assert Decimal(row["usdc_balance_usd"]) == Decimal("5.05")
    # Served from the snapshot's PSM3.totalAssets(), not recomputed from *_usd.
    assert Decimal(row["total_assets"]) == Decimal("10")


def test_list_reserves_network_filter_with_no_rows_returns_empty(client: TestClient) -> None:
    response = client.get("/v1/psm3/reserves?network=arbitrum")

    assert response.status_code == 200
    assert response.json() == []


def test_list_reserves_returns_422_for_unknown_network(client: TestClient) -> None:
    response = client.get("/v1/psm3/reserves?network=mainnet")

    assert response.status_code == 422


def test_list_reserve_history_orders_by_block_then_versions(client: TestClient) -> None:
    response = client.get("/v1/psm3/reserves/history?network=base")

    assert response.status_code == 200
    rows = response.json()
    # usds_balance disambiguates rows that share a block_number.
    assert [Decimal(r["usds_balance"]) for r in rows] == [Decimal(v) for v in (2, 7, 8, 9)]


def test_list_reserve_history_respects_limit(client: TestClient) -> None:
    response = client.get("/v1/psm3/reserves/history?network=base&limit=2")

    assert response.status_code == 200
    rows = response.json()
    assert [Decimal(r["usds_balance"]) for r in rows] == [Decimal("2"), Decimal("7")]
