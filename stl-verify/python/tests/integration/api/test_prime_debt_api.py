"""Integration tests for prime-debt address resolution.

``/v1/primes/{id}/debt`` accepts either identity a prime is reachable by — its
``prime.vault_address`` or one of its ``allocation_position.proxy_address`` values —
and must serve the same rows for both, always reporting the vault address back.
Resolution happens once up front, so these tests pin the payload for a fixed window
to catch the two identities drifting apart.
"""

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.prime_debt_repository import DEBT_BUCKETS_SQL, DEBT_SNAPSHOTS_SQL
from app.config import Settings
from app.main import create_app

_SPARK_VAULT_ADDR = "0x691a6c29e9e96dd897718305427ad5d534db16ba"
_SPARK_PROXY_ADDR = "0x" + "3c" * 20
_UNKNOWN_ADDR = "0x" + "9d" * 20

_FROM_TIMESTAMP = "2026-03-01T00:00:00Z"
_TO_TIMESTAMP = "2026-03-31T00:00:00Z"


async def _seed(async_url: str) -> None:
    engine = create_async_engine(async_url)
    try:
        async with engine.begin() as conn:
            spark_prime_id = (await conn.execute(text("SELECT id FROM prime WHERE name = 'spark'"))).scalar_one()
            usdc_token_id = (
                await conn.execute(
                    text("SELECT id FROM token WHERE chain_id = 1 AND symbol = 'USDC' ORDER BY id LIMIT 1")
                )
            ).scalar_one()

            # prime_proxy is static reference data and this proxy is not in the
            # migration's list, so the scenario declares it — positions alone do
            # not make an address resolvable.
            await conn.execute(
                text(
                    """
                    INSERT INTO prime_proxy (chain_id, proxy_address, prime_id)
                    VALUES (1, decode(:proxy, 'hex'), :pid)
                    ON CONFLICT (chain_id, proxy_address) DO NOTHING
                    """
                ),
                {"pid": spark_prime_id, "proxy": _SPARK_PROXY_ADDR.removeprefix("0x")},
            )

            await conn.execute(
                text(
                    """
                    INSERT INTO allocation_position
                        (chain_id, token_id, prime_id, proxy_address, balance,
                         block_number, block_version, tx_hash, log_index, tx_amount,
                         direction, created_at)
                    VALUES
                        (1, :tid, :pid, decode(:proxy, 'hex'), 100, 10, 0,
                         decode(:tx, 'hex'), 0, 10, 'in', '2026-03-05T12:00:00Z')
                    """
                ),
                {
                    "tid": usdc_token_id,
                    "pid": spark_prime_id,
                    "proxy": _SPARK_PROXY_ADDR.removeprefix("0x"),
                    "tx": "aa" * 32,
                },
            )

            await conn.execute(
                text(
                    """
                    INSERT INTO prime_debt
                        (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at)
                    VALUES
                        (:pid, 'ALLOCATOR-SPARK-A', 2000, 201, 0, '2026-03-06T12:00:00Z'),
                        (:pid, 'ALLOCATOR-SPARK-A', 1000, 200, 0, '2026-03-05T12:00:00Z')
                    """
                ),
                {"pid": spark_prime_id},
            )
    finally:
        await engine.dispose()


@pytest.fixture(scope="module")
def async_db_url(module_db):
    asyncio.run(_seed(module_db["async_url"]))
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


def _debt(client: TestClient, address: str, **params: str) -> dict:
    response = client.get(
        f"/v1/primes/{address}/debt",
        params={"from_timestamp": _FROM_TIMESTAMP, "to_timestamp": _TO_TIMESTAMP, **params},
    )
    assert response.status_code == 200
    return response.json()


def test_debt_resolves_by_vault_address(client: TestClient) -> None:
    body = _debt(client, _SPARK_VAULT_ADDR)

    assert body["mode"] == "raw"
    assert [(row["block_number"], row["debt_wad"]) for row in body["data"]] == [(201, "2000"), (200, "1000")]


def test_debt_resolves_by_proxy_address_to_the_same_payload(client: TestClient) -> None:
    by_vault = _debt(client, _SPARK_VAULT_ADDR)

    by_proxy = _debt(client, _SPARK_PROXY_ADDR)

    assert by_proxy == by_vault
    # The vault address is reported back regardless of which identity was queried.
    assert {row["prime_address"] for row in by_proxy["data"]} == {_SPARK_VAULT_ADDR}


def test_aggregated_debt_resolves_by_proxy_address_to_the_same_buckets(client: TestClient) -> None:
    by_vault = _debt(client, _SPARK_VAULT_ADDR, aggregate="true", resolution="P1D")

    by_proxy = _debt(client, _SPARK_PROXY_ADDR, aggregate="true", resolution="P1D")

    assert by_proxy == by_vault
    assert "2000" in {bucket["debt_wad"] for bucket in by_proxy["data"]}


def test_unknown_address_is_not_found(client: TestClient) -> None:
    response = client.get(f"/v1/primes/{_UNKNOWN_ADDR}/debt")

    assert response.status_code == 404
    assert response.json()["detail"] == "Prime not found"


def _plan_nodes(node: object) -> list[dict]:
    """Flatten every plan node in an ``EXPLAIN (FORMAT JSON)`` tree."""
    if isinstance(node, list):
        return [found for item in node for found in _plan_nodes(item)]
    if isinstance(node, dict):
        nested = [found for value in node.values() for found in _plan_nodes(value)]
        return [node, *nested] if "Node Type" in node else nested
    return []


async def _explain(async_url: str, sql: str) -> tuple[list[dict], set[str]]:
    """Return *sql*'s plan nodes, plus every relation name ``allocation_position`` spans.

    Plain EXPLAIN, so the plan does not depend on the seeded rows being present.
    """
    engine = create_async_engine(async_url)
    try:
        async with engine.connect() as conn:
            plan = (
                await conn.execute(
                    text(f"EXPLAIN (FORMAT JSON) {sql}"),
                    {
                        "prime_id": 1,
                        "from_timestamp": datetime(2026, 3, 1, tzinfo=UTC),
                        "to_timestamp": datetime(2026, 3, 31, tzinfo=UTC),
                        "bucket_seconds": 3600.0,
                        "limit": 100,
                    },
                )
            ).scalar_one()
            chunks = (
                (
                    await conn.execute(
                        text(
                            "SELECT chunk_name FROM timescaledb_information.chunks "
                            "WHERE hypertable_name = 'allocation_position'"
                        )
                    )
                )
                .scalars()
                .all()
            )
    finally:
        await engine.dispose()

    return _plan_nodes(json.loads(plan) if isinstance(plan, str) else plan), {"allocation_position", *chunks}


@pytest.mark.parametrize("sql", [DEBT_SNAPSHOTS_SQL, DEBT_BUCKETS_SQL], ids=["snapshots", "buckets"])
def test_debt_query_plans_never_touch_allocation_position(async_db_url: str, sql: str) -> None:
    """The debt statements must never plan ``allocation_position``.

    An inline address match is an EXISTS under an OR, which Postgres cannot decorrelate:
    the plan carries a SubPlan re-scanning every ``allocation_position`` chunk once per
    joined ``prime_debt`` row. Plan shape only — timings and memory are data-volume
    dependent.
    """
    nodes, allocation_relations = asyncio.run(_explain(async_db_url, sql))

    assert nodes, "EXPLAIN returned no plan nodes"
    # A chunk scan names the chunk (``_hyper_N_M_chunk``), never the hypertable, so the
    # names have to come from the catalog. The seeded row guarantees at least one exists,
    # without which this assertion could not fire at all.
    assert len(allocation_relations) > 1
    assert not [node for node in nodes if node.get("Relation Name") in allocation_relations]
    assert not [node for node in nodes if "Subplan Name" in node or node.get("Parent Relationship") == "SubPlan"]
