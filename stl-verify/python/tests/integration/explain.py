"""Plan-shape helpers: flatten an ``EXPLAIN (FORMAT JSON)`` tree so a test can assert
which relations a statement plans, independent of the seeded data volume."""

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


def plan_nodes(node: object) -> list[dict]:
    """Flatten every plan node in an ``EXPLAIN (FORMAT JSON)`` tree."""
    if isinstance(node, list):
        return [found for item in node for found in plan_nodes(item)]
    if isinstance(node, dict):
        nested = [found for value in node.values() for found in plan_nodes(value)]
        return [node, *nested] if "Node Type" in node else nested
    return []


async def explain_nodes(conn: AsyncConnection, sql: str, params: dict[str, Any]) -> list[dict]:
    """Plain ``EXPLAIN`` of *sql* (no execution), flattened to its plan nodes."""
    plan = (await conn.execute(text(f"EXPLAIN (FORMAT JSON) {sql}"), params)).scalar_one()
    return plan_nodes(json.loads(plan) if isinstance(plan, str) else plan)


async def hypertable_relations(conn: AsyncConnection, hypertables: list[str]) -> set[str]:
    """The hypertables plus every chunk they currently have.

    A chunk scan names the chunk (``_hyper_N_M_chunk``), never the hypertable, so
    the names have to come from the catalog.
    """
    chunks = (
        (
            await conn.execute(
                text("SELECT chunk_name FROM timescaledb_information.chunks WHERE hypertable_name = ANY(:names)"),
                {"names": hypertables},
            )
        )
        .scalars()
        .all()
    )
    return {*hypertables, *chunks}
