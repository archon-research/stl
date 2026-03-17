# tests/integration/test_sparklend_liquidation_params_repository.py
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.sparklend_liquidation_params_repository import (
    SparkLendLiquidationParamsRepository,
)
from tests.integration.conftest import insert_token, store_test_ids


async def _insert_reserve_with_liq_params(
    conn: asyncpg.Connection,
    protocol_id: int,
    token_id: int,
    block_number: int,
    *,
    liquidation_threshold_bps: int,
    liquidation_bonus_bps: int,
) -> None:
    await conn.execute(
        """
        INSERT INTO sparklend_reserve_data
            (protocol_id, token_id, block_number, block_version,
             usage_as_collateral_enabled, ltv,
             liquidation_threshold, liquidation_bonus)
        VALUES ($1, $2, $3, 0, true, $4, $5, $6)
        """,
        protocol_id,
        token_id,
        block_number,
        Decimal("8000"),
        Decimal(liquidation_threshold_bps),
        Decimal(liquidation_bonus_bps),
    )


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def _seed_data(db_url: str) -> None:
    conn = await asyncpg.connect(db_url)
    try:
        protocol_id = cast(int, await conn.fetchval("SELECT id FROM protocol WHERE name = 'SparkLend'"))
        weth_id = cast(int, await conn.fetchval("SELECT id FROM token WHERE symbol = 'WETH' AND chain_id = 1"))
        cbbtc_id = cast(int, await conn.fetchval("SELECT id FROM token WHERE symbol = 'cbBTC' AND chain_id = 1"))

        await _insert_reserve_with_liq_params(
            conn, protocol_id, weth_id, 20_000_000,
            liquidation_threshold_bps=8250,  # 82.5%
            liquidation_bonus_bps=10500,     # 5% bonus → 1.05
        )
        await _insert_reserve_with_liq_params(
            conn, protocol_id, cbbtc_id, 20_000_000,
            liquidation_threshold_bps=7000,  # 70.0%
            liquidation_bonus_bps=11000,     # 10% bonus → 1.10
        )
        await store_test_ids(conn, {"protocol_id": protocol_id, "weth_id": weth_id, "cbbtc_id": cbbtc_id})
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def test_ids(db_url: str, _seed_data: None) -> dict[str, int]:
    conn = await asyncpg.connect(db_url)
    try:
        rows = await conn.fetch("SELECT key, val FROM _test_ids")
        return {row["key"]: row["val"] for row in rows}
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repository(async_db_url: str, _seed_data: None, test_ids: dict[str, int]):
    engine = create_async_engine(async_db_url)
    try:
        yield SparkLendLiquidationParamsRepository(engine, protocol_id=test_ids["protocol_id"])
    finally:
        await engine.dispose()


@pytest.mark.asyncio(loop_scope="module")
async def test_returns_normalised_params_for_known_tokens(repository, test_ids: dict[str, int]) -> None:
    result = await repository.get_params(
        backed_asset_id=0,  # unused for SparkLend
        token_ids=[test_ids["weth_id"], test_ids["cbbtc_id"]],
    )

    assert test_ids["weth_id"] in result
    assert test_ids["cbbtc_id"] in result

    weth = result[test_ids["weth_id"]]
    assert weth.liquidation_threshold == Decimal("0.825")
    assert weth.liquidation_bonus == Decimal("1.05")

    cbbtc = result[test_ids["cbbtc_id"]]
    assert cbbtc.liquidation_threshold == Decimal("0.70")
    assert cbbtc.liquidation_bonus == Decimal("1.10")


@pytest.mark.asyncio(loop_scope="module")
async def test_missing_token_absent_from_result(repository, test_ids: dict[str, int]) -> None:
    result = await repository.get_params(backed_asset_id=0, token_ids=[99999])
    assert 99999 not in result


@pytest.mark.asyncio(loop_scope="module")
async def test_empty_token_ids_returns_empty_dict(repository) -> None:
    result = await repository.get_params(backed_asset_id=0, token_ids=[])
    assert result == {}
