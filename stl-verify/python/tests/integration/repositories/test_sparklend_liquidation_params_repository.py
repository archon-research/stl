from decimal import Decimal
from typing import cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.aave_like_liquidation_params_repository import (
    AaveLikeLiquidationParamsRepository,
)
from tests.integration.seed import store_test_ids


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
            conn,
            protocol_id,
            weth_id,
            20_000_000,
            liquidation_threshold_bps=8250,  # 82.5%
            liquidation_bonus_bps=10500,  # 5% bonus → 1.05
        )
        await _insert_reserve_with_liq_params(
            conn,
            protocol_id,
            cbbtc_id,
            20_000_000,
            liquidation_threshold_bps=7000,  # 70.0%
            liquidation_bonus_bps=11000,  # 10% bonus → 1.10
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
        yield AaveLikeLiquidationParamsRepository(engine)
    finally:
        await engine.dispose()


@pytest.mark.asyncio(loop_scope="module")
async def test_returns_normalised_params_for_known_tokens(repository, test_ids: dict[str, int]) -> None:
    result = await repository.get_params(protocol_id=test_ids["protocol_id"])

    assert test_ids["weth_id"] in result
    assert test_ids["cbbtc_id"] in result

    weth = result[test_ids["weth_id"]]
    assert weth.liquidation_threshold == Decimal("0.825")
    assert weth.liquidation_bonus == Decimal("1.05")

    cbbtc = result[test_ids["cbbtc_id"]]
    assert cbbtc.liquidation_threshold == Decimal("0.70")
    assert cbbtc.liquidation_bonus == Decimal("1.10")


@pytest.mark.asyncio(loop_scope="module")
async def test_another_protocols_reserves_are_not_returned(repository, db_url: str, test_ids: dict[str, int]) -> None:
    """The read is protocol-scoped, so a same-token reserve elsewhere must not leak in."""
    conn = await asyncpg.connect(db_url)
    try:
        other_protocol_id = cast(int, await conn.fetchval("SELECT id FROM protocol WHERE name = 'Aave V3'"))
        await _insert_reserve_with_liq_params(
            conn,
            other_protocol_id,
            test_ids["weth_id"],
            20_000_001,
            liquidation_threshold_bps=6000,
            liquidation_bonus_bps=12000,
        )
    finally:
        await conn.close()

    result = await repository.get_params(protocol_id=test_ids["protocol_id"])
    assert result[test_ids["weth_id"]].liquidation_threshold == Decimal("0.825")


@pytest.mark.asyncio(loop_scope="module")
async def test_reserve_disabled_as_collateral_drops_out(repository, db_url: str, test_ids: dict[str, int]) -> None:
    """A reserve the protocol has since stopped accepting as collateral is not returned.

    The old query filtered on usage_as_collateral_enabled *before* reducing to the
    newest row, so it could keep serving an older, still-enabled row after the
    protocol disabled the reserve. Reading the newest row and filtering that is what
    the flag means, and matches how the backed-breakdown query reads this table.
    """
    conn = await asyncpg.connect(db_url)
    try:
        await conn.execute(
            """
            INSERT INTO sparklend_reserve_data
                (protocol_id, token_id, block_number, block_version,
                 usage_as_collateral_enabled, liquidation_threshold, liquidation_bonus)
            VALUES ($1, $2, $3, 0, false, 7000, 11000)
            """,
            test_ids["protocol_id"],
            test_ids["cbbtc_id"],
            20_000_002,
        )
    finally:
        await conn.close()

    result = await repository.get_params(protocol_id=test_ids["protocol_id"])
    assert test_ids["cbbtc_id"] not in result
    assert test_ids["weth_id"] in result
