"""Reference debt buckets read against a real TimescaleDB.

The risk here is in the SQL: the wad rescale, the gap-fill, the correction
ordering, and resolving a prime by either its vault or a proxy address. None of
that is exercised by mocking the repository.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.prime_debt_repository import PrimeDebtRepository
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import insert_allocation_position

_PROXY = bytes.fromhex("1601843c5e9bc251a3272907010afa41fa18347e")
_WINDOW_START = datetime(2026, 8, 19, 0, 0, tzinfo=timezone.utc)
_OBSERVED = _WINDOW_START + timedelta(hours=2)


async def _insert_day(
    conn: asyncpg.Connection, prime_id: int, observed_at: datetime, *, debt: str, build_id: int = 1
) -> None:
    await conn.execute(
        """
        INSERT INTO prime_reference_balance_sheet (
            prime_id, observed_at, treasury_balance_usd, assets_usd, allocated_assets_usd,
            idle_assets_usd, debt_usd, backstop_capital_usd, source, build_id
        ) VALUES ($1, $2, '1', '1', '1', '0', $3, '0', 'skyeco:reference', $4)
        """,
        prime_id,
        observed_at,
        Decimal(debt),
        build_id,
    )


@pytest_asyncio.fixture(loop_scope="module")
async def seeded(db_url: str):
    conn = await asyncpg.connect(db_url)
    try:
        prime_id = cast(int, await conn.fetchval("SELECT id FROM prime WHERE name = 'spark'"))
        vault = cast(bytes, await conn.fetchval("SELECT vault_address FROM prime WHERE name = 'spark'"))
        token_id = cast(int, await conn.fetchval("SELECT id FROM token WHERE symbol = 'WETH' AND chain_id = 1"))
        await insert_allocation_position(
            conn,
            token_id=token_id,
            prime_id=prime_id,
            proxy_hex=_PROXY.hex(),
            balance=1,
            block=1,
            tx="11" * 32,
            direction="in",
            created_at=_OBSERVED,
        )
        await conn.execute("DELETE FROM prime_reference_balance_sheet WHERE prime_id = $1", prime_id)
        yield conn, prime_id, vault
        await conn.execute("DELETE FROM prime_reference_balance_sheet WHERE prime_id = $1", prime_id)
        await conn.execute("DELETE FROM allocation_position WHERE proxy_address = $1", _PROXY)
    finally:
        await conn.close()


async def _buckets(async_db_url: str, address: str, *, hours: int = 6):
    engine = create_async_engine(async_db_url)
    try:
        repository = PrimeDebtRepository(engine)
        return await repository.list_reference_debt_buckets(
            EthAddress(address),
            from_timestamp=_WINDOW_START,
            to_timestamp=_WINDOW_START + timedelta(hours=hours),
            bucket_seconds=3600,
        )
    finally:
        await engine.dispose()


@pytest.mark.asyncio(loop_scope="module")
async def test_rescales_the_upstream_usd_figure_to_wad(seeded, async_db_url: str):
    conn, prime_id, _ = seeded
    await _insert_day(conn, prime_id, _OBSERVED, debt="2645260280.72")

    buckets = await _buckets(async_db_url, "0x" + _PROXY.hex())

    observed = [b for b in buckets if b.debt_wad is not None]
    assert observed, "expected the seeded day to populate the series"
    assert observed[0].debt_wad == Decimal("2645260280.72") * Decimal(10) ** 18


@pytest.mark.asyncio(loop_scope="module")
async def test_carries_the_last_observation_forward(seeded, async_db_url: str):
    conn, prime_id, _ = seeded
    await _insert_day(conn, prime_id, _OBSERVED, debt="100")

    buckets = await _buckets(async_db_url, "0x" + _PROXY.hex())

    carried = [b for b in buckets if b.bucket_start > _OBSERVED]
    assert carried, "expected buckets after the observation"
    assert all(b.debt_wad == Decimal(100) * Decimal(10) ** 18 for b in carried)


@pytest.mark.asyncio(loop_scope="module")
async def test_leaves_buckets_before_the_first_observation_null(seeded, async_db_url: str):
    conn, prime_id, _ = seeded
    await _insert_day(conn, prime_id, _OBSERVED, debt="100")

    buckets = await _buckets(async_db_url, "0x" + _PROXY.hex())

    leading = [b for b in buckets if b.bucket_start < _OBSERVED]
    assert leading, "expected buckets before the observation"
    assert all(b.debt_wad is None for b in leading)


@pytest.mark.asyncio(loop_scope="module")
async def test_prefers_a_correction_over_the_original_it_supersedes(seeded, async_db_url: str):
    conn, prime_id, _ = seeded
    await _insert_day(conn, prime_id, _OBSERVED, debt="1", build_id=1)
    await _insert_day(conn, prime_id, _OBSERVED, debt="999", build_id=2)

    buckets = await _buckets(async_db_url, "0x" + _PROXY.hex())

    observed = [b for b in buckets if b.debt_wad is not None]
    assert observed[0].debt_wad == Decimal(999) * Decimal(10) ** 18


# The endpoint accepts either identity, so the reference read must resolve both.
@pytest.mark.asyncio(loop_scope="module")
async def test_resolves_the_prime_by_its_vault_address(seeded, async_db_url: str):
    conn, prime_id, vault = seeded
    await _insert_day(conn, prime_id, _OBSERVED, debt="100")

    buckets = await _buckets(async_db_url, "0x" + vault.hex())

    assert any(b.debt_wad is not None for b in buckets)
