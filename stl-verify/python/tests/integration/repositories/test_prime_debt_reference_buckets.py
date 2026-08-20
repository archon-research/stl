"""Reference debt buckets read against a real TimescaleDB.

The risk here is in the SQL: the wad rescale, the gap-fill, the correction
ordering, and filtering by the already-resolved prime ID. None of that is
exercised by mocking the repository.
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
from tests.integration.seed import insert_allocation_position, insert_token

_WINDOW_START = datetime(2026, 8, 19, 0, 0, tzinfo=timezone.utc)
_OBSERVED = _WINDOW_START + timedelta(hours=2)
_PRIME_NAME = "prime_debt_reference_buckets"
_PRIME_ADDRESS = bytes.fromhex("7a" * 20)


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
        prime_id = cast(
            int,
            await conn.fetchval(
                "INSERT INTO prime (name, vault_address) VALUES ($1, $2) RETURNING id",
                _PRIME_NAME,
                _PRIME_ADDRESS,
            ),
        )
        await conn.execute("DELETE FROM prime_reference_balance_sheet WHERE prime_id = $1", prime_id)
        yield conn, prime_id
        await conn.execute("DELETE FROM prime_reference_balance_sheet WHERE prime_id = $1", prime_id)
        await conn.execute("DELETE FROM prime WHERE id = $1", prime_id)
    finally:
        await conn.close()


async def _buckets(async_db_url: str, prime_id: int, *, hours: int = 6):
    engine = create_async_engine(async_db_url)
    try:
        repository = PrimeDebtRepository(engine)
        return await repository.list_reference_debt_buckets(
            prime_id,
            from_timestamp=_WINDOW_START,
            to_timestamp=_WINDOW_START + timedelta(hours=hours),
            bucket_seconds=3600,
        )
    finally:
        await engine.dispose()


@pytest.mark.asyncio(loop_scope="module")
async def test_rescales_the_upstream_usd_figure_to_wad(seeded, async_db_url: str):
    conn, prime_id = seeded
    await _insert_day(conn, prime_id, _OBSERVED, debt="2645260280.72")

    buckets = await _buckets(async_db_url, prime_id)

    observed = [b for b in buckets if b.debt_wad is not None]
    assert observed, "expected the seeded day to populate the series"
    assert observed[0].debt_wad == Decimal("2645260280.72") * Decimal(10) ** 18


@pytest.mark.asyncio(loop_scope="module")
async def test_carries_the_last_observation_forward(seeded, async_db_url: str):
    conn, prime_id = seeded
    await _insert_day(conn, prime_id, _OBSERVED, debt="100")

    buckets = await _buckets(async_db_url, prime_id)

    carried = [b for b in buckets if b.bucket_start > _OBSERVED]
    assert carried, "expected buckets after the observation"
    assert all(b.debt_wad == Decimal(100) * Decimal(10) ** 18 for b in carried)


@pytest.mark.asyncio(loop_scope="module")
async def test_carries_an_observation_from_before_the_window_into_it(seeded, async_db_url: str):
    # Upstream publishes one row per prime per day, so from a minute past
    # midnight the newest row already sits outside a 24h window. Without seeding
    # locf from the prior observation the card reports a figure while its own
    # chart reports none.
    conn, prime_id, _vault = seeded
    await _insert_day(conn, prime_id, _WINDOW_START - timedelta(days=2), debt="2642983145.21")

    buckets = await _buckets(async_db_url, f"0x{_PROXY.hex()}")

    assert buckets
    assert all(b.debt_wad == Decimal("2642983145.21") * Decimal("1e18") for b in buckets)


@pytest.mark.asyncio(loop_scope="module")
async def test_leaves_buckets_before_the_first_observation_null(seeded, async_db_url: str):
    conn, prime_id = seeded
    await _insert_day(conn, prime_id, _OBSERVED, debt="100")

    buckets = await _buckets(async_db_url, prime_id)

    leading = [b for b in buckets if b.bucket_start < _OBSERVED]
    assert leading, "expected buckets before the observation"
    assert all(b.debt_wad is None for b in leading)


@pytest.mark.asyncio(loop_scope="module")
async def test_prefers_a_correction_over_the_original_it_supersedes(seeded, async_db_url: str):
    conn, prime_id = seeded
    await _insert_day(conn, prime_id, _OBSERVED, debt="1", build_id=1)
    await _insert_day(conn, prime_id, _OBSERVED, debt="999", build_id=2)

    buckets = await _buckets(async_db_url, prime_id)

    observed = [b for b in buckets if b.debt_wad is not None]
    assert observed[0].debt_wad == Decimal(999) * Decimal(10) ** 18


@pytest.mark.asyncio(loop_scope="module")
async def test_reads_the_prime_by_its_resolved_id(seeded, async_db_url: str):
    conn, prime_id = seeded
    await _insert_day(conn, prime_id, _OBSERVED, debt="100")

    buckets = await _buckets(async_db_url, prime_id)

    assert any(b.debt_wad is not None for b in buckets)


@pytest.mark.asyncio(loop_scope="module")
async def test_resolve_prime_id_prefers_a_vault_address_over_a_matching_proxy(seeded, async_db_url: str):
    conn, _ = seeded
    address = bytes.fromhex("7b" * 20)
    token_id = await insert_token(conn, "PRIME-DEBT-RESOLVE", 18, bytes.fromhex("7c" * 20))
    vault_prime_id = cast(
        int,
        await conn.fetchval(
            "INSERT INTO prime (name, vault_address) VALUES ('prime_debt_vault_match', $1) RETURNING id",
            address,
        ),
    )
    proxy_prime_id = cast(
        int,
        await conn.fetchval(
            "INSERT INTO prime (name, vault_address) VALUES ('prime_debt_proxy_match', $1) RETURNING id",
            bytes.fromhex("7d" * 20),
        ),
    )
    try:
        await insert_allocation_position(
            conn,
            token_id=token_id,
            prime_id=proxy_prime_id,
            proxy_hex=address.hex(),
            balance=1,
            block=1,
            tx="7e" * 32,
            direction="in",
        )
        engine = create_async_engine(async_db_url)
        try:
            resolved_id = await PrimeDebtRepository(engine).resolve_prime_id(EthAddress("0x" + address.hex()))
        finally:
            await engine.dispose()

        assert resolved_id == vault_prime_id
    finally:
        await conn.execute("DELETE FROM allocation_position WHERE prime_id = $1", proxy_prime_id)
        await conn.execute("DELETE FROM prime WHERE id = ANY($1::bigint[])", [vault_prime_id, proxy_prime_id])
        await conn.execute("DELETE FROM token WHERE id = $1", token_id)
