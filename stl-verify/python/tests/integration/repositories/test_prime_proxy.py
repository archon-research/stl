"""The reads that resolve a prime through ``prime_proxy``.

``prime_proxy`` is static reference data: the migration transcribes the declared
proxy universe from the axis-synome contract, and nothing writes to it at runtime.
So these tests seed no rows of their own for the declared addresses — the migration
already did — and what they cover is that the reads answer from that list, and that
an address absent from it resolves to nothing.
"""

from collections.abc import AsyncIterator
from typing import cast

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.adapters.postgres.reference_as_of import utc_now

# Declared in the migration. The ALM/SubProxy split matters: the reads classify by
# address, so a made-up one would be treated as ALM and the SubProxy exclusions
# would go untested.
_SPARK_MAINNET_ALM = "1601843c5e9bc251a3272907010afa41fa18347e"
_SPARK_BASE_ALM = "2917956eff0b5eaf030abdb4ef4296df775009ca"
_SPARK_SUB_PROXY = "3300f198988e4c9c63f75df86de36421f06af8c4"
_UNDECLARED = "ab" * 20


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def conn(db_url: str) -> AsyncIterator[asyncpg.Connection]:
    """One connection for the module's isolated database."""
    connection = await asyncpg.connect(db_url)
    try:
        yield connection
    finally:
        await connection.close()


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repository(async_db_url: str) -> AsyncIterator[AllocationRepository]:
    """The repository whose reads resolve a proxy to its prime."""
    engine = create_async_engine(async_db_url)
    try:
        yield AllocationRepository(engine, utc_now)
    finally:
        await engine.dispose()


@pytest.mark.asyncio(loop_scope="module")
async def test_the_migration_declares_the_full_proxy_list(conn: asyncpg.Connection) -> None:
    """Twelve rows, and every one resolves to a real prime.

    A prime name in the migration that does not match ``prime.name`` would drop its
    proxies silently, and every endpoint for them would return empty rather than fail.
    """
    rows = await conn.fetch(
        "SELECT p.name, pp.chain_id, encode(pp.proxy_address, 'hex') AS address "
        "FROM prime_proxy pp JOIN prime p ON p.id = pp.prime_id"
    )

    assert len(rows) == 12
    assert {row["name"] for row in rows} == {"spark", "grove"}


@pytest.mark.asyncio(loop_scope="module")
async def test_list_primes_reports_every_declared_alm_proxy(repository: AllocationRepository) -> None:
    """/v1/primes lists the declared ALM proxies, one row per (proxy, chain)."""
    primes = await repository.list_primes()

    spark = {prime.address.removeprefix("0x"): prime for prime in primes if prime.name == "spark"}
    assert _SPARK_MAINNET_ALM in spark
    assert _SPARK_BASE_ALM in spark
    assert _SPARK_SUB_PROXY not in spark
    assert spark[_SPARK_MAINNET_ALM].chain_id == 1
    assert spark[_SPARK_BASE_ALM].chain_id == 8453
    assert all(prime.prime_vault_address is not None for prime in spark.values())


@pytest.mark.asyncio(loop_scope="module")
async def test_an_address_cannot_be_declared_on_two_chains(conn: asyncpg.Connection) -> None:
    """Every read resolves by address alone, so the address has to be unique on its own.

    Without this the primary key would still permit one address on two chains, and
    ``WHERE proxy_address = ... LIMIT 1`` could return either prime — serving one
    prime's capital or custody data under another's address.
    """
    spark_id = cast(int, await conn.fetchval("SELECT id FROM prime WHERE name = 'spark'"))

    with pytest.raises(asyncpg.UniqueViolationError):
        await conn.execute(
            "INSERT INTO prime_proxy (chain_id, proxy_address, prime_id) VALUES (10, $1, $2)",
            bytes.fromhex(_SPARK_MAINNET_ALM),
            spark_id,
        )
