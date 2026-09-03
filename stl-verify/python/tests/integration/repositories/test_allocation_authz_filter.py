"""The authorization filter, run against a real database.

The allow-list is the only part of the enforcement change that lives in SQL, so
"an empty list means no rows" is a claim about Postgres until Postgres is asked:
``ANY(ARRAY[]::BYTEA[])`` matching nothing, and a NULL cast short-circuiting the
predicate, are both database behaviour that a mock cannot get wrong.

``get_prime_vault_address`` is here for the same reason — it is what the
per-resource gate resolves an OpenFGA object id with, so a wrong answer is a
403 on a prime the caller owns or a check against the wrong prime entirely.

Seeded from the migration's own declared primes plus ``seed_prime_fan_out``, so
the filter runs against the real vault and proxy addresses rather than ones
invented here.
"""

import asyncio
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.adapters.postgres.reference_as_of import utc_now
from app.domain.entities.allocation import EthAddress
from tests.integration.seed import (
    GROVE_MAINNET_ALM_HEX,
    SPARK_MAINNET_ALM_HEX,
    SPARK_SUB_PROXY_HEX,
    seed_prime_fan_out,
)

# Declared by 20260305_120000_create_prime_debts.sql.
SPARK_VAULT = EthAddress("0x691a6c29e9e96dd897718305427ad5d534db16ba")
GROVE_VAULT = EthAddress("0x26512a41c8406800f21094a7a7a0f980f6e25d43")
# A declared prime with no proxies and no activity: it can never be a false positive.
OBEX_VAULT = EthAddress("0xf275110dfe7b80df66a762f968f59b70babe2b29")
UNDECLARED = EthAddress("0x" + "ab" * 20)

SPARK_MAINNET_ALM = EthAddress(f"0x{SPARK_MAINNET_ALM_HEX}")
SPARK_SUB_PROXY = EthAddress(f"0x{SPARK_SUB_PROXY_HEX}")
GROVE_MAINNET_ALM = EthAddress(f"0x{GROVE_MAINNET_ALM_HEX}")


@pytest.fixture(scope="module")
def async_db_url(module_db):
    """Seed spark's and grove's activity into the module's isolated database."""
    asyncio.run(seed_prime_fan_out(module_db["db_url"]))
    return module_db["async_url"]


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def repository(async_db_url: str) -> AsyncIterator[AllocationRepository]:
    engine = create_async_engine(async_db_url)
    try:
        yield AllocationRepository(engine, utc_now)
    finally:
        await engine.dispose()


async def _listed_names(repository: AllocationRepository, allowed) -> set[str]:
    return {prime.name for prime in await repository.list_primes(allowed_vaults=allowed)}


async def _activity_names(repository: AllocationRepository, allowed) -> set[str]:
    events = await repository.list_allocation_activity(allowed_vaults=allowed, limit=1000)
    return {event.prime_name for event in events}


# --- list_primes ------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="module")
async def test_no_allow_list_returns_every_prime(repository: AllocationRepository) -> None:
    """None is auth off. The filter must not narrow anything while dark."""
    assert await _listed_names(repository, None) == {"spark", "grove"}


@pytest.mark.asyncio(loop_scope="module")
async def test_a_permitted_vault_returns_only_that_primes_rows(repository: AllocationRepository) -> None:
    assert await _listed_names(repository, [SPARK_VAULT]) == {"spark"}


@pytest.mark.asyncio(loop_scope="module")
async def test_an_empty_allow_list_returns_no_rows(repository: AllocationRepository) -> None:
    """The contract the comment claims and nothing had executed: [] is "may
    view none", not "no filter". Collapsing the two discloses every prime."""
    assert await repository.list_primes(allowed_vaults=[]) == []


@pytest.mark.asyncio(loop_scope="module")
async def test_permitting_both_vaults_returns_both_primes(repository: AllocationRepository) -> None:
    assert await _listed_names(repository, [SPARK_VAULT, GROVE_VAULT]) == {"spark", "grove"}


@pytest.mark.asyncio(loop_scope="module")
async def test_the_filter_matches_the_vault_and_returns_its_proxies(repository: AllocationRepository) -> None:
    """The allow-list holds VAULT addresses — what the reconciler writes — while
    the rows are keyed by the prime's per-chain ALM PROXY addresses."""
    primes = await repository.list_primes(allowed_vaults=[SPARK_VAULT])

    assert {prime.prime_vault_address for prime in primes} == {str(SPARK_VAULT)}
    assert str(SPARK_MAINNET_ALM) in {prime.address for prime in primes}


@pytest.mark.asyncio(loop_scope="module")
async def test_a_vault_naming_no_indexed_prime_returns_no_rows(repository: AllocationRepository) -> None:
    assert await repository.list_primes(allowed_vaults=[UNDECLARED]) == []


# --- list_allocation_activity ----------------------------------------------


@pytest.mark.asyncio(loop_scope="module")
async def test_activity_without_an_allow_list_spans_every_prime(repository: AllocationRepository) -> None:
    assert await _activity_names(repository, None) == {"spark", "grove"}


@pytest.mark.asyncio(loop_scope="module")
async def test_activity_returns_only_the_permitted_primes_events(repository: AllocationRepository) -> None:
    assert await _activity_names(repository, [SPARK_VAULT]) == {"spark"}


@pytest.mark.asyncio(loop_scope="module")
async def test_activity_with_an_empty_allow_list_returns_no_rows(repository: AllocationRepository) -> None:
    assert await repository.list_allocation_activity(allowed_vaults=[], limit=1000) == []


@pytest.mark.asyncio(loop_scope="module")
async def test_the_allow_list_wins_over_an_explicitly_requested_prime(repository: AllocationRepository) -> None:
    """Both filters are in one WHERE, so asking for grove's proxy by name does
    not widen a spark-only caller — the route's own check is not the only gate."""
    events = await repository.list_allocation_activity(
        proxy_addresses=[GROVE_MAINNET_ALM], allowed_vaults=[SPARK_VAULT], limit=1000
    )

    assert events == []


# --- get_prime_vault_address ------------------------------------------------


@pytest.mark.asyncio(loop_scope="module")
@pytest.mark.parametrize(
    "presented,expected",
    [
        pytest.param(SPARK_MAINNET_ALM, str(SPARK_VAULT), id="alm-proxy"),
        pytest.param(SPARK_SUB_PROXY, str(SPARK_VAULT), id="sub-proxy"),
        pytest.param(SPARK_VAULT, str(SPARK_VAULT), id="the-vault-itself"),
        pytest.param(GROVE_MAINNET_ALM, str(GROVE_VAULT), id="another-prime"),
        pytest.param(OBEX_VAULT, str(OBEX_VAULT), id="a-prime-with-no-proxies"),
        pytest.param(UNDECLARED, None, id="unknown-address"),
    ],
)
async def test_every_address_of_a_prime_resolves_to_its_vault(
    repository: AllocationRepository, presented: EthAddress, expected: str | None
) -> None:
    """The gate keys the OpenFGA object id on this, so a caller presenting any
    of a prime's addresses must reach the same ``prime:<vault>`` resource."""
    assert await repository.get_prime_vault_address(presented) == expected
