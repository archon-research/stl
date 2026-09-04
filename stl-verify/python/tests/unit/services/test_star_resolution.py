"""Resolving an address to the prime it names.

Shared by both reference services, so the resolution scenarios live here once
rather than being restated against each of them.
"""

from unittest.mock import AsyncMock

from app.domain.entities.allocation import EthAddress, Prime
from app.services.star_resolution import star_for

# A real axis-synome spark ALM proxy: resolution goes through the contract, so a
# placeholder address would resolve to no prime at all.
_SPARK_ALM = EthAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
_UNKNOWN_PROXY = EthAddress("0x" + "ab" * 20)
_VAULT = EthAddress("0x" + "ba" * 20)


def _directory(*primes: Prime) -> AsyncMock:
    directory = AsyncMock()
    directory.list_primes.return_value = list(primes)
    return directory


def _prime(name: str, address: EthAddress, vault: EthAddress | None = None) -> Prime:
    return Prime(
        id=str(address),
        name=name,
        address=str(address),
        chain_id=1,
        chain="mainnet",
        role="alm",
        prime_vault_address=str(vault) if vault else None,
    )


async def test_resolves_a_proxy_the_contract_indexes_without_touching_the_directory() -> None:
    # The contract is the tracked set, so it answers before any I/O.
    directory = _directory(_prime("wrong-name", _SPARK_ALM))

    assert await star_for(_SPARK_ALM, directory) == "spark"
    directory.list_primes.assert_not_awaited()


# Self mode answers for a prime's vault address, so reference mode must too: the
# same URL differs only in which figures it returns.
async def test_resolves_a_prime_by_its_vault_address() -> None:
    directory = _directory(_prime("spark", _UNKNOWN_PROXY, vault=_VAULT))

    assert await star_for(_VAULT, directory) == "spark"


# A proxy holds positions before the contract is told about it during a chain
# onboarding; reference mode must not go dark for it.
async def test_resolves_a_proxy_the_contract_does_not_index_yet() -> None:
    directory = _directory(_prime("spark", _UNKNOWN_PROXY))

    assert await star_for(_UNKNOWN_PROXY, directory) == "spark"


async def test_returns_none_for_an_address_that_names_no_prime() -> None:
    assert await star_for(_UNKNOWN_PROXY, _directory()) is None
