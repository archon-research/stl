"""Topology tests for the prime ↔ proxy registry.

The expected addresses below are the real spark/grove proxies from the pinned
axis-synome contract. They are asserted verbatim rather than re-derived from the
contract, so an upstream re-point of a prime's proxies fails this test and forces
a conscious decision instead of silently changing what the API attributes to a
prime.
"""

import pytest

from app.domain.prime_registry import (
    ProxyKind,
    alm_proxies_for_prime,
    classify_proxy,
    is_primary_proxy,
    primary_proxy_for_prime,
    prime_name_for,
    proxy_entry,
    subproxy_addresses,
)

_SPARK_MAINNET_ALM = "0x1601843c5e9bc251a3272907010afa41fa18347e"
_SPARK_BASE_ALM = "0x2917956eff0b5eaf030abdb4ef4296df775009ca"
_SPARK_AVALANCHE_ALM = "0xece6b0e8a54c2f44e066fbb9234e7157b15b7fec"
_SPARK_SUB_PROXY = "0x3300f198988e4c9c63f75df86de36421f06af8c4"
_GROVE_MAINNET_ALM = "0x491edfb0b8b608044e227225c715981a30f3a44e"
_GROVE_SUB_PROXY = "0x1369f7b2b38c76b6478c0f0e66d94923421891ba"
_UNKNOWN = "0x" + "ab" * 20


@pytest.mark.parametrize(
    "address",
    [_SPARK_SUB_PROXY, _GROVE_SUB_PROXY],
)
def test_classify_proxy_returns_sub_proxy_for_contract_subproxies(address):
    assert classify_proxy(address) is ProxyKind.SUB_PROXY


@pytest.mark.parametrize(
    "address",
    [_SPARK_MAINNET_ALM, _SPARK_BASE_ALM, _SPARK_AVALANCHE_ALM, _GROVE_MAINNET_ALM],
)
def test_classify_proxy_returns_alm_for_contract_alm_proxies(address):
    assert classify_proxy(address) is ProxyKind.ALM


def test_classify_proxy_returns_alm_for_unknown_addresses():
    assert classify_proxy(_UNKNOWN) is ProxyKind.ALM


def test_classify_proxy_is_case_insensitive():
    assert classify_proxy(_SPARK_SUB_PROXY.upper()) is ProxyKind.SUB_PROXY


def test_subproxy_addresses_returns_the_contract_subproxies():
    assert subproxy_addresses() == frozenset({_SPARK_SUB_PROXY, _GROVE_SUB_PROXY})


def test_proxy_entry_carries_prime_name_chain_and_kind():
    entry = proxy_entry(_SPARK_AVALANCHE_ALM)

    assert entry is not None
    assert entry.address == _SPARK_AVALANCHE_ALM
    assert entry.prime_name == "spark"
    assert entry.chain == "avalanche-c"
    assert entry.kind is ProxyKind.ALM


def test_proxy_entry_returns_none_for_unknown_address():
    assert proxy_entry(_UNKNOWN) is None


def test_prime_name_for_resolves_a_non_mainnet_proxy_to_its_prime():
    assert prime_name_for(_SPARK_BASE_ALM) == "spark"


def test_prime_name_for_returns_none_for_unknown_address():
    assert prime_name_for(_UNKNOWN) is None


def test_alm_proxies_for_prime_includes_the_proxy_on_every_chain():
    addresses = {entry.address for entry in alm_proxies_for_prime("spark")}

    assert _SPARK_MAINNET_ALM in addresses
    assert _SPARK_BASE_ALM in addresses
    assert _SPARK_AVALANCHE_ALM in addresses


def test_alm_proxies_for_prime_excludes_subproxy_treasury_wallets():
    addresses = {entry.address for entry in alm_proxies_for_prime("spark")}

    assert _SPARK_SUB_PROXY not in addresses


def test_alm_proxies_for_prime_returns_empty_for_unknown_prime():
    assert alm_proxies_for_prime("nonesuch") == ()


def test_alm_proxies_for_prime_is_ordered_by_address():
    addresses = [entry.address for entry in alm_proxies_for_prime("spark")]

    assert addresses == sorted(addresses)


def test_primary_proxy_for_prime_is_the_mainnet_alm_proxy():
    entry = primary_proxy_for_prime("grove")

    assert entry is not None
    assert entry.address == _GROVE_MAINNET_ALM
    assert entry.chain == "mainnet"


def test_primary_proxy_for_prime_returns_none_for_unknown_prime():
    assert primary_proxy_for_prime("nonesuch") is None


def test_is_primary_proxy_accepts_the_mainnet_alm_proxy():
    assert is_primary_proxy(_SPARK_MAINNET_ALM) is True


@pytest.mark.parametrize(
    "address",
    [_SPARK_BASE_ALM, _SPARK_SUB_PROXY, _UNKNOWN],
)
def test_is_primary_proxy_rejects_every_other_address(address):
    assert is_primary_proxy(address) is False
