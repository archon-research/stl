"""Classification of allocation proxies by role.

PR #310 introduced SubProxy wallets that hold risk capital separately from
the main ALM proxy of each prime. Both wallet kinds end up in
``allocation_position`` under the same ``prime_id``, so a naive listing of
distinct proxy addresses surfaces them as duplicate primes.

This module centralises the address → kind mapping so ``/v1/primes`` can
filter SubProxies out today, and so the same mapping can later be exposed
to consumers that need to render the distinction.

Keep ``_SUB_PROXY_HEX`` in sync with the SubProxy entries in
``stl-verify/internal/services/allocation_tracker/config.go``.
"""

from enum import Enum


class ProxyKind(str, Enum):
    """Role of an allocation proxy wallet."""

    ALM = "alm"
    SUB_PROXY = "sub_proxy"


# Addresses (lowercase, 0x-prefixed) of proxies that hold capital on behalf
# of a prime but are not the prime's primary ALM proxy. Anything not listed
# here is treated as ALM — that matches the historical behaviour, where
# every tracked proxy was an ALM proxy before PR #310.
_SUB_PROXY_HEX: frozenset[str] = frozenset(
    {
        # Spark SubProxy — Atlas A.6.1.1.1.2.1.1.3.1.1.2 (mainnet).
        "0x3300f198988e4c9c63f75df86de36421f06af8c4",
        # Grove SubProxy — Atlas A.6.1.1.2.2.1.1.3.1.1.2 (mainnet).
        "0x1369f7b2b38c76b6478c0f0e66d94923421891ba",
    }
)


def classify_proxy(address: str) -> ProxyKind:
    """Return the :class:`ProxyKind` for a 0x-prefixed proxy address."""
    return ProxyKind.SUB_PROXY if address.lower() in _SUB_PROXY_HEX else ProxyKind.ALM
