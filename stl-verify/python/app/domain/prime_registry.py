"""Prime-proxy topology from the axis-synome contract.

Keyed by address. Callers resolve a proxy without knowing its chain.
"""

from dataclasses import dataclass
from enum import Enum

from app.risk_engine._vendored_synome.export_entities import (
    PROXY_ROLE_ALM,
    PROXY_ROLE_SUBPROXY,
    build_axis_synome_contract,
)


class ProxyKind(str, Enum):
    """Role of an allocation proxy wallet."""

    ALM = "alm"
    SUB_PROXY = "sub_proxy"


@dataclass(frozen=True)
class ProxyEntry:
    """One proxy wallet. ``address`` is lowercase 0x-prefixed."""

    address: str
    prime_name: str
    chain: str
    kind: ProxyKind


_CONTRACT_ROLES: dict[str, ProxyKind] = {
    PROXY_ROLE_ALM: ProxyKind.ALM,
    PROXY_ROLE_SUBPROXY: ProxyKind.SUB_PROXY,
}


def _kind_from_role(role: str, address: str) -> ProxyKind:
    """Map a contract role string to a :class:`ProxyKind`, rejecting the unknown."""
    try:
        return _CONTRACT_ROLES[role]
    except KeyError:
        raise ValueError(
            f"axis-synome contract proxy {address} has unrecognised role {role!r}; "
            f"expected one of {sorted(_CONTRACT_ROLES)}"
        ) from None


def _index_proxies() -> dict[str, ProxyEntry]:
    """Flatten the contract's star -> chain -> [proxy] map, keyed by address."""
    contract = build_axis_synome_contract().model_dump(by_alias=True, mode="json")
    alm_proxies = contract["axis_synome"]["spec"]["entities"]["alm_proxies"]["AlmProxy"]

    entries: dict[str, ProxyEntry] = {}
    for prime_name, by_chain in alm_proxies.items():
        for chain, proxies in by_chain.items():
            for proxy in proxies:
                address = proxy["address"].lower()
                entry = ProxyEntry(
                    address=address,
                    prime_name=prime_name,
                    chain=chain,
                    kind=_kind_from_role(proxy["role"], address),
                )
                existing = entries.get(address)
                if existing is not None:
                    raise ValueError(
                        f"axis-synome contract has duplicate proxy address {address}: "
                        f"{existing.prime_name}/{existing.chain} and {entry.prime_name}/{entry.chain}"
                    )
                entries[address] = entry

    if not entries:
        raise ValueError("axis-synome contract carries no ALM proxies")
    return entries


_PROXIES: dict[str, ProxyEntry] = _index_proxies()


def proxy_entry(address: str) -> ProxyEntry | None:
    """Return the registry entry for ``address``, or ``None`` if unknown."""
    return _PROXIES.get(address.lower())


def classify_proxy(address: str) -> ProxyKind:
    """Return the :class:`ProxyKind` for a 0x-prefixed proxy address.

    Unknown addresses default to ALM.
    """
    entry = proxy_entry(address)
    return entry.kind if entry is not None else ProxyKind.ALM


def subproxy_addresses() -> frozenset[str]:
    """Return the known SubProxy addresses (lowercase, 0x-prefixed).

    Raises if the contract has no SubProxy entries.
    """
    addresses = frozenset(entry.address for entry in _PROXIES.values() if entry.kind is ProxyKind.SUB_PROXY)
    if not addresses:
        raise ValueError("axis-synome contract carries no SubProxy entries")
    return addresses


def prime_name_for(address: str) -> str | None:
    """Return the prime name owning ``address``, or ``None`` if unknown."""
    entry = proxy_entry(address)
    return entry.prime_name if entry is not None else None


def alm_proxies_for_prime(prime_name: str) -> tuple[ProxyEntry, ...]:
    """Return the prime's ALM proxies across every chain, ordered by address."""
    return tuple(
        sorted(
            (entry for entry in _PROXIES.values() if entry.prime_name == prime_name and entry.kind is ProxyKind.ALM),
            key=lambda entry: entry.address,
        )
    )
