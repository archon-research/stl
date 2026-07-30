"""The prime ↔ proxy topology, derived from the axis-synome contract.

A prime is a *set* of proxies: one ALM proxy per chain it allocates on, plus
SubProxy treasury wallets. Every proxy shares the prime's ``prime_id`` in
``allocation_position``, so proxy address alone is not a prime identity and prime
name alone is not a proxy identity — consumers need both directions.

The contract (``axis_synome.export_entities``) already carries
``star -> chain -> [{address, role}]``, which is the same data the Go allocation
tracker configures itself from (``internal/services/allocation_tracker/config.go``).
Reading it here keeps the two languages on one source of truth.

Entries are keyed by address alone. Every address in the contract is distinct
(``_index_proxies`` enforces it), which lets callers resolve a proxy without
knowing its chain — necessary because the contract's chain vocabulary
(``mainnet``, ``avalanche-c``) is not the DB's ``chain.name`` display strings.
Chain *ids* therefore come from ``allocation_position.chain_id``, not from here.
"""

from dataclasses import dataclass
from enum import Enum

from axis_synome.export_entities import build_axis_synome_contract

# The chain a prime's primary proxy lives on. Prime-level figures — the Star
# feed row, the SubProxy treasury, the Anchorage custody leg — all sit behind the
# mainnet ALM proxy, so that is the proxy they are attributed to.
_PRIMARY_CHAIN = "mainnet"

_CONTRACT_ALM_ROLE = "alm"


class ProxyKind(str, Enum):
    """Role of an allocation proxy wallet."""

    ALM = "alm"
    SUB_PROXY = "sub_proxy"


@dataclass(frozen=True)
class ProxyEntry:
    """One proxy wallet, resolved to the prime and chain it belongs to.

    ``address`` is lowercase 0x-prefixed. ``chain`` is the contract's chain
    string, not a DB chain name.
    """

    address: str
    prime_name: str
    chain: str
    kind: ProxyKind


def _kind_from_role(role: str) -> ProxyKind:
    return ProxyKind.ALM if role == _CONTRACT_ALM_ROLE else ProxyKind.SUB_PROXY


def _index_proxies() -> dict[str, ProxyEntry]:
    """Flatten the contract's star -> chain -> [proxy] map, keyed by address.

    Raises on a duplicate address alone, not on duplicate (chain, address) like
    the Go loader (``proxiesFromAlmProxy``): this module resolves a proxy
    without knowing its chain (see module docstring), so a bare address must
    already be a unique key here, and two contract entries sharing one address
    on different chains would be exactly as ambiguous as sharing it on the same
    chain.
    """
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
                    kind=_kind_from_role(proxy["role"]),
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

    An address absent from the contract is treated as ALM: SubProxy is the
    exception that must be positively identified, so an address the contract
    hasn't told us about yet defaults to the common case rather than being
    silently excluded from the ALM set.
    """
    entry = proxy_entry(address)
    return entry.kind if entry is not None else ProxyKind.ALM


def subproxy_addresses() -> frozenset[str]:
    """Return the known SubProxy addresses (lowercase, 0x-prefixed).

    These hold a prime's treasury USDS (its total capital), tracked in
    ``allocation_position`` under the prime's ``prime_id`` but a distinct
    ``proxy_address``. Consumers scope to these to read the treasury series.

    Raises if the contract has no SubProxy entries. ``_index_proxies`` only
    rejects a wholly empty contract, so a regeneration that drops every
    SubProxy would otherwise return an empty set here — turning every
    treasury query's ``IN`` predicate always-false and every
    ``total_risk_capital_usd`` (and the encumbrance ratios built on it)
    silently ``null`` instead of failing loudly.
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
    """Return the prime's ALM proxies across every chain, ordered by address.

    SubProxy treasury wallets are excluded: they hold the denominator (total
    capital), not allocations, so summing them into an exposure or RRC numerator
    would double-count the treasury.
    """
    return tuple(
        sorted(
            (entry for entry in _PROXIES.values() if entry.prime_name == prime_name and entry.kind is ProxyKind.ALM),
            key=lambda entry: entry.address,
        )
    )


def primary_proxy_for_prime(prime_name: str) -> ProxyEntry | None:
    """Return the prime's mainnet ALM proxy, or ``None`` if it has none."""
    for entry in alm_proxies_for_prime(prime_name):
        if entry.chain == _PRIMARY_CHAIN:
            return entry
    return None


def is_primary_proxy(address: str) -> bool:
    """Return whether ``address`` is its prime's primary (mainnet ALM) proxy."""
    entry = proxy_entry(address)
    if entry is None:
        return False
    primary = primary_proxy_for_prime(entry.prime_name)
    return primary is not None and primary.address == entry.address
