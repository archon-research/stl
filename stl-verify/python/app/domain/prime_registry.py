"""The prime ↔ proxy topology, derived from the axis-synome contract.

A prime is a *set* of proxies: one ALM proxy per chain it allocates on, plus
SubProxy treasury wallets. Every proxy shares the prime's ``prime_id`` in
``allocation_position``, so proxy address alone is not a prime identity and prime
name alone is not a proxy identity — consumers need both directions.

The contract (``app.risk_engine._vendored_synome.export_entities``) already carries
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
    """One proxy wallet, resolved to the prime and chain it belongs to.

    ``address`` is lowercase 0x-prefixed. ``chain`` is the contract's chain
    string, not a DB chain name.
    """

    address: str
    prime_name: str
    chain: str
    kind: ProxyKind


# The contract's role vocabulary, imported rather than spelled out so an upstream
# rename fails at import instead of silently reclassifying a wallet. Note the
# vocabularies differ: the contract says ``subproxy``, ``ProxyKind.SUB_PROXY``
# serializes as ``sub_proxy``.
_CONTRACT_ROLES: dict[str, ProxyKind] = {
    PROXY_ROLE_ALM: ProxyKind.ALM,
    PROXY_ROLE_SUBPROXY: ProxyKind.SUB_PROXY,
}


def _kind_from_role(role: str, address: str) -> ProxyKind:
    """Map a contract role string to a :class:`ProxyKind`, rejecting the unknown.

    Defaulting an unrecognised role to either kind is unsafe in a way that does
    not surface: as SubProxy it drops the proxy from ``/v1/primes`` and every
    ``prime_`` sum *and* folds its balance into the treasury denominator, so a
    position silently leaves the numerator and reappears below the line. A third
    role added upstream is a contract change we must be told about.
    """
    try:
        return _CONTRACT_ROLES[role]
    except KeyError:
        raise ValueError(
            f"axis-synome contract proxy {address} has unrecognised role {role!r}; "
            f"expected one of {sorted(_CONTRACT_ROLES)}"
        ) from None


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


# Which proxy of a prime carries its prime-scoped rows is deliberately NOT
# answered here. It is answered from the `prime_proxy` table, which holds the same
# declared universe this module reads and is what /v1/primes is built from, so
# server and client cannot disagree. That table is the source of truth for the
# proxy set on the DB side; endpoints for a declared proxy may be empty until data
# arrives, which is expected rather than an error.
# See AllocationRepository.primary_proxy_address.
