"""Prime identity — what every accepted external identifier resolves to."""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from app.domain.entities.allocation import EthAddress
from app.domain.prime_registry import ProxyKind


@dataclass(frozen=True, slots=True)
class PrimeIdentity:
    """One prime, reached by name, vault address, ALM proxy or SubProxy address.

    ``id`` is the canonical internal identifier and never leaves the API — its
    numbering is environment-specific. ``external_id`` is the opaque public handle:
    minted once and never changed, unlike ``name`` and ``vault_address``, which are
    time-varying attributes of the prime rather than its identity.
    """

    id: int
    name: str
    external_id: str
    vault_address: EthAddress


@dataclass(frozen=True, slots=True)
class ProxyWallet:
    """One wallet of a prime, as ``prime_proxy`` records it.

    ``kind`` is not a column: the table declares which prime owns a wallet, and
    the axis-synome contract declares what the wallet is for. An address the
    contract does not list is an ALM proxy, which is what a tracker running
    ahead of the contract pin writes.
    """

    address: EthAddress
    chain_id: int
    kind: ProxyKind


@dataclass(frozen=True, slots=True)
class PrimeScope:
    """Every wallet a prime-scoped read may touch, resolved once per request.

    ``alm_proxies`` and ``subproxies`` are address-sorted and de-duplicated, so
    two identifiers naming one prime produce equal scopes — which is what makes
    "identical whichever form you pass" a property of the type rather than of
    each call site.

    ``alm_proxies`` is empty for a prime that has a vault and no proxies. That
    is a whole-prime answer over an empty set, not an error.

    ``unserved_chains`` names the chains the axis-synome contract declares for
    this prime that no allocation tracker indexes. Only the contract can say a
    chain exists and is not indexed, which is what keeps a whole-prime total an
    honestly declared lower bound rather than a silent understatement.
    """

    identity: PrimeIdentity
    alm_proxies: tuple[EthAddress, ...]
    subproxies: tuple[EthAddress, ...]
    unserved_chains: tuple[str, ...]

    @classmethod
    def build(
        cls,
        identity: PrimeIdentity,
        wallets: Sequence[ProxyWallet],
        unserved_chains: Iterable[str],
    ) -> "PrimeScope":
        """Split ``wallets`` by kind into a sorted, de-duplicated scope."""

        def _sorted(kind: ProxyKind) -> tuple[EthAddress, ...]:
            unique = {wallet.address for wallet in wallets if wallet.kind is kind}
            return tuple(sorted(unique, key=lambda address: address.lower()))

        return cls(
            identity=identity,
            alm_proxies=_sorted(ProxyKind.ALM),
            subproxies=_sorted(ProxyKind.SUB_PROXY),
            unserved_chains=tuple(sorted(set(unserved_chains))),
        )
