"""Prime identity — what every accepted external identifier resolves to."""

from collections.abc import Iterable
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

    A prime is a set of wallets — a vault, one ALM proxy per chain, a SubProxy
    treasury — so every prime-scoped figure is answered from more than one of
    them, and how they fold together is a property of the figure. An ADDITIVE
    one is held per wallet and summed over ``alm_proxies``, with ``None``
    meaning unobserved rather than zero, so a sum of nothing is ``None`` and
    never a ``"0"`` that would assert the prime holds nothing where the truth is
    that nothing was indexed. A SHARED one is a single figure for the prime, so
    the wallet set scopes one read — ``subproxies`` for the treasury, the
    resolved ``identity.id`` for debt, custody and the upstream figures —
    because summing it across a prime's proxies multiplies it by their number
    and still reads as a plausible result.

    Each read names its kind and is written the way that kind requires. The
    guarantee is the scoped query, not a check that runs afterwards.

    ``wallets`` is address-sorted and de-duplicated, so two identifiers naming
    one prime produce equal scopes — which is what makes "identical whichever
    form you pass" a property of the type rather than of each call site.

    It is empty for a prime that has a vault and no proxies. That is a
    whole-prime answer over an empty set, not an error.

    ``unserved_chains`` names the chains the axis-synome contract declares for
    this prime that no allocation tracker indexes. Only the contract can say a
    chain exists and is not indexed, which is what keeps a whole-prime total an
    honestly declared lower bound rather than a silent understatement.
    """

    identity: PrimeIdentity
    wallets: tuple[ProxyWallet, ...]
    unserved_chains: tuple[str, ...]

    @classmethod
    def build(
        cls,
        identity: PrimeIdentity,
        wallets: Iterable[ProxyWallet],
        unserved_chains: Iterable[str],
    ) -> "PrimeScope":
        """Sort and de-duplicate ``wallets`` by address."""
        by_address = {wallet.address.lower(): wallet for wallet in wallets}
        return cls(
            identity=identity,
            wallets=tuple(wallet for _, wallet in sorted(by_address.items())),
            unserved_chains=tuple(sorted(set(unserved_chains))),
        )

    @property
    def alm_wallets(self) -> tuple[ProxyWallet, ...]:
        """The allocation proxies — what additive reads fan out over."""
        return tuple(wallet for wallet in self.wallets if wallet.kind is ProxyKind.ALM)

    @property
    def alm_proxies(self) -> tuple[EthAddress, ...]:
        return tuple(wallet.address for wallet in self.alm_wallets)

    @property
    def subproxies(self) -> tuple[EthAddress, ...]:
        """The treasury wallets — what the shared capital reads are scoped to."""
        return tuple(wallet.address for wallet in self.wallets if wallet.kind is ProxyKind.SUB_PROXY)
