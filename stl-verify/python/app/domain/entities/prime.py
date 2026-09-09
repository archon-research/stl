"""Prime identity — what every accepted external identifier resolves to."""

from dataclasses import dataclass

from app.domain.entities.allocation import EthAddress


@dataclass(frozen=True, slots=True)
class PrimeIdentity:
    """One prime, reached by name, vault address, ALM proxy or SubProxy address.

    ``id`` is the canonical internal identifier and never leaves the API — its
    numbering is environment-specific. ``prime_key`` is the opaque public handle:
    minted once and never changed, unlike ``name`` and ``vault_address``, which are
    time-varying attributes of the prime rather than its identity.
    """

    id: int
    name: str
    prime_key: str
    vault_address: EthAddress
