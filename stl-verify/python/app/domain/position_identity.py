"""Identity for one position, stable across the two provenances.

Merging STL's indexed rows with Sky's reported ones needs a key both sides
compute the same way. Every rule below exists because a simpler one merged two
different positions or split one position in two — see the tests, which pin the
cases measured against live data.
"""

from dataclasses import dataclass

_ADDRESS_LENGTH = 42
"""A 0x-prefixed 20-byte address. Uniswap V4 rows carry a 32-byte pool id in the
same field, which is not an address and cannot key against one."""

CUSTODY_PROTOCOL = "anchorage"
"""Off-chain custody, which the two provenances describe differently."""


@dataclass(frozen=True)
class PositionFacts:
    """What either provenance can say about a position, in one shape.

    ``position_address`` is the token that *is* the position — a receipt token,
    or the held asset itself for a direct holding. Never the underlying of a
    wrapped position: two Morpho vaults lending the same asset are two
    positions, and keying on what they lend collapses them into one.
    """

    chain_id: int | None
    network: str | None
    position_address: str | None
    receipt_token_id: int | None
    protocol_name: str | None
    symbol: str


def position_identities(facts: PositionFacts) -> list[str]:
    """Every key this position could be recognised by, strongest first.

    Two rows are the same position when they share any one key. Candidates
    rather than a single key because the provenances do not carry the same kind
    of identifier: the risk-capital breakdown gives STL's rows a registry id and
    no address, and Sky's rows both.

    The symbol is a last resort and only offered when nothing better exists —
    two vaults can share one, so matching on it where an address was available
    would merge different positions.
    """
    if (facts.protocol_name or "").lower() == CUSTODY_PROTOCOL:
        # The two sides agree on nothing else here: STL files the leg off-chain
        # (chain 0, symbol BTC, no address) while upstream reports it on
        # ethereum under its own symbol with a token address.
        return [f"custody:{CUSTODY_PROTOCOL}"]

    candidates: list[str] = []
    if facts.receipt_token_id is not None:
        # STL's own registry id, so both sides mean the same token by it.
        candidates.append(f"token:{facts.receipt_token_id}")

    address = (facts.position_address or "").lower()
    if len(address) == _ADDRESS_LENGTH:
        candidates.append(f"position:{_chain_key(facts)}:{address}")

    if candidates:
        return candidates

    # A pool id, or a token neither side could resolve.
    return [f"symbol:{_chain_key(facts)}:{facts.protocol_name or ''}:{facts.symbol.lower()}"]


def position_identity(facts: PositionFacts) -> str:
    """The strongest single key, for grouping rows within one provenance."""
    return position_identities(facts)[0]


def _chain_key(facts: PositionFacts) -> str:
    """The chain, however it is known.

    A null id means a chain STL has no number for, where the upstream network
    name is the only identifier — and two such chains must not share a key.
    """
    return str(facts.chain_id) if facts.chain_id is not None else f"net:{facts.network or 'unknown'}"
