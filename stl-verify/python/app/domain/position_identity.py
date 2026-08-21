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
        # STL's own registry id, so both sides mean the same token by it — and it
        # needs no chain to qualify it, unlike everything below.
        candidates.append(f"token:{facts.receipt_token_id}")

    chain = _chain_key(facts)
    address = (facts.position_address or "").lower()
    if chain is not None and len(address) == _ADDRESS_LENGTH:
        candidates.append(f"position:{chain}:{address}")

    if candidates:
        return candidates

    if chain is None:
        # Nothing left to key on but a symbol, and no chain to qualify it. Two
        # vaults can share a symbol, so a placeholder chain would let positions
        # on two unidentified chains key alike; no key at all leaves the row
        # standing on its own in the union, which is the honest answer.
        return []

    # A pool id, or a token neither side could resolve.
    return [f"symbol:{chain}:{facts.protocol_name or ''}:{facts.symbol.lower()}"]


def _chain_key(facts: PositionFacts) -> str | None:
    """The chain, however it is known, or ``None`` when it is not known at all.

    A null id means a chain STL has no number for, where the upstream network
    name is the only identifier — and two such chains must not share a key.
    """
    if facts.chain_id is not None:
        return str(facts.chain_id)
    return f"net:{facts.network}" if facts.network else None
