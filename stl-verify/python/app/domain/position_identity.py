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


def position_identity(facts: PositionFacts) -> str:
    """Return a key equal for one position however it was reported."""
    if (facts.protocol_name or "").lower() == CUSTODY_PROTOCOL:
        # The two sides agree on nothing else here: STL files the leg off-chain
        # (chain 0, symbol BTC, no address) while upstream reports it on
        # ethereum under its own symbol with a token address.
        return f"custody:{CUSTODY_PROTOCOL}"

    address = (facts.position_address or "").lower()
    if len(address) == _ADDRESS_LENGTH:
        # Preferred because both provenances carry it for almost every row,
        # while the registry id resolves on fewer of them.
        return f"position:{_chain_key(facts)}:{address}"

    if facts.receipt_token_id is not None:
        return f"token:{facts.receipt_token_id}"

    # A pool id, or a token neither side could resolve. Symbol is weak, so it is
    # scoped as tightly as possible.
    return f"symbol:{_chain_key(facts)}:{facts.protocol_name or ''}:{facts.symbol.lower()}"


def _chain_key(facts: PositionFacts) -> str:
    """The chain, however it is known.

    A null id means a chain STL has no number for, where the upstream network
    name is the only identifier — and two such chains must not share a key.
    """
    return str(facts.chain_id) if facts.chain_id is not None else f"net:{facts.network or 'unknown'}"
