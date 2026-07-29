"""Internal chain names, keyed by EVM chain id.

These are the names the allocation trackers are configured with and the names
the axis-synome contract uses — not the DB ``chain`` table's display strings
(``Ethereum Mainnet``), which do not match either.

Kept in lockstep with ``entity.ChainIDToName`` in
``stl-verify/internal/domain/entity/chain.go``; ``test_chain_names.py`` reads that
file and fails if the two diverge.
"""

CHAIN_ID_TO_NAME: dict[int, str] = {
    1: "mainnet",
    10: "optimism",
    130: "unichain",
    8453: "base",
    42161: "arbitrum",
    43114: "avalanche-c",
}


def chain_name_for(chain_id: int) -> str | None:
    """Return the internal chain name for ``chain_id``, or ``None`` if unknown.

    Returns ``None`` rather than raising: an unrecognised chain id means a
    position exists on a chain this vocabulary has not been taught, which must
    surface as a null field on the response rather than a failed request.
    """
    return CHAIN_ID_TO_NAME.get(chain_id)
