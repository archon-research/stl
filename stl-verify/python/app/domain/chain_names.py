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


MAINNET_CHAIN_ID = 1
"""Ethereum mainnet, the chain a prime's prime-scoped rows are attributed to."""

# The chains an allocation tracker is actually deployed for, mirroring
# ``servedTrackerChains`` in
# ``stl-verify/internal/services/allocation_tracker/chains.go``. The contract
# lists ALM proxies on chains no tracker indexes, and those proxies have no
# ``allocation_position`` rows at all — so a figure aggregated over them reads as
# a genuine zero rather than as absent data. Anything summed or reported per chain
# has to know the difference; ``test_chain_names.py`` fails if the two sides drift.
SERVED_TRACKER_CHAINS: frozenset[str] = frozenset(
    {"mainnet", "avalanche-c", "base", "optimism", "unichain", "arbitrum"}
)


def chain_name_for(chain_id: int) -> str | None:
    """Return the internal chain name for ``chain_id``, or ``None`` if unknown.

    Returns ``None`` rather than raising: an unrecognised chain id means a
    position exists on a chain this vocabulary has not been taught, which must
    surface as a null field on the response rather than a failed request.
    """
    return CHAIN_ID_TO_NAME.get(chain_id)


def chain_is_served(chain: str | None) -> bool:
    """Whether an allocation tracker indexes ``chain``.

    ``None`` (a proxy with no discoverable chain) is not served: nothing can be
    asserted about data STL does not collect.
    """
    return chain in SERVED_TRACKER_CHAINS


def chain_id_for(name: str) -> int | None:
    """Return the EVM chain id for an internal chain ``name``, or ``None``.

    Reverse of :func:`chain_name_for`; ``CHAIN_ID_TO_NAME`` values are unique, so
    the mapping is unambiguous.
    """
    for chain_id, chain_name in CHAIN_ID_TO_NAME.items():
        if chain_name == name:
            return chain_id
    return None
