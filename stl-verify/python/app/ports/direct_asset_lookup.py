"""Outbound port for direct-asset-holding lookups by on-chain address.

Direct asset holdings are raw ERC-20 balances held in a prime's proxy wallet
without a protocol wrapper (no receipt token). This port lets the risk
breakdown endpoint fall back to a self-backed breakdown when the address
does not resolve as a receipt token.
"""

from typing import Protocol

from app.domain.entities.allocation import DirectAssetHolding, EthAddress


class DirectAssetLookup(Protocol):
    """Look up a direct asset holding by chain and token address."""

    async def get_by_chain_and_address(self, chain_id: int, token_address: EthAddress) -> DirectAssetHolding | None:
        """Return the direct asset holding at ``(chain_id, address)``, or ``None``.

        Searches across all known prime proxy wallets for a non-receipt-token
        position at the given address.
        """
        ...
