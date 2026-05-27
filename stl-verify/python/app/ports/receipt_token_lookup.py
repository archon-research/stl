"""Outbound port for receipt-token metadata lookup.

The risk API uses this to enrich responses with the on-chain identifiers
(``chain_id`` + ``receipt_token_address``) that correspond to the surrogate
``receipt_token_id`` callers pass in. The same port also signals "asset
unknown" with a ``None`` return so handlers can return 404 explicitly.
"""

from typing import Protocol

from app.domain.entities.allocation import EthAddress
from app.domain.entities.receipt_token import ReceiptTokenInfo


class ReceiptTokenLookup(Protocol):
    """Look up a receipt token's on-chain identifiers + protocol routing."""

    async def get(self, receipt_token_id: int) -> ReceiptTokenInfo | None:
        """Return resolved metadata for ``receipt_token_id``, or ``None`` if unknown."""
        ...

    async def get_by_chain_and_address(self, chain_id: int, address: EthAddress) -> ReceiptTokenInfo | None:
        """Return resolved metadata for ``(chain_id, address)``, or ``None`` if unknown.

        ``address`` is matched against ``receipt_token.receipt_token_address``
        (the ALM-proxy / aToken address, not the underlying ERC-20 address).
        """
        ...
