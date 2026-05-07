"""Outbound port for receipt-token metadata lookup.

The risk API uses this to enrich responses with the on-chain identifiers
(``chain_id`` + ``receipt_token_address``) that correspond to the surrogate
``receipt_token_id`` callers pass in. The same port also signals "asset
unknown" with a ``None`` return so handlers can return 404 explicitly.
"""

from typing import Protocol

from app.domain.entities.receipt_token import ReceiptTokenInfo


class ReceiptTokenLookup(Protocol):
    """Look up a receipt token's on-chain identifiers + protocol routing."""

    async def get(self, receipt_token_id: int) -> ReceiptTokenInfo | None:
        """Return resolved metadata for ``receipt_token_id``, or ``None`` if unknown."""
        ...
