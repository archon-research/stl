"""Outbound port for receipt-token metadata lookup.

The risk API uses this to enrich responses with the on-chain identifiers
(``chain_id`` + ``receipt_token_address``) that correspond to the surrogate
``receipt_token_id`` callers pass in. The same port also signals "asset
unknown" with a ``None`` return so handlers can return 404 explicitly.
"""

from dataclasses import dataclass
from typing import Protocol

from app.domain.entities.allocation import EthAddress
from app.domain.entities.receipt_token import ReceiptTokenInfo


@dataclass(frozen=True)
class ReceiptTokenAddressRef:
    """Address-only reference to a receipt token wrapping a given underlying.

    Used by the 404 "did you mean" hint when the caller passes an
    underlying-token address where a receipt-token address is expected.
    """

    chain_id: int
    receipt_token_address: bytes
    symbol: str | None

    @property
    def receipt_token_address_hex(self) -> str:
        return "0x" + self.receipt_token_address.hex()


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

    async def list_receipt_tokens_for_underlying(
        self, chain_id: int, underlying_address: EthAddress
    ) -> list[ReceiptTokenAddressRef]:
        """Return receipt tokens wrapping the underlying ERC-20 at ``(chain_id, address)``.

        Used to produce a helpful 404 when callers pass an underlying-token
        address where a receipt-token address is expected. Returns an empty
        list when nothing matches; never raises for "no rows".
        """
        ...
