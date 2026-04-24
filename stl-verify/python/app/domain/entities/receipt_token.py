from dataclasses import dataclass


@dataclass(frozen=True)
class ReceiptTokenInfo:
    """Resolved receipt token with protocol routing info."""

    receipt_token_id: int
    protocol_id: int
    underlying_token_id: int
    receipt_token_address: bytes
    chain_id: int
    protocol_name: str
    # token.id for the receipt token's own address (distinct from receipt_token.id).
    # Optional because the `token` row is created lazily — typically when an
    # allocation_position is first persisted for this address. Aave-like share
    # lookups need this field; Morpho does not.
    receipt_token_token_id: int | None

    @property
    def receipt_token_address_hex(self) -> str:
        """Return the receipt token address as a 0x-prefixed hex string."""
        return "0x" + self.receipt_token_address.hex()
