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

    @property
    def receipt_token_address_hex(self) -> str:
        """Return the receipt token address as a 0x-prefixed hex string."""
        return "0x" + self.receipt_token_address.hex()
