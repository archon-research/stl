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
