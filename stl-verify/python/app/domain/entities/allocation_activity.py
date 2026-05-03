from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class AllocationActivityEvent:
    """Single allocation activity event enriched for API consumption."""

    chain_id: int
    prime_address: str
    prime_name: str
    protocol_name: str | None
    token_id: int
    token_symbol: str | None
    action_type: str
    tx_amount: Decimal
    balance: Decimal
    tx_hash: str | None
    log_index: int
    block_number: int
    block_version: int
    created_at: datetime
