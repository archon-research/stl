from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class ProtocolEvent:
    """Protocol event detail with transaction context."""

    tx_hash: str
    log_index: int
    chain_id: int
    block_number: int
    block_version: int
    protocol_name: str
    event_name: str
    contract_address: str
    event_data: dict[str, Any] | None
    created_at: datetime
