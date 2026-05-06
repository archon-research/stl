import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

_TX_HASH_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
_ETH_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


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

    def __post_init__(self) -> None:
        if not isinstance(self.tx_hash, str) or not _TX_HASH_RE.match(self.tx_hash):
            raise ValueError(f"tx_hash must be a valid transaction hash (0x + 64 hex chars), got {self.tx_hash!r}")
        if self.log_index < 0:
            raise ValueError(f"log_index must be non-negative, got {self.log_index}")
        if self.chain_id <= 0:
            raise ValueError(f"chain_id must be positive, got {self.chain_id}")
        if self.block_number <= 0:
            raise ValueError(f"block_number must be positive, got {self.block_number}")
        if self.block_version < 0:
            raise ValueError(f"block_version must be non-negative, got {self.block_version}")
        if not self.protocol_name or not self.protocol_name.strip():
            raise ValueError("protocol_name must be non-empty")
        if not self.event_name or not self.event_name.strip():
            raise ValueError("event_name must be non-empty")
        if not isinstance(self.contract_address, str) or not _ETH_ADDRESS_RE.match(self.contract_address):
            raise ValueError(
                f"contract_address must be a valid Ethereum address (0x + 40 hex chars), got {self.contract_address!r}"
            )
