from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass
class Star:
    name: str


@dataclass
class AllocationPosition:
    id: int
    chain_id: int
    star: str
    proxy_address: str
    token_address: str
    token_symbol: str | None
    token_decimals: int | None
    balance: Decimal
    scaled_balance: Decimal | None
    block_number: int
    block_version: int
    tx_hash: str
    log_index: int
    tx_amount: Decimal
    direction: str
    created_at: datetime
