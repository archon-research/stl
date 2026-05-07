from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class PrimeDebtSnapshot:
    """Prime debt snapshot enriched with prime metadata."""

    prime_address: str
    prime_name: str
    ilk_name: str
    debt_wad: Decimal
    block_number: int
    block_version: int
    synced_at: datetime
