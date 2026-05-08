import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

_ETH_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


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

    def __post_init__(self) -> None:
        if not isinstance(self.prime_address, str) or not _ETH_ADDRESS_RE.match(self.prime_address):
            raise ValueError(
                f"prime_address must be a valid Ethereum address (0x + 40 hex chars), got {self.prime_address!r}"
            )
        if not self.prime_name or not self.prime_name.strip():
            raise ValueError("prime_name must be non-empty")
        if not self.ilk_name or not self.ilk_name.strip():
            raise ValueError("ilk_name must be non-empty")
        if self.debt_wad < 0:
            raise ValueError(f"debt_wad must be non-negative, got {self.debt_wad}")
        if self.block_number <= 0:
            raise ValueError(f"block_number must be positive, got {self.block_number}")
        if self.block_version < 0:
            raise ValueError(f"block_version must be non-negative, got {self.block_version}")
