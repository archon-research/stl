import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

_ETH_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")

SourceType = Literal["onchain", "offchain"]


@dataclass(frozen=True)
class TokenMetadata:
    id: int
    chain_id: int
    address: str
    symbol: str | None
    decimals: int | None
    updated_at: datetime
    metadata: dict[str, Any] | None

    def __post_init__(self) -> None:
        if self.id <= 0:
            raise ValueError(f"id must be positive, got {self.id}")
        if self.chain_id <= 0:
            raise ValueError(f"chain_id must be positive, got {self.chain_id}")
        if not isinstance(self.address, str) or not _ETH_ADDRESS_RE.match(self.address):
            raise ValueError(f"address must be a valid Ethereum address (0x + 40 hex chars), got {self.address!r}")
        if self.symbol is not None and not self.symbol.strip():
            raise ValueError("symbol must be non-empty when present")
        if self.decimals is not None and not (0 <= self.decimals <= 255):
            raise ValueError(f"decimals must be between 0 and 255 when present, got {self.decimals}")


@dataclass(frozen=True)
class TokenPriceQuote:
    token_id: int
    source_type: SourceType
    source_id: int
    source_name: str
    source_display_name: str | None
    price_usd: Decimal
    timestamp: datetime
    staleness_seconds: int

    def __post_init__(self) -> None:
        if self.token_id <= 0:
            raise ValueError(f"token_id must be positive, got {self.token_id}")
        if self.source_type not in ("onchain", "offchain"):
            raise ValueError(f"source_type must be 'onchain' or 'offchain', got {self.source_type!r}")
        if self.source_id <= 0:
            raise ValueError(f"source_id must be positive, got {self.source_id}")
        if not self.source_name or not self.source_name.strip():
            raise ValueError("source_name must be non-empty")
        if self.price_usd.is_nan() or self.price_usd < 0:
            raise ValueError(f"price_usd must be non-negative and finite, got {self.price_usd}")
        if self.staleness_seconds < 0:
            raise ValueError(f"staleness_seconds must be non-negative, got {self.staleness_seconds}")
