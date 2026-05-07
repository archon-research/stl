from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class TokenMetadata:
    id: int
    chain_id: int
    address: str
    symbol: str | None
    decimals: int | None
    updated_at: datetime
    metadata: dict[str, Any] | None


@dataclass(frozen=True)
class TokenPriceQuote:
    token_id: int
    source_type: str
    source_id: int
    source_name: str
    source_display_name: str | None
    price_usd: Decimal
    timestamp: datetime
    staleness_seconds: int
