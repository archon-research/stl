from datetime import datetime
from typing import Protocol

from app.domain.entities.orderbook import OrderbookSnapshot


class OrderbookRepository(Protocol):
    async def get_latest_snapshots_for_symbol(self, symbol: str) -> list[OrderbookSnapshot]: ...
    async def get_snapshots_in_range(self, symbol: str, from_dt: datetime, to_dt: datetime) -> list[OrderbookSnapshot]: ...
