from datetime import datetime

from pydantic import BaseModel


class OrderbookLevel(BaseModel):
    price: float
    sz: float
    liquidity: float  # price * sz


class OrderbookSnapshot(BaseModel):
    exchange: str
    symbol: str
    captured_at: datetime
    bids: list[OrderbookLevel]
    asks: list[OrderbookLevel]


class AggregatedOrderbook(BaseModel):
    symbol: str
    exchanges: list[str]
    captured_at: datetime
    asks: list[OrderbookLevel]
