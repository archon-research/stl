from collections import defaultdict
from datetime import datetime, timezone

from app.domain.entities.orderbook import AggregatedOrderbook, OrderbookLevel, OrderbookSnapshot


def aggregate_sell_orderbooks(symbol: str, snapshots: list[OrderbookSnapshot]) -> AggregatedOrderbook:
    if not snapshots:
        return AggregatedOrderbook(symbol=symbol, exchanges=[], captured_at=datetime.now(tz=timezone.utc), asks=[])

    price_to_size: dict[float, float] = defaultdict(float)
    exchanges = []
    latest_time = snapshots[0].captured_at

    for snap in snapshots:
        exchanges.append(snap.exchange)
        if snap.captured_at > latest_time:
            latest_time = snap.captured_at
        for level in snap.asks:
            price_to_size[level.price] += level.sz

    asks = sorted(
        [OrderbookLevel(price=price, sz=sz, liquidity=price * sz) for price, sz in price_to_size.items()],
        key=lambda l: l.price,
    )
    return AggregatedOrderbook(symbol=symbol, exchanges=sorted(set(exchanges)), captured_at=latest_time, asks=asks)
