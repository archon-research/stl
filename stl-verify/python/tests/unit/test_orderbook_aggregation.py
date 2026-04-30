from datetime import datetime, timezone

from app.domain.entities.orderbook import AggregatedOrderbook, OrderbookLevel, OrderbookSnapshot
from app.services.orderbook_service import aggregate_sell_orderbooks


def _snap(exchange: str, asks: list[tuple[float, float]]) -> OrderbookSnapshot:
    return OrderbookSnapshot(
        exchange=exchange, symbol="BTC", captured_at=datetime.now(tz=timezone.utc),
        bids=[], asks=[OrderbookLevel(price=p, sz=s, liquidity=p * s) for p, s in asks],
    )


def test_aggregate_merges_by_price_level():
    snaps = [
        _snap("binance", [(95000, 1.0), (95100, 2.0)]),
        _snap("bybit", [(95000, 0.5), (95200, 1.0)]),
    ]
    result = aggregate_sell_orderbooks("BTC", snaps)
    assert isinstance(result, AggregatedOrderbook)
    assert result.symbol == "BTC"
    assert set(result.exchanges) == {"binance", "bybit"}
    prices = [level.price for level in result.asks]
    assert prices == sorted(prices)
    level_95000 = next(l for l in result.asks if l.price == 95000)
    assert level_95000.sz == 1.5
    assert level_95000.liquidity == 95000 * 1.5


def test_aggregate_empty_snapshots():
    result = aggregate_sell_orderbooks("BTC", [])
    assert result.asks == []
    assert result.exchanges == []
