from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.orderbook import OrderbookLevel, OrderbookSnapshot


class PostgresOrderbookRepository:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def get_latest_snapshots_for_symbol(self, symbol: str) -> list[OrderbookSnapshot]:
        query = text("""
            SELECT DISTINCT ON (exchange)
                exchange, symbol, captured_at, bid_data, ask_data
            FROM cex_orderbook_snapshots
            WHERE symbol = :symbol
            ORDER BY exchange, captured_at DESC
        """)
        async with self._engine.connect() as conn:
            result = await conn.execute(query, {"symbol": symbol})
            rows = result.fetchall()
        return [self._row_to_snapshot(row) for row in rows]

    async def get_snapshots_in_range(self, symbol: str, from_dt: datetime, to_dt: datetime) -> list[OrderbookSnapshot]:
        query = text("""
            SELECT exchange, symbol, captured_at, bid_data, ask_data
            FROM cex_orderbook_snapshots
            WHERE symbol = :symbol AND captured_at BETWEEN :from_dt AND :to_dt
            ORDER BY captured_at DESC
        """)
        async with self._engine.connect() as conn:
            result = await conn.execute(query, {"symbol": symbol, "from_dt": from_dt, "to_dt": to_dt})
            rows = result.fetchall()
        return [self._row_to_snapshot(row) for row in rows]

    @staticmethod
    def _row_to_snapshot(row) -> OrderbookSnapshot:
        return OrderbookSnapshot(
            exchange=row.exchange,
            symbol=row.symbol,
            captured_at=row.captured_at,
            bids=[OrderbookLevel(**level) for level in row.bid_data],
            asks=[OrderbookLevel(**level) for level in row.ask_data],
        )
