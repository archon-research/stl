from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.orderbook_repository import PostgresOrderbookRepository
from app.api.deps import get_engine
from app.domain.entities.orderbook import OrderbookLevel
from app.services.orderbook_service import aggregate_sell_orderbooks

router = APIRouter()


class SellOrderbookResponse(BaseModel):
    symbol: str
    exchanges: list[str]
    captured_at: str
    levels: list[OrderbookLevel]
    level_count: int


@router.get("/orderbooks/{symbol}/sell", response_model=SellOrderbookResponse)
async def get_aggregated_sell_orderbook(
    symbol: str,
    engine: AsyncEngine = Depends(get_engine),
) -> SellOrderbookResponse:
    repo = PostgresOrderbookRepository(engine)
    snapshots = await repo.get_latest_snapshots_for_symbol(symbol.upper())
    if not snapshots:
        raise HTTPException(status_code=404, detail=f"No orderbook data for {symbol}")
    aggregated = aggregate_sell_orderbooks(symbol.upper(), snapshots)
    return SellOrderbookResponse(
        symbol=aggregated.symbol,
        exchanges=aggregated.exchanges,
        captured_at=aggregated.captured_at.isoformat(),
        levels=aggregated.asks,
        level_count=len(aggregated.asks),
    )


@router.get("/orderbooks/{symbol}/exchanges")
async def get_available_exchanges(
    symbol: str,
    engine: AsyncEngine = Depends(get_engine),
) -> dict:
    repo = PostgresOrderbookRepository(engine)
    snapshots = await repo.get_latest_snapshots_for_symbol(symbol.upper())
    return {
        "symbol": symbol.upper(),
        "exchanges": [
            {"exchange": s.exchange, "captured_at": s.captured_at.isoformat(), "ask_levels": len(s.asks), "bid_levels": len(s.bids)}
            for s in snapshots
        ],
    }
