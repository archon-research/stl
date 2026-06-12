from datetime import datetime
from decimal import Decimal
from typing import Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.psm3_repository import PostgresPsm3Repository
from app.api.deps import get_engine
from app.services.psm3_service import Psm3Service

router = APIRouter(tags=["psm3"])

Network = Literal["base", "optimism", "arbitrum", "unichain"]


class Psm3ReservesResponse(BaseModel):
    """PSM3 reserve composition at a block, with USD prices joined at read time."""

    network: str = Field(description="Network the PSM3 instance is deployed on.", examples=["base"])
    address: str = Field(
        description="0x-prefixed PSM3 contract address.",
        examples=["0x1601843c5e9bc251a3272907010afa41fa18347e"],
    )
    usds_balance: Decimal = Field(description="USDS held by the PSM3, in token units.", examples=["1234567.89"])
    susds_balance: Decimal = Field(description="sUSDS held by the PSM3, in token units.", examples=["1234567.89"])
    usdc_balance: Decimal = Field(
        description="USDC held by the PSM3's pocket, in token units.", examples=["1234567.89"]
    )
    usds_balance_usd: Decimal = Field(description="USDS balance valued at `usds_price`.", examples=["1234567.89"])
    susds_balance_usd: Decimal = Field(description="sUSDS balance valued at `susds_price`.", examples=["1296296.28"])
    usdc_balance_usd: Decimal = Field(description="USDC balance valued at `usdc_price`.", examples=["1234567.89"])
    usds_price: Decimal = Field(
        description="USDS/USD market price; par 1.0 when the price feed is missing or stale.",
        examples=["0.9998"],
    )
    usdc_price: Decimal = Field(
        description="USDC/USD market price; par 1.0 when the price feed is missing or stale.",
        examples=["1.0001"],
    )
    susds_price: Decimal = Field(
        description="sUSDS/USD price, computed as conversion rate x `usds_price`.",
        examples=["1.05"],
    )
    total_assets: Decimal = Field(
        description="PSM3.totalAssets() par valuation in token units (not the sum of the `*_usd` fields).",
        examples=["3703703.67"],
    )
    block_number: int = Field(description="Block number the snapshot was observed at.", examples=[30000000])
    block_version: int = Field(description="Cache-key version that increments on chain reorgs.", examples=[0])
    block_timestamp: datetime = Field(description="Timestamp of the observed block.")

    model_config = {
        "json_schema_extra": {
            "example": {
                "network": "base",
                "address": "0x1601843c5e9bc251a3272907010afa41fa18347e",
                "usds_balance": "1234567.89",
                "susds_balance": "1234567.89",
                "usdc_balance": "1234567.89",
                "usds_balance_usd": "1234444.43",
                "susds_balance_usd": "1296296.28",
                "usdc_balance_usd": "1234691.34",
                "usds_price": "0.9999",
                "usdc_price": "1.0001",
                "susds_price": "1.05",
                "total_assets": "3703703.67",
                "block_number": 30000000,
                "block_version": 0,
                "block_timestamp": "2026-06-12T12:00:00Z",
            }
        }
    }


async def _get_psm3_service(engine: AsyncEngine = Depends(get_engine)) -> Psm3Service:
    return Psm3Service(PostgresPsm3Repository(engine))


@router.get(
    "/psm3/reserves",
    response_model=list[Psm3ReservesResponse],
    summary="List latest PSM3 reserves per network",
    description=(
        "Return the latest PSM3 reserve snapshot for every tracked network, with USD "
        "prices joined at read time. Use `network` to filter to a single network; an "
        "unknown network returns `422`."
    ),
)
async def list_psm3_reserves(
    network: Network | None = Query(None, description="Filter to a single network."),
    service: Psm3Service = Depends(_get_psm3_service),
) -> list[Psm3ReservesResponse]:
    reserves = await service.list_reserves(network)
    return [Psm3ReservesResponse(**row.__dict__) for row in reserves]


@router.get(
    "/psm3/reserves/history",
    response_model=list[Psm3ReservesResponse],
    summary="List PSM3 reserve history for a network",
    description=(
        "Return PSM3 reserve snapshots for one network, newest first. All rows are "
        "priced with the current latest USD prices."
    ),
)
async def list_psm3_reserve_history(
    network: Network = Query(description="Network to return history for."),
    limit: int = Query(100, ge=1, le=500, description="Max snapshots returned (default 100, max 500)."),
    service: Psm3Service = Depends(_get_psm3_service),
) -> list[Psm3ReservesResponse]:
    reserves = await service.list_reserve_history(network, limit=limit)
    return [Psm3ReservesResponse(**row.__dict__) for row in reserves]
