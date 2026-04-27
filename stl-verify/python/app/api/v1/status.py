from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.api.deps import get_engine

router = APIRouter()


@router.get("/status")
def get_status():
    return {"status": "ok"}


@router.get("/ready")
async def get_ready(engine: AsyncEngine = Depends(get_engine)):
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    return {"status": "ok"}
