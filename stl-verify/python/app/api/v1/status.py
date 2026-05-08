from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.api.deps import get_engine

router = APIRouter(tags=["status"])


@router.get(
    "/status",
    summary="Liveness probe",
    description='Returns `{"status": "ok"}` if the process is running. Does not check downstream dependencies.',
)
def get_status():
    return {"status": "ok"}


@router.get(
    "/ready",
    summary="Readiness probe",
    description=(
        'Returns `{"status": "ok"}` if the database is reachable. '
        "Used by orchestrators (e.g. Kubernetes) to gate traffic until the "
        "service can serve requests."
    ),
)
async def get_ready(engine: AsyncEngine = Depends(get_engine)):
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    return {"status": "ok"}
