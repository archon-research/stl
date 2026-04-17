from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.api.v1 import allocations, risk, status
from app.config import Settings, get_settings
from app.logging import setup_logging
from app.middleware.request_id import RequestIdMiddleware
from app.telemetry import instrument_sqlalchemy_engine, setup_telemetry, shutdown_telemetry

APP_DIR = Path(__file__).resolve().parent
DEFAULT_STATIC_DIR = APP_DIR / "static"
RESERVED_FRONTEND_PREFIXES = ("v1", "docs", "redoc", "openapi.json")


def configure_static_hosting(application: FastAPI, static_dir: Path) -> None:
    index_file = static_dir / "index.html"
    if not index_file.is_file():
        return

    static_root = static_dir.resolve()

    @application.get("/", include_in_schema=False)
    async def serve_root() -> FileResponse:
        return FileResponse(index_file)

    @application.get("/{requested_path:path}", include_in_schema=False)
    async def serve_frontend(requested_path: str) -> FileResponse:
        if _is_reserved_frontend_path(requested_path):
            raise HTTPException(status_code=404, detail="Not Found")

        requested_file = _resolve_static_file(static_root, requested_path)
        if requested_file is not None and requested_file.is_file():
            return FileResponse(requested_file)

        if _is_asset_path(requested_path):
            raise HTTPException(status_code=404, detail="Not Found")

        return FileResponse(index_file)


def _is_reserved_frontend_path(requested_path: str) -> bool:
    return any(
        requested_path == prefix or requested_path.startswith(f"{prefix}/") for prefix in RESERVED_FRONTEND_PREFIXES
    )


def _resolve_static_file(static_root: Path, requested_path: str) -> Path | None:
    candidate = (static_root / requested_path).resolve()
    try:
        candidate.relative_to(static_root)
    except ValueError:
        return None
    return candidate


def _is_asset_path(requested_path: str) -> bool:
    return requested_path.split("/", 1)[0] == "assets"


def create_app(settings: Settings, static_dir: Path | None = None) -> FastAPI:
    setup_logging(log_level=settings.log_level, log_format=settings.log_format)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(settings.async_database_url, pool_pre_ping=True)
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        app.state.engine = engine
        instrument_sqlalchemy_engine(engine)
        async with httpx.AsyncClient() as http_client:
            app.state.http_client = http_client
            yield
        try:
            await engine.dispose()
        finally:
            shutdown_telemetry(app.state.tracer_provider)

    application = FastAPI(title="stl-verify", lifespan=lifespan)
    application.add_middleware(RequestIdMiddleware)
    application.state.tracer_provider = setup_telemetry(application, settings)
    application.include_router(status.router, prefix="/v1")
    application.include_router(allocations.router, prefix="/v1")
    application.include_router(risk.router, prefix="/v1")
    configure_static_hosting(application, static_dir or DEFAULT_STATIC_DIR)
    return application


app = create_app(get_settings())
