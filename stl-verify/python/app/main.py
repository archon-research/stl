from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import FileResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.receipt_token_repository import resolve_receipt_token_mapping
from app.api.v1 import allocations, orderbooks, risk, status
from app.config import Settings, get_settings
from app.logging import get_logger, setup_logging
from app.middleware.request_id import RequestIdMiddleware
from app.risk_engine.mapping import MappingError, load_asset_mapping
from app.risk_engine.suraf.loader import load_all_ratings
from app.risk_engine.suraf.result import SurafResult
from app.services.suraf_rrc_service import SurafRrcService
from app.telemetry import instrument_sqlalchemy_engine, setup_telemetry, shutdown_telemetry

logger = get_logger(__name__)

APP_DIR = Path(__file__).resolve().parent
DEFAULT_STATIC_DIR = APP_DIR / "static"
RESERVED_FRONTEND_PREFIXES = ("v1", "docs", "redoc", "openapi.json")
DOCS_FAVICON_URL = "/assets/archon-32.png"


def _check_mapping_refs(
    raw_mapping: list[tuple[int, bytes, str]],
    ratings: dict[str, SurafResult],
) -> None:
    unknown = sorted(set(r_id for _, _, r_id in raw_mapping) - set(ratings))
    if unknown:
        raise MappingError(f"asset mapping references unknown rating_ids: {unknown}")


def configure_static_hosting(application: FastAPI, static_dir: Path) -> None:
    index_file = static_dir / "index.html"
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


def configure_docs(application: FastAPI) -> None:
    @application.get("/docs", include_in_schema=False)
    async def swagger_ui_html():
        openapi_url = application.openapi_url or "/openapi.json"
        return get_swagger_ui_html(
            openapi_url=openapi_url,
            title=f"{application.title} - Swagger UI",
            swagger_favicon_url=DOCS_FAVICON_URL,
        )


def create_app(settings: Settings, static_dir: Path | None = None) -> FastAPI:
    setup_logging(log_level=settings.log_level, log_format=settings.log_format)

    # Validate risk-engine config before acquiring any resources so a bad
    # configuration fails startup without leaking a telemetry provider or
    # DB engine.  File-based checks (ratings, mapping shape, rating_id
    # cross-refs) run here.  DB-dependent resolution (composite key ->
    # receipt_token_id) runs in the lifespan after the engine is created.
    logger.info(
        "starting stl-verify git_commit=%s suraf_inputs_dir=%s suraf_mappings_file=%s",
        settings.git_commit,
        settings.suraf_inputs_dir,
        settings.suraf_mappings_file,
    )
    suraf_ratings = load_all_ratings(
        settings.suraf_inputs_dir,
        source_commit_sha=settings.git_commit,
    )
    raw_mapping = load_asset_mapping(settings.suraf_mappings_file)
    _check_mapping_refs(raw_mapping, suraf_ratings)
    logger.info("asset->rating mapping loaded entries=%d", len(raw_mapping))

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_async_engine(settings.async_database_url, pool_pre_ping=True)
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))

            asset_to_rating = await resolve_receipt_token_mapping(raw_mapping, engine)
            suraf_rrc_service = SurafRrcService(asset_to_rating, suraf_ratings)

            app.state.engine = engine
            app.state.suraf_ratings = suraf_ratings
            app.state.asset_to_rating = asset_to_rating
            app.state.suraf_rrc_service = suraf_rrc_service

            instrument_sqlalchemy_engine(engine)
            yield
        finally:
            try:
                await engine.dispose()
            finally:
                shutdown_telemetry(app.state.tracer_provider)

    application = FastAPI(title="stl-verify", lifespan=lifespan, docs_url=None)
    application.add_middleware(RequestIdMiddleware)
    application.state.tracer_provider = setup_telemetry(application, settings)
    application.include_router(status.router, prefix="/v1")
    application.include_router(allocations.router, prefix="/v1")
    application.include_router(risk.router, prefix="/v1")
    application.include_router(orderbooks.router, prefix="/v1")
    configure_docs(application)
    configure_static_hosting(application, static_dir or DEFAULT_STATIC_DIR)
    return application


app = create_app(get_settings())
