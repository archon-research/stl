from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.api.v1 import allocations, risk, status
from app.config import Settings, get_settings
from app.logging import get_logger, setup_logging
from app.middleware.request_id import RequestIdMiddleware
from app.risk_engine.mapping import MappingError, load_asset_mapping
from app.risk_engine.suraf.loader import load_all_ratings
from app.risk_engine.suraf.result import SurafResult
from app.services.suraf_rrc_service import SurafRrcService
from app.telemetry import instrument_sqlalchemy_engine, setup_telemetry, shutdown_telemetry

logger = get_logger(__name__)


def _check_mapping_refs(mapping: dict[str, str], ratings: dict[str, SurafResult]) -> None:
    unknown = sorted(set(mapping.values()) - set(ratings))
    if unknown:
        raise MappingError(f"asset mapping references unknown rating_ids: {unknown}")


def create_app(settings: Settings) -> FastAPI:
    setup_logging(log_level=settings.log_level, log_format=settings.log_format)

    # Validate risk-engine config before acquiring any resources so a bad
    # configuration fails startup without leaking a telemetry provider or
    # DB engine.
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
    asset_to_rating = load_asset_mapping(settings.suraf_mappings_file)
    _check_mapping_refs(asset_to_rating, suraf_ratings)
    logger.info("asset->rating mapping loaded entries=%d", len(asset_to_rating))

    suraf_rrc_service = SurafRrcService(asset_to_rating, suraf_ratings)

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
    application.state.suraf_ratings = suraf_ratings
    application.state.asset_to_rating = asset_to_rating
    application.state.suraf_rrc_service = suraf_rrc_service
    application.add_middleware(RequestIdMiddleware)
    application.state.tracer_provider = setup_telemetry(application, settings)
    application.include_router(status.router, prefix="/v1")
    application.include_router(allocations.router, prefix="/v1")
    application.include_router(risk.router, prefix="/v1")
    return application


app = create_app(get_settings())
