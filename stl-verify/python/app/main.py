from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.postgres.aave_like_backed_breakdown_repository import AaveLikeBackedBreakdownRepository
from app.adapters.postgres.aave_like_liquidation_params_repository import AaveLikeLiquidationParamsRepository
from app.adapters.postgres.allocation_position_repository import PostgresAllocationRepository
from app.adapters.postgres.backed_breakdown_repository_morpho import MorphoBackedBreakdownRepository
from app.adapters.postgres.crypto_lending_reader import PostgresCryptoLendingReader
from app.adapters.postgres.morpho_liquidation_params_repository import MorphoLiquidationParamsRepository
from app.adapters.postgres.receipt_token_repository import ReceiptTokenRepository, resolve_receipt_token_mapping
from app.api.v1 import allocations, data_sources, prime_debts, protocol_events, risk, status, tokens
from app.config import Settings, get_settings
from app.logging import get_logger, setup_logging
from app.middleware.request_id import RequestIdMiddleware
from app.risk_engine.mapping import MappingError, load_asset_mapping
from app.risk_engine.suraf.loader import load_all_ratings
from app.risk_engine.suraf.result import SurafResult
from app.services.crypto_lending_risk_service import CryptoLendingRiskService
from app.services.model_registry import ModelRegistry
from app.services.suraf_rrc_service import SurafRrcService
from app.telemetry import instrument_sqlalchemy_engine, setup_telemetry, shutdown_telemetry

logger = get_logger(__name__)

APP_DIR = Path(__file__).resolve().parent
DEFAULT_STATIC_DIR = APP_DIR / "static"
RESERVED_FRONTEND_PREFIXES = ("v1", "docs", "redoc", "openapi.json")
DOCS_FAVICON_URL = "/assets/archon-32.png"

# Operations tagged with this are kept in the source OpenAPI schema (so the
# UI's typed client still gets generated against them) but stripped from the
# public-facing /openapi.json that Swagger UI consumes.
INTERNAL_OPERATION_TAG = "internal"


def strip_internal_operations(schema: dict[str, Any]) -> dict[str, Any]:
    """Return ``schema`` with operations tagged ``internal`` removed."""
    filtered_paths: dict[str, dict[str, Any]] = {}
    for path, methods in schema.get("paths", {}).items():
        kept_methods = {
            method: op
            for method, op in methods.items()
            if not (isinstance(op, dict) and INTERNAL_OPERATION_TAG in op.get("tags", []))
        }
        if kept_methods:
            filtered_paths[path] = kept_methods
    return {**schema, "paths": filtered_paths}


OPENAPI_TAGS: list[dict[str, str]] = [
    {"name": "status", "description": "Liveness and readiness probes."},
    {"name": "primes", "description": "Primes (capital allocators) and their on-chain debt snapshots."},
    {"name": "allocations", "description": "Receipt-token positions held by primes and their activity feed."},
    {"name": "capital", "description": "Per-prime capital metrics (risk capital, first-loss capital, buffers)."},
    {
        "name": "risk",
        "description": "Risk-capital computations: RRC, bad-debt estimates, and risk-enriched breakdowns.",
    },
    {"name": "tokens", "description": "Token catalog metadata and latest USD prices."},
    {"name": "protocol events", "description": "Decoded on-chain events emitted by tracked protocols."},
    {"name": "data sources", "description": "Registry of upstream data sources used by STL."},
    {"name": "metadata", "description": "Reference data for clients (chains, protocols)."},
]


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
            allocation_repo = PostgresAllocationRepository(engine)
            suraf_rrc_service = SurafRrcService(asset_to_rating, suraf_ratings, allocation_repo)

            receipt_token_repo = ReceiptTokenRepository(engine)
            crypto_lending_reader = PostgresCryptoLendingReader(
                receipt_token_repo=receipt_token_repo,
                aave_breakdown_repo=AaveLikeBackedBreakdownRepository(engine),
                morpho_breakdown_repo=MorphoBackedBreakdownRepository(engine),
                aave_liq_repo=AaveLikeLiquidationParamsRepository(engine),
                morpho_liq_repo=MorphoLiquidationParamsRepository(engine),
                engine=engine,
                allocation_share_max_stale_seconds=settings.allocation_share_max_stale_seconds,
            )
            # Snapshot supported crypto-lending assets at startup. New
            # receipt tokens added after startup require a restart to appear
            # in applies_to(), which matches other startup-loaded state such
            # as the SURAF asset mapping.
            supported_crypto_lending_asset_ids = await crypto_lending_reader.list_supported_asset_ids()
            crypto_lending_risk_service = CryptoLendingRiskService(
                reader=crypto_lending_reader,
                default_gap_pct=settings.risk_default_gap_pct,
                supported_asset_ids=supported_crypto_lending_asset_ids,
            )
            model_registry = ModelRegistry([suraf_rrc_service, crypto_lending_risk_service])

            app.state.engine = engine
            app.state.suraf_ratings = suraf_ratings
            app.state.asset_to_rating = asset_to_rating
            app.state.crypto_lending_risk_service = crypto_lending_risk_service
            app.state.model_registry = model_registry
            app.state.receipt_token_lookup = receipt_token_repo

            instrument_sqlalchemy_engine(engine)
            yield
        finally:
            try:
                await engine.dispose()
            finally:
                shutdown_telemetry(app.state.tracer_provider)

    application = FastAPI(
        title="stl-verify",
        description=(
            "Verify-side HTTP API for the STL pipeline.\n\n"
            "Endpoints expose primes (capital allocators), their allocations and debt, "
            "decoded protocol events, token catalog and pricing, and risk-capital "
            "computations (RRC, bad debt, breakdown)."
        ),
        lifespan=lifespan,
        docs_url=None,
        openapi_tags=OPENAPI_TAGS,
    )
    application.add_middleware(RequestIdMiddleware)
    application.state.tracer_provider = setup_telemetry(application, settings)

    @application.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = exc.errors()
        logger.warning(
            "Request validation failed",
            extra={
                "path": request.url.path,
                "method": request.method,
                "validation_error_count": len(errors),
            },
        )
        # Convert validation errors to JSON-serializable format
        serializable_errors = []
        for error in errors:
            serializable_error = {
                "loc": error.get("loc", []),
                "msg": error.get("msg", ""),
                "type": error.get("type", ""),
            }
            # Log input for diagnostics but do not echo it in the response body
            # to avoid reflecting potentially sensitive user-provided data.
            if "input" in error:
                try:
                    raw = error["input"]
                    logger.debug(
                        "Validation error input",
                        extra={
                            "path": request.url.path,
                            "method": request.method,
                            "input_type": type(raw).__name__,
                            "input_len": len(str(raw)),
                        },
                    )
                except Exception:  # noqa: BLE001 - best-effort diagnostic logging
                    pass
            serializable_errors.append(serializable_error)

        return JSONResponse(status_code=422, content={"detail": serializable_errors})

    application.include_router(status.router, prefix="/v1")
    application.include_router(allocations.router, prefix="/v1")
    application.include_router(tokens.router, prefix="/v1")
    application.include_router(protocol_events.router, prefix="/v1")
    application.include_router(prime_debts.router, prefix="/v1")
    application.include_router(data_sources.router, prefix="/v1")
    application.include_router(risk.router, prefix="/v1")

    def public_openapi() -> dict[str, Any]:
        if application.openapi_schema is not None:
            return application.openapi_schema
        full = get_openapi(
            title=application.title,
            version=application.version,
            description=application.description,
            routes=application.routes,
            tags=application.openapi_tags,
        )
        application.openapi_schema = strip_internal_operations(full)
        return application.openapi_schema

    application.openapi = public_openapi

    configure_docs(application)
    configure_static_hosting(application, static_dir or DEFAULT_STATIC_DIR)
    return application


app = create_app(get_settings())
