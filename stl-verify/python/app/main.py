from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.docs import get_swagger_ui_html, get_swagger_ui_oauth2_redirect_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy import text

from app.adapters.postgres.aave_like_backed_breakdown_repository import AaveLikeBackedBreakdownRepository
from app.adapters.postgres.aave_like_liquidation_params_repository import AaveLikeLiquidationParamsRepository
from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.adapters.postgres.backed_breakdown_repository_maple import MapleBackedBreakdownRepository
from app.adapters.postgres.backed_breakdown_repository_morpho import MorphoBackedBreakdownRepository
from app.adapters.postgres.core_model_results_reader import PostgresCoreModelResultsReader
from app.adapters.postgres.crypto_lending_reader import PostgresCryptoLendingReader
from app.adapters.postgres.engine import create_db_engine
from app.adapters.postgres.morpho_liquidation_params_repository import MorphoLiquidationParamsRepository
from app.adapters.postgres.morpho_vault_allocations_reader import PostgresMorphoVaultAllocationsReader
from app.adapters.postgres.receipt_token_repository import ReceiptTokenRepository, resolve_receipt_token_mapping
from app.adapters.postgres.reference_as_of import pinned_to
from app.api.deps import require_analyst, require_viewer
from app.api.v1 import (
    allocations,
    data_sources,
    exposure,
    prime_debts,
    prime_risk_capital,
    protocol_events,
    provenance_availability,
    risk,
    status,
    tokens,
    total_capital,
)
from app.auth.fga import FgaClient
from app.auth.jwt import TokenVerifier
from app.config import Settings, get_settings
from app.logging import get_logger, setup_logging
from app.middleware.request_id import RequestIdMiddleware
from app.risk_engine.core_model.config import load_commented_json
from app.risk_engine.mapping import MappingError, load_asset_mapping
from app.risk_engine.suraf.loader import load_all_ratings
from app.risk_engine.suraf.result import SurafResult
from app.services.core_model_risk_service import MAINNET_CHAIN_ID, CoreModelRiskService, morpho_market_key_index
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


def configure_docs(application: FastAPI, settings: Settings) -> None:
    # With auth on, Swagger gets an Authorize button (authorization-code +
    # PKCE against Keycloak) and the redirect page it needs. The redirect
    # route is NOT auto-registered because docs_url=None — without it the
    # OAuth flow dead-ends silently after login (ADR-015, app-code notes).
    init_oauth = None
    if settings.auth_enabled and settings.oidc_issuer:
        init_oauth = {
            "clientId": "swagger-ui",
            "usePkceWithAuthorizationCodeGrant": True,
            "scopes": "openid profile",
        }

        @application.get("/docs/oauth2-redirect", include_in_schema=False)
        async def swagger_ui_redirect():
            return get_swagger_ui_oauth2_redirect_html()

    @application.get("/docs", include_in_schema=False)
    async def swagger_ui_html():
        openapi_url = application.openapi_url or "/openapi.json"
        return get_swagger_ui_html(
            openapi_url=openapi_url,
            title=f"{application.title} - Swagger UI",
            swagger_favicon_url=DOCS_FAVICON_URL,
            oauth2_redirect_url="/docs/oauth2-redirect" if init_oauth else None,
            init_oauth=init_oauth,
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

    core_raw_mapping = load_asset_mapping(settings.core_model_mappings_file)
    logger.info("core model asset->market_key mapping loaded entries=%d", len(core_raw_mapping))

    core_morpho_market_keys = morpho_market_key_index(load_commented_json(settings.core_model_market_configs_file))
    logger.info("core model morpho market keys loaded entries=%d", len(core_morpho_market_keys))

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Initialised before the try: the finally below closes it, and startup
        # can raise long before the auth block runs (e.g. mapping validation) —
        # a late declaration turns that real error into an UnboundLocalError.
        auth_http: httpx.AsyncClient | None = None
        engine = create_db_engine(
            settings.async_database_url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_timeout=settings.db_pool_timeout,
            pool_recycle=settings.db_pool_recycle_seconds,
            statement_cache_size=settings.db_statement_cache_size,
        )
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))

            asset_to_rating = await resolve_receipt_token_mapping(raw_mapping, engine)
            # Published on app.state so every route resolves the same provider via
            # deps.get_reference_as_of; a per-route default would leave most unpinned.
            reference_effective_at = pinned_to(settings.resolved_reference_effective_at())
            app.state.reference_effective_at = reference_effective_at
            allocation_repo = AllocationRepository(engine, reference_effective_at)
            suraf_rrc_service = SurafRrcService(asset_to_rating, suraf_ratings, allocation_repo)

            receipt_token_repo = ReceiptTokenRepository(engine)
            crypto_lending_reader = PostgresCryptoLendingReader(
                receipt_token_repo=receipt_token_repo,
                aave_breakdown_repo=AaveLikeBackedBreakdownRepository(engine, reference_effective_at),
                morpho_breakdown_repo=MorphoBackedBreakdownRepository(engine, reference_effective_at),
                maple_breakdown_repo=MapleBackedBreakdownRepository(engine),
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
            asset_to_market_key = await resolve_receipt_token_mapping(core_raw_mapping, engine)
            core_model_results_reader = PostgresCoreModelResultsReader(engine)
            # Same startup-snapshot rule as the crypto-lending set above: a
            # Morpho receipt token registered after boot needs a restart.
            # Mainnet only — CORE market keys are Ethereum-only, and symbol-pair
            # matching would hand another chain's vault mainnet results.
            morpho_asset_ids = await crypto_lending_reader.list_morpho_asset_ids(chain_id=MAINNET_CHAIN_ID)
            core_model_risk_service = CoreModelRiskService(
                asset_to_market_key=asset_to_market_key,
                results_reader=core_model_results_reader,
                allocation_repo=allocation_repo,
                receipt_tokens=receipt_token_repo,
                morpho_allocations=PostgresMorphoVaultAllocationsReader(engine),
                morpho_market_keys=core_morpho_market_keys,
                morpho_asset_ids=morpho_asset_ids,
                min_coverage_pct=settings.core_model_min_coverage_pct,
            )
            model_registry = ModelRegistry([suraf_rrc_service, crypto_lending_risk_service, core_model_risk_service])

            app.state.engine = engine
            app.state.suraf_ratings = suraf_ratings
            app.state.asset_to_rating = asset_to_rating
            app.state.crypto_lending_risk_service = crypto_lending_risk_service
            app.state.model_registry = model_registry
            app.state.receipt_token_lookup = receipt_token_repo

            # Auth plane (ADR-015). Built here, beside the engine, so it is
            # disposed in the same finally. Absent from app.state when auth is
            # off — the dependencies treat that as "anonymous, no checks".
            if settings.auth_enabled:
                auth_http = httpx.AsyncClient()
                app.state.verifier = TokenVerifier(
                    issuer=settings.oidc_issuer,
                    audience=settings.oidc_audience,
                    http=auth_http,
                    jwks_url=settings.oidc_jwks_url or None,
                )
                app.state.fga = FgaClient(
                    base_url=settings.openfga_url,
                    api_key=settings.openfga_api_key.get_secret_value(),
                    store_name=settings.openfga_store_name,
                    http=auth_http,
                    list_ceiling=settings.openfga_list_ceiling,
                )

            instrument_sqlalchemy_engine(engine)
            yield
        finally:
            try:
                if auth_http is not None:
                    await auth_http.aclose()
                await engine.dispose()
            finally:
                shutdown_telemetry(app.state.telemetry_providers)

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
    application.state.telemetry_providers = setup_telemetry(application, settings)

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

    # Coarse RBAC per ROUTER, never as global middleware: the probes on
    # status.router are reached by kubelet directly and would 401 → CrashLoop.
    # Gates are no-ops while auth_enabled is false.
    viewer = [Depends(require_viewer)]
    analyst = [Depends(require_analyst)]  # /v1/risk/* incl. bad-debt: org:analyst+
    application.include_router(status.router, prefix="/v1")
    application.include_router(allocations.router, prefix="/v1", dependencies=viewer)
    application.include_router(tokens.router, prefix="/v1", dependencies=viewer)
    application.include_router(protocol_events.router, prefix="/v1", dependencies=viewer)
    application.include_router(prime_debts.router, prefix="/v1", dependencies=viewer)
    application.include_router(total_capital.router, prefix="/v1", dependencies=viewer)
    application.include_router(prime_risk_capital.router, prefix="/v1", dependencies=viewer)
    application.include_router(exposure.router, prefix="/v1", dependencies=viewer)
    application.include_router(data_sources.router, prefix="/v1", dependencies=viewer)
    application.include_router(provenance_availability.router, prefix="/v1", dependencies=viewer)
    application.include_router(risk.router, prefix="/v1", dependencies=analyst)

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
        full = strip_internal_operations(full)
        # Swagger's Authorize button exists only if the schema declares a
        # security scheme. Emitted only when auth is on, so the published
        # /openapi.json is unchanged while the app ships dark.
        if settings.auth_enabled and settings.oidc_issuer:
            full.setdefault("components", {})["securitySchemes"] = {
                "oidc": {
                    "type": "oauth2",
                    "flows": {
                        "authorizationCode": {
                            "authorizationUrl": f"{settings.oidc_issuer}/protocol/openid-connect/auth",
                            "tokenUrl": f"{settings.oidc_issuer}/protocol/openid-connect/token",
                            "scopes": {"openid": "", "profile": ""},
                        }
                    },
                }
            }
            # Declaring the scheme is not enough: Swagger only attaches the
            # authorized token to operations that carry a security REQUIREMENT.
            # Root-level so every operation inherits it (the probes gain a
            # cosmetic padlock in the docs; they are not gated in the app).
            full["security"] = [{"oidc": []}]
        application.openapi_schema = full
        return application.openapi_schema

    # FastAPI's documented openapi override pattern
    application.openapi = public_openapi  # ty: ignore[invalid-assignment]

    configure_docs(application, settings)
    configure_static_hosting(application, static_dir or DEFAULT_STATIC_DIR)
    return application


app = create_app(get_settings())
