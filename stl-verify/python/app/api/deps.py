from collections.abc import Callable

from fastapi import Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.adapters.postgres.prime_capital_stack_repository import PrimeCapitalStackRepository
from app.adapters.postgres.reference_position_repository import ReferencePositionRepository
from app.adapters.postgres.reference_risk_capital_repository import ReferenceRiskCapitalRepository
from app.auth.fga import FgaError, FgaTruncated
from app.auth.jwt import Principal, TokenError
from app.config import get_settings
from app.domain.entities.allocation import EthAddress
from app.ports.receipt_token_lookup import ReceiptTokenLookup
from app.ports.reference_capital_repository import ReferenceCapitalRepository
from app.risk_engine.suraf.result import SurafResult
from app.services.crypto_lending_risk_service import CryptoLendingRiskService
from app.services.model_registry import ModelRegistry
from app.services.reference_positions_service import ReferencePositionsService
from app.services.reference_risk_capital_service import ReferenceRiskCapitalService


async def get_principal(request: Request) -> Principal | None:
    """Resolve the caller from the token the app verifies ITSELF.

    Never from proxy-written claim headers — edge-agnostic, so the same code
    serves the Tailscale-only present and the Envoy edge later. With
    auth_enabled=False (the default) every caller is anonymous (None) and no
    request-time work happens. FastAPI caches this per request, so gates and
    handlers share one resolution without any manual state.
    """
    if not get_settings().auth_enabled:
        return None
    verifier = getattr(request.app.state, "verifier", None)
    if verifier is None:
        # Enabled but the lifespan never built a verifier: fail CLOSED rather
        # than treating everyone as anonymous.
        raise HTTPException(status_code=503, detail="auth enabled but verifier not initialised")
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(status_code=401, detail="bearer token required", headers={"WWW-Authenticate": "Bearer"})
    try:
        return await verifier.verify(token)
    except TokenError as exc:
        raise HTTPException(
            status_code=401, detail=f"invalid token: {exc}", headers={"WWW-Authenticate": "Bearer"}
        ) from exc


def require_role(role: str) -> Callable:
    """Coarse RBAC gate (ADR-011 Plane 2, layer 1) — applied per ROUTER.

    Never as global middleware: kubelet probes hit /v1/status and /v1/ready
    directly and would 401 → CrashLoop. Keycloak expands composites into
    realm_access.roles, so org:admin tokens also carry org:analyst and org:viewer.
    """

    async def _dep(request: Request, principal: Principal | None = Depends(get_principal)) -> None:
        if principal is None:  # auth off
            return
        if role not in principal.roles:
            raise HTTPException(status_code=403, detail=f"requires role {role}")

    return _dep


require_viewer = require_role("org:viewer")
require_analyst = require_role("org:analyst")


async def _vault_for(request: Request, address: str) -> str | None:
    """Vault address for a vault-or-proxy address — one indexed point query."""
    repo = AllocationRepository(request.app.state.engine)
    return await repo.get_prime_vault_address(EthAddress(address))


async def require_prime_view(request: Request, principal: Principal | None = Depends(get_principal)) -> None:
    """Per-resource check for /v1/primes/{prime_id}/* (ADR-011 Plane 2, layer 2).

    Reads the path param from the request rather than declaring it: a declared
    parameter is merged into the OpenAPI operation and overrides each route's
    own annotated description. The route's own param does the format validation.
    The OpenFGA object id is the prime's VAULT address — the identity shared by
    all of a prime's proxies, and what the reconciler writes.
    """
    if principal is None:
        return
    vault = await _vault_for(request, request.path_params.get("prime_id", ""))
    if vault is None:
        raise HTTPException(status_code=404, detail="prime not found")
    try:
        allowed = await request.app.state.fga.check(principal.fga_user, "can_view", f"prime:{vault.lower()}")
    except FgaError as exc:
        raise HTTPException(status_code=503, detail="authorization service unavailable") from exc
    if not allowed:
        raise HTTPException(status_code=403, detail="not permitted for this prime")


async def allowed_prime_vaults(
    request: Request, principal: Principal | None = Depends(get_principal)
) -> frozenset[str] | None:
    """Vault addresses of the primes the caller may view; None = auth off (no
    filtering). Consumers push this into the QUERY so authorization applies
    before ORDER BY/LIMIT. At the ListObjects ceiling this raises: a silently
    partial allow-list is a correctness bug that looks like missing data.
    """
    if principal is None:
        return None
    try:
        vaults = await request.app.state.fga.list_objects(principal.fga_user, "can_view", "prime")
    except FgaTruncated as exc:
        raise HTTPException(status_code=500, detail="authorization result truncated") from exc
    except FgaError as exc:
        raise HTTPException(status_code=503, detail="authorization service unavailable") from exc
    return frozenset(v.lower() for v in vaults)


def get_engine(request: Request) -> AsyncEngine:
    """Extract the shared SQLAlchemy engine from application state."""
    return request.app.state.engine


def get_suraf_ratings(request: Request) -> dict[str, SurafResult]:
    """Extract the SURAF rating_id -> result lookup built at startup."""
    return request.app.state.suraf_ratings


def get_asset_to_rating(request: Request) -> dict[int, str]:
    """Extract the receipt_token_id -> rating_id mapping built at startup."""
    return request.app.state.asset_to_rating


def get_crypto_lending_risk_service(request: Request) -> CryptoLendingRiskService:
    """Extract the crypto-lending risk service built at startup."""
    return request.app.state.crypto_lending_risk_service


def get_model_registry(request: Request) -> ModelRegistry:
    """Extract the model registry built at startup."""
    return request.app.state.model_registry


def get_receipt_token_lookup(request: Request) -> ReceiptTokenLookup:
    """Extract the receipt-token lookup built at startup."""
    return request.app.state.receipt_token_lookup


def get_reference_risk_capital_service_factory(
    request: Request,
) -> Callable[[], ReferenceRiskCapitalService]:
    """Build the stored-reference risk-capital service on demand.

    Returned as a factory, not the service, because FastAPI resolves every
    declared dependency on every request: a self-mode request would otherwise
    construct a reader it never calls. Matches the two sibling factories below.
    """

    def build() -> ReferenceRiskCapitalService:
        return ReferenceRiskCapitalService(
            ReferenceRiskCapitalRepository(request.app.state.engine),
            AllocationRepository(request.app.state.engine),
        )

    return build


def get_reference_positions_service_factory(
    request: Request,
) -> Callable[[], ReferencePositionsService]:
    """Build the stored-reference balance-sheet service on demand, for the same reason."""

    def build() -> ReferencePositionsService:
        return ReferencePositionsService(
            ReferencePositionRepository(request.app.state.engine),
            AllocationRepository(request.app.state.engine),
        )

    return build


def get_reference_capital_repository_factory(
    request: Request,
) -> Callable[[], ReferenceCapitalRepository]:
    """Build the stored-reference-snapshot reader on demand, for the same reason."""
    return lambda: PrimeCapitalStackRepository(request.app.state.engine)
