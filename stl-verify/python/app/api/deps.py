from collections.abc import Callable

from fastapi import Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.adapters.postgres.prime_capital_stack_repository import PrimeCapitalStackRepository
from app.adapters.postgres.receipt_token_repository import ReceiptTokenRepository
from app.adapters.sky.internal_positions_client import SkyInternalPositionsClient
from app.adapters.sky.reference_risk_capital_client import SkyReferenceRiskCapitalClient
from app.auth.fga import FgaError, FgaTruncated
from app.auth.jwt import Principal, TokenError
from app.config import get_settings
from app.ports.receipt_token_lookup import ReceiptTokenLookup
from app.ports.reference_capital_repository import ReferenceCapitalRepository
from app.risk_engine.suraf.result import SurafResult
from app.services.crypto_lending_risk_service import CryptoLendingRiskService
from app.services.model_registry import ModelRegistry
from app.services.reference_positions_service import ReferencePositionsService
from app.services.reference_risk_capital_service import ReferenceRiskCapitalService


def _auth_on(request: Request) -> bool:
    return bool(getattr(request.app.state, "verifier", None))


async def get_principal(request: Request) -> Principal | None:
    """Resolve the caller from the token the app verifies ITSELF.

    Never from proxy-written claim headers — edge-agnostic, so the same code
    serves the Tailscale-only present and the Envoy edge later without resting
    on a NetworkPolicy staying correct. With auth off (the default) every caller
    is anonymous (None) and no request-time work happens.
    """
    if not _auth_on(request):
        return None
    cached = getattr(request.state, "principal", None)
    if cached is not None:
        return cached
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(status_code=401, detail="bearer token required", headers={"WWW-Authenticate": "Bearer"})
    try:
        principal = await request.app.state.verifier.verify(token)
    except TokenError as exc:
        raise HTTPException(
            status_code=401, detail=f"invalid token: {exc}", headers={"WWW-Authenticate": "Bearer"}
        ) from exc
    request.state.principal = principal
    return principal


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


async def _prime_address_map(request: Request) -> dict[str, str]:
    """address (vault or proxy) → prime vault address, all lowercase.

    Built from the primes table and cached on app state for 60s. The OpenFGA
    object id for a prime is its vault address — the one identity shared by all
    of a prime's proxies, and what the reconciler writes from Keycloak.
    """
    import time

    st = request.app.state
    cache = getattr(st, "prime_address_map", None)
    if cache and time.monotonic() - cache[0] < 60:
        return cache[1]
    repo = AllocationRepository(st.engine)
    mapping: dict[str, str] = {}
    for p in await repo.list_primes():
        vault = (p.prime_vault_address or "").lower()
        if not vault:
            continue
        mapping[vault] = vault
        mapping[p.address.lower()] = vault
    st.prime_address_map = (time.monotonic(), mapping)
    return mapping


async def require_prime_view(
    prime_id: str, request: Request, principal: Principal | None = Depends(get_principal)
) -> None:
    """Per-resource check for /v1/primes/{prime_id}/* (ADR-011 Plane 2, layer 2).

    Resolves the path address to the prime's vault address and asks OpenFGA
    `can_view`. Unknown address → 404 (same as the handler would say); denied →
    403; OpenFGA unreachable → 503, failing CLOSED.
    """
    if principal is None:
        return
    vault = (await _prime_address_map(request)).get(prime_id.lower())
    if vault is None:
        raise HTTPException(status_code=404, detail="prime not found")
    try:
        allowed = await request.app.state.fga.check(principal.fga_user, "can_view", f"prime:{vault}")
    except FgaError as exc:
        raise HTTPException(status_code=503, detail="authorization service unavailable") from exc
    if not allowed:
        raise HTTPException(status_code=403, detail="not permitted for this prime")


async def allowed_prime_addresses(
    request: Request, principal: Principal | None = Depends(get_principal)
) -> frozenset[str] | None:
    """Every address (vault + proxies) of the primes the caller can view, or
    None when auth is off (meaning: no filtering). Feeds the list endpoints.

    ListObjects at its ceiling raises 500: a partial allow-list pushed into a
    WHERE clause is a correctness bug that looks like missing data.
    """
    if principal is None:
        return None
    try:
        vaults = await request.app.state.fga.list_objects(principal.fga_user, "can_view", "prime")
    except FgaTruncated as exc:
        raise HTTPException(status_code=500, detail="authorization result truncated") from exc
    except FgaError as exc:
        raise HTTPException(status_code=503, detail="authorization service unavailable") from exc
    vaults_l = {v.lower() for v in vaults}
    mapping = await _prime_address_map(request)
    return frozenset(addr for addr, vault in mapping.items() if vault in vaults_l)


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
    """Build the upstream Star-monitor service on demand.

    Returned as a factory, not the service, because FastAPI resolves every
    declared dependency on every request: constructing it eagerly would make a
    self-mode request build an upstream HTTP client it never uses.
    """

    def build() -> ReferenceRiskCapitalService:
        return ReferenceRiskCapitalService(
            SkyReferenceRiskCapitalClient(get_settings().star_risk_capital_base_url),
            ReceiptTokenRepository(request.app.state.engine),
            AllocationRepository(request.app.state.engine),
        )

    return build


def get_reference_positions_service_factory(
    request: Request,
) -> Callable[[], ReferencePositionsService]:
    """Build the upstream balance-sheet service on demand, for the same reason.

    Two upstream clients, because coverage and content come from different
    hosts: the Star monitor decides whether a prime has reference data at all,
    the internal feed says what it holds.
    """

    def build() -> ReferencePositionsService:
        settings = get_settings()
        return ReferencePositionsService(
            SkyInternalPositionsClient(settings.sky_internal_base_url),
            SkyReferenceRiskCapitalClient(settings.star_risk_capital_base_url),
            ReceiptTokenRepository(request.app.state.engine),
            AllocationRepository(request.app.state.engine),
        )

    return build


def get_reference_capital_repository_factory(
    request: Request,
) -> Callable[[], ReferenceCapitalRepository]:
    """Build the stored-reference-snapshot reader on demand, for the same reason."""
    return lambda: PrimeCapitalStackRepository(request.app.state.engine)
