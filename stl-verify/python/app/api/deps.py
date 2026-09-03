import logging
from collections.abc import Callable

from fastapi import Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.adapters.postgres.prime_capital_stack_repository import PrimeCapitalStackRepository
from app.adapters.postgres.reference_as_of import ReferenceEffectiveAtProvider
from app.adapters.postgres.reference_position_repository import ReferencePositionRepository
from app.adapters.postgres.reference_risk_capital_repository import ReferenceRiskCapitalRepository
from app.auth.fga import FgaClient, FgaError, FgaTruncated
from app.auth.jwt import JwksUnavailable, Principal, TokenError
from app.config import get_settings
from app.domain.entities.allocation import EthAddress
from app.logging import get_logger
from app.ports.receipt_token_lookup import ReceiptTokenLookup
from app.ports.reference_capital_repository import ReferenceCapitalRepository
from app.risk_engine.suraf.result import SurafResult
from app.services.crypto_lending_risk_service import CryptoLendingRiskService
from app.services.model_registry import ModelRegistry
from app.services.reference_positions_service import ReferencePositionsService
from app.services.reference_risk_capital_service import ReferenceRiskCapitalService

logger = get_logger(__name__)

# One event name for every authorization outcome, so a single Loki stream
# selector finds the lot: {app="python-api"} | json | event="authz.decision".
AUTHZ_EVENT = "authz.decision"


def _emit_decision(
    request: Request,
    *,
    gate: str,
    decision: str,
    reason: str,
    status: int | None = None,
    principal: Principal | None = None,
    resource: str | None = None,
    fields: dict[str, object] | None = None,
) -> None:
    """Emit one structured authorization decision event (ADR-015 gate 3).

    The field NAMES are the contract here, not the message: they become the
    Loki query surface now and the S3 audit archive later, so renaming one
    breaks a saved query and a retained record. ``request_id`` is attached by
    the formatter from ``RequestIdMiddleware``'s context var — the correlation
    anchor the ADR names — so it is deliberately absent from this signature.

    Denials are WARNING and allows are INFO. Never carries the bearer token,
    an FGA api key, or any other credential: a principal id and a resource id
    are the whole payload.
    """
    payload: dict[str, object] = {
        "event": AUTHZ_EVENT,
        "gate": gate,
        "decision": decision,
        "reason": reason,
        "principal": principal.fga_user if principal is not None else "anonymous",
        "resource": resource if resource is not None else request.url.path,
        "method": request.method,
        "path": request.url.path,
    }
    if status is not None:
        payload["status"] = status
    if fields:
        payload.update(fields)
    logger.log(
        logging.WARNING if decision == "deny" else logging.INFO,
        "authorization %s at %s gate: %s",
        decision,
        gate,
        reason,
        extra=payload,
    )


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
        _emit_decision(request, gate="authn", decision="deny", reason="verifier_unwired", status=503)
        raise HTTPException(status_code=503, detail="auth enabled but verifier not initialised")
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        _emit_decision(request, gate="authn", decision="deny", reason="missing_bearer", status=401)
        raise HTTPException(status_code=401, detail="bearer token required", headers={"WWW-Authenticate": "Bearer"})
    try:
        return await verifier.verify(token)
    except TokenError as exc:
        # PyJWT's own message ("Signature has expired", "Audience doesn't
        # match"). Diagnostic, and never any part of the token itself.
        _emit_decision(
            request, gate="authn", decision="deny", reason="invalid_token", status=401, fields={"error": str(exc)}
        )
        raise HTTPException(
            status_code=401, detail=f"invalid token: {exc}", headers={"WWW-Authenticate": "Bearer"}
        ) from exc
    except JwksUnavailable as exc:
        # Our dependency is down, not the caller's token. 503 like an OpenFGA
        # outage — a 401 here would tell every caller to go re-authenticate
        # against a Keycloak that is already struggling.
        _emit_decision(
            request, gate="authn", decision="deny", reason="jwks_unavailable", status=503, fields={"error": str(exc)}
        )
        raise HTTPException(status_code=503, detail="token verification unavailable") from exc


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
            _emit_decision(
                request,
                gate="role",
                decision="deny",
                reason="missing_role",
                status=403,
                principal=principal,
                resource=f"role:{role}",
            )
            raise HTTPException(status_code=403, detail=f"requires role {role}")

    return _dep


require_viewer = require_role("org:viewer")
require_analyst = require_role("org:analyst")


def _fga_or_503(request: Request, *, gate: str, principal: Principal | None) -> FgaClient:
    """The OpenFGA client, or 503 — mirrors the verifier guard in get_principal.

    An app with auth on but no client on state is misconfigured, not open: an
    unguarded attribute read would surface as an AttributeError 500 and read
    like a bug in the check rather than a deployment that half-landed.
    """
    fga = getattr(request.app.state, "fga", None)
    if fga is None:
        _emit_decision(request, gate=gate, decision="deny", reason="authz_unwired", status=503, principal=principal)
        raise HTTPException(status_code=503, detail="auth enabled but authorization client not initialised")
    return fga


async def _vault_for(request: Request, address: EthAddress) -> str | None:
    """Vault address for a vault-or-proxy address — one indexed point query.

    Built with the process-wide reference provider, exactly as every route
    factory builds this repository (``reference_effective_at``, ADR-0006 §4).
    A one-argument construction is a TypeError, which is a 500 on every gated
    prime-scoped request rather than anything a test double would notice.

    Takes an already-parsed address: the repository reports a failed DB query
    as ValueError too, and a caller that parsed here could not tell a
    malformed id (422) from a database that is down (500).
    """
    repo = AllocationRepository(request.app.state.engine, request.app.state.reference_effective_at)
    return await repo.get_prime_vault_address(address)


async def _check_prime_view(request: Request, principal: Principal | None, prime_id: str | None) -> None:
    """The per-resource ``prime:can_view`` check (ADR-011 Plane 2, layer 2).

    One implementation behind three dependencies, because the prime id reaches
    us three ways: a path segment on ``/v1/primes/{prime_id}/*``, a query
    parameter on the risk routes, and a body field on the RRC POSTs. The
    OpenFGA object id is always the prime's VAULT address — the identity
    shared by all of a prime's proxies, and what the reconciler writes.

    ``prime_id is None`` means the request is not prime-scoped at all (the
    optional ``?prime_id=`` on the breakdown routes), so there is no resource
    to check and the router's role gate is the whole control.
    """
    if principal is None:  # auth off
        return
    if prime_id is None:
        return
    try:
        address = EthAddress(prime_id)
    except ValueError as exc:
        # This dependency resolves BEFORE the route's own parameter
        # validation, so without this the API's documented 422 for a
        # malformed id would become a 500.
        _emit_decision(
            request, gate="prime", decision="deny", reason="malformed_prime_id", status=422, principal=principal
        )
        raise HTTPException(status_code=422, detail="malformed prime id") from exc
    # Before the lookup: an app with auth on and no client is misconfigured,
    # and should say so rather than spend a query finding out.
    fga = _fga_or_503(request, gate="prime", principal=principal)
    vault = await _vault_for(request, address)
    if vault is None:
        _emit_decision(
            request, gate="prime", decision="deny", reason="prime_not_found", status=404, principal=principal
        )
        raise HTTPException(status_code=404, detail="prime not found")
    resource = f"prime:{vault.lower()}"
    try:
        allowed = await fga.check(principal.fga_user, "can_view", resource)
    except FgaError as exc:
        _emit_decision(
            request,
            gate="prime",
            decision="deny",
            reason="authz_unavailable",
            status=503,
            principal=principal,
            resource=resource,
        )
        raise HTTPException(status_code=503, detail="authorization service unavailable") from exc
    if not allowed:
        _emit_decision(
            request,
            gate="prime",
            decision="deny",
            reason="not_permitted",
            status=403,
            principal=principal,
            resource=resource,
        )
        raise HTTPException(status_code=403, detail="not permitted for this prime")
    _emit_decision(request, gate="prime", decision="allow", reason="permitted", principal=principal, resource=resource)


async def require_prime_view(request: Request, principal: Principal | None = Depends(get_principal)) -> None:
    """Per-resource check for routes that name the prime in the PATH.

    Reads the path param from the request rather than declaring it: a declared
    parameter is merged into the OpenAPI operation and overrides each route's
    own annotated description. The route's own param does the format validation.
    """
    await _check_prime_view(request, principal, request.path_params.get("prime_id"))


async def require_prime_view_query(request: Request, principal: Principal | None = Depends(get_principal)) -> None:
    """Per-resource check for routes that name the prime in the QUERY STRING.

    ``/v1/risk/*`` scopes to a prime with ``?prime_id=``, never a path segment,
    so the path-param dependency above would silently check nothing and leave
    an analyst able to read any prime's risk through this router (ADR-015 wants
    BOTH the coarse role gate and the per-resource check).
    """
    await _check_prime_view(request, principal, request.query_params.get("prime_id"))


async def require_prime_view_body(request: Request, principal: Principal | None = Depends(get_principal)) -> None:
    """Per-resource check for routes that name the prime in the JSON BODY.

    FastAPI reads and caches the request body before it solves dependencies,
    so re-reading it here cannot consume the stream out from under the route.
    A body that will not parse is left to the route's own validation, which
    answers 422 — deciding authorization on a body nobody could read is worse
    than letting the request die at the validator a moment later.
    """
    if principal is None:  # auth off — never touch the body
        return
    await _check_prime_view(request, principal, await _body_prime_id(request))


async def _body_prime_id(request: Request) -> str | None:
    try:
        body = await request.json()
    except ValueError:  # unparseable or empty body; JSONDecodeError, UnicodeDecodeError
        return None
    if not isinstance(body, dict):
        return None
    value = body.get("prime_id")
    return value if isinstance(value, str) else None


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
    fga = _fga_or_503(request, gate="prime_list", principal=principal)
    try:
        vaults = await fga.list_objects(principal.fga_user, "can_view", "prime")
    except FgaTruncated as exc:
        _emit_decision(
            request,
            gate="prime_list",
            decision="deny",
            reason="authz_truncated",
            status=500,
            principal=principal,
            resource="prime:*",
        )
        raise HTTPException(status_code=500, detail="authorization result truncated") from exc
    except FgaError as exc:
        _emit_decision(
            request,
            gate="prime_list",
            decision="deny",
            reason="authz_unavailable",
            status=503,
            principal=principal,
            resource="prime:*",
        )
        raise HTTPException(status_code=503, detail="authorization service unavailable") from exc
    allowed = frozenset(v.lower() for v in vaults)
    # The COUNT, never the list: an allow-list runs to the ListObjects ceiling
    # and would put thousands of addresses in one log line.
    _emit_decision(
        request,
        gate="prime_list",
        decision="allow",
        reason="filtered",
        principal=principal,
        resource="prime:*",
        fields={"prime_count": len(allowed)},
    )
    return allowed


def get_engine(request: Request) -> AsyncEngine:
    """Extract the shared SQLAlchemy engine from application state."""
    return request.app.state.engine


def get_reference_as_of(request: Request) -> ReferenceEffectiveAtProvider:
    """Extract the process-wide reference effective-instant provider (ADR-0006 §4).

    Every repository reading a converted reference table takes this, so one setting
    pins the whole API. Resolved once at startup from `reference_effective_at`.
    """
    return request.app.state.reference_effective_at


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
            AllocationRepository(request.app.state.engine, request.app.state.reference_effective_at),
        )

    return build


def get_reference_positions_service_factory(
    request: Request,
) -> Callable[[], ReferencePositionsService]:
    """Build the stored-reference balance-sheet service on demand, for the same reason."""

    def build() -> ReferencePositionsService:
        return ReferencePositionsService(
            ReferencePositionRepository(request.app.state.engine),
            AllocationRepository(request.app.state.engine, request.app.state.reference_effective_at),
        )

    return build


def get_reference_capital_repository_factory(
    request: Request,
) -> Callable[[], ReferenceCapitalRepository]:
    """Build the stored-reference-snapshot reader on demand, for the same reason."""
    return lambda: PrimeCapitalStackRepository(request.app.state.engine)
