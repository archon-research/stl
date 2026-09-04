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
from app.domain.entities.allocation import EthAddress, as_address
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


def log_auth_event(
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

    The field NAMES are the contract: renaming one breaks a saved Loki query
    and, later, a retained audit record. ``request_id`` is absent deliberately
    — the formatter attaches it. Never carries a credential of any kind.
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
    serves the Tailscale-only present and the Envoy edge later. Anonymous
    (None) with auth off, which every gate below treats as "no checks".
    """
    if not get_settings().auth_enabled:
        return None
    verifier = getattr(request.app.state, "verifier", None)
    if verifier is None:
        # Enabled but the lifespan never built a verifier: fail CLOSED rather
        # than treating everyone as anonymous.
        log_auth_event(request, gate="authn", decision="deny", reason="verifier_unwired", status=503)
        raise HTTPException(status_code=503, detail="auth enabled but verifier not initialised")
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        log_auth_event(request, gate="authn", decision="deny", reason="missing_bearer", status=401)
        raise HTTPException(status_code=401, detail="bearer token required", headers={"WWW-Authenticate": "Bearer"})
    try:
        return await verifier.verify(token)
    except TokenError as exc:
        # PyJWT's own message ("Signature has expired", "Audience doesn't
        # match"). Diagnostic, and never any part of the token itself.
        log_auth_event(
            request, gate="authn", decision="deny", reason="invalid_token", status=401, fields={"error": str(exc)}
        )
        raise HTTPException(
            status_code=401, detail=f"invalid token: {exc}", headers={"WWW-Authenticate": "Bearer"}
        ) from exc
    except JwksUnavailable as exc:
        # 503, not 401: telling every caller to go re-authenticate would only
        # add load to a Keycloak that is already struggling.
        log_auth_event(
            request, gate="authn", decision="deny", reason="jwks_unavailable", status=503, fields={"error": str(exc)}
        )
        raise HTTPException(status_code=503, detail="token verification unavailable") from exc


def require_role(role: str) -> Callable:
    """Coarse RBAC gate (ADR-011 Plane 2, layer 1) — applied per ROUTER.

    Never as global middleware: kubelet probes hit /v1/status and /v1/ready
    directly and would 401 → CrashLoop. Keycloak expands composites, so an
    org:admin token also carries org:analyst and org:viewer.
    """

    async def _dep(request: Request, principal: Principal | None = Depends(get_principal)) -> None:
        if principal is None:  # auth off
            return
        if role not in principal.roles:
            log_auth_event(
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

    A half-landed deployment is not an open app; an unguarded read would
    surface as an AttributeError 500 instead.
    """
    fga = getattr(request.app.state, "fga", None)
    if fga is None:
        log_auth_event(request, gate=gate, decision="deny", reason="authz_unwired", status=503, principal=principal)
        raise HTTPException(status_code=503, detail="auth enabled but authorization client not initialised")
    return fga


async def _vault_for(request: Request, address: EthAddress) -> str | None:
    """Vault address for a vault-or-proxy address — one indexed point query.

    Takes the process-wide reference provider like every route factory does
    (ADR-0006 §4): a one-argument construction is a TypeError, so a 500 on
    every gated request.
    """
    repo = AllocationRepository(request.app.state.engine, request.app.state.reference_effective_at)
    return await repo.get_prime_vault_address(address)


async def _check_prime_view(request: Request, principal: Principal | None, prime_id: str | None) -> None:
    """The per-resource ``prime:can_view`` check (ADR-011 Plane 2, layer 2).

    One implementation behind three dependencies — the prime id reaches us as a
    path segment, a query parameter or a body field. The object id is always
    the VAULT address: the identity shared by all of a prime's proxies, and
    what the reconciler writes. ``prime_id is None`` is not prime-scoped at
    all, so the router's role gate is the whole control.
    """
    if principal is None:  # auth off
        return
    if prime_id is None:
        return
    try:
        address = EthAddress(prime_id)
    except ValueError as exc:
        # Resolves BEFORE the route's own validator, so without this parse the
        # API's documented 422 for a malformed id would be a 500.
        log_auth_event(
            request, gate="prime", decision="deny", reason="malformed_prime_id", status=422, principal=principal
        )
        raise HTTPException(status_code=422, detail="malformed prime id") from exc
    # Before the lookup, so a misconfigured app says so without spending a query.
    fga = _fga_or_503(request, gate="prime", principal=principal)
    try:
        vault = await _vault_for(request, address)
    except ValueError as exc:
        # The repository reports a failed query as ValueError. A database blip
        # behind the gate is our failure, not a bad request: 503, like OpenFGA.
        log_auth_event(
            request,
            gate="prime",
            decision="deny",
            reason="prime_lookup_unavailable",
            status=503,
            principal=principal,
            fields={"error": str(exc)},
        )
        raise HTTPException(status_code=503, detail="prime lookup unavailable") from exc
    if vault is None:
        log_auth_event(
            request, gate="prime", decision="deny", reason="prime_not_found", status=404, principal=principal
        )
        raise HTTPException(status_code=404, detail="prime not found")
    resource = f"prime:{vault.lower()}"
    try:
        allowed = await fga.check(principal.fga_user, "can_view", resource)
    except FgaError as exc:
        log_auth_event(
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
        log_auth_event(
            request,
            gate="prime",
            decision="deny",
            reason="not_permitted",
            status=403,
            principal=principal,
            resource=resource,
        )
        raise HTTPException(status_code=403, detail="not permitted for this prime")
    log_auth_event(request, gate="prime", decision="allow", reason="permitted", principal=principal, resource=resource)


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
        log_auth_event(
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
        log_auth_event(
            request,
            gate="prime_list",
            decision="deny",
            reason="authz_unavailable",
            status=503,
            principal=principal,
            resource="prime:*",
        )
        raise HTTPException(status_code=503, detail="authorization service unavailable") from exc
    # Lowercase FIRST (the regex rejects a `0X` prefix), then drop what is still
    # not an address: it matches no vault_address, so dropping it grants nothing
    # and keeps one bad tuple — written by another system — from 500ing everyone.
    allowed = frozenset(str(a) for a in (as_address(v.lower()) for v in vaults) if a is not None)
    fields: dict[str, object] = {"prime_count": len(allowed)}
    if len(allowed) != len(vaults):
        fields["malformed_count"] = len(vaults) - len(allowed)
    # The COUNT, never the list: an allow-list runs to the ListObjects ceiling
    # and would put thousands of addresses in one log line.
    log_auth_event(
        request,
        gate="prime_list",
        decision="allow",
        reason="filtered",
        principal=principal,
        resource="prime:*",
        fields=fields,
    )
    return allowed


def vault_filter(allowed: frozenset[str] | None) -> list[EthAddress] | None:
    """``allowed_prime_vaults`` as the query parameter the repositories take.

    One helper, not a comprehension per route: forgetting it discloses primes.
    """
    return None if allowed is None else [EthAddress(v) for v in allowed]


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
