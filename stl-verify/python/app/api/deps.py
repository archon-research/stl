import logging
from collections.abc import Callable

from fastapi import Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.adapters.postgres.prime_capital_stack_repository import PrimeCapitalStackRepository
from app.adapters.postgres.reference_as_of import ReferenceEffectiveAtProvider
from app.adapters.postgres.reference_position_repository import ReferencePositionRepository
from app.adapters.postgres.reference_risk_capital_repository import ReferenceRiskCapitalRepository
from app.api.errors import ApiRejectionError
from app.auth.fga import FgaClient, FgaError, FgaTruncated
from app.auth.jwt import JwksUnavailable, Principal, TokenError
from app.config import Settings, get_settings
from app.domain.chain_names import chain_is_served
from app.domain.entities.allocation import EthAddress, as_address
from app.domain.entities.prime import PrimeIdentity, PrimeScope
from app.domain.exceptions import InvalidPrimeIdentifierError
from app.domain.prime_registry import alm_proxies_for_prime
from app.logging import get_logger
from app.ports.prime_resolver import PrimeResolver
from app.ports.receipt_token_lookup import ReceiptTokenLookup
from app.ports.reference_capital_repository import ReferenceCapitalRepository
from app.risk_engine.suraf.result import SurafResult
from app.services.crypto_lending_risk_service import CryptoLendingRiskService
from app.services.model_registry import ModelRegistry
from app.services.reference_positions_service import ReferencePositionsService
from app.services.reference_risk_capital_service import ReferenceRiskCapitalService

logger = get_logger(__name__)

# One event name for every authorization outcome, so a single Loki stream
# selector finds the lot:
#   {k8s_namespace_name="vector", k8s_container_name="python-api"}
#     | json | event="authz.decision"
AUTHZ_EVENT = "authz.decision"

# One body for both prime denials, so the response cannot be read as an
# existence oracle. Which one it was lives in the decision event.
PRIME_DENIED_DETAIL = "prime not found"


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


def settings_for(request: Request) -> Settings:
    """The settings the app was BUILT with, not a fresh read of the environment.

    ``create_app`` validates the object it is handed and wires the verifier and
    FGA client from it, so a gate reading ``get_settings()`` instead can serve
    every route ungated while the app advertises auth as on. The fallback is for
    an app assembled by hand — some tests mount routers onto a bare FastAPI.
    """
    stashed = getattr(request.app.state, "settings", None)
    return stashed if stashed is not None else get_settings()


async def get_principal(request: Request) -> Principal | None:
    """Resolve the caller from the token the app verifies ITSELF.

    Never from proxy-written claim headers — edge-agnostic, so the same code
    serves the Tailscale-only present and the Envoy edge later. Anonymous
    (None) with auth off, which every gate below treats as "no checks".
    """
    if not settings_for(request).auth_enabled:
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


async def check_prime_view(
    request: Request,
    principal: Principal | None,
    prime_id: str | None,
    *,
    not_found_reason: str = "prime_not_found",
) -> None:
    """The per-resource ``prime:can_view`` check (ADR-011 Plane 2, layer 2).

    One implementation behind every caller — the prime id reaches us as a path
    segment, a query parameter, a body field, or the wallet a pool-level risk
    read resolves to. When the input resolves, the object id is the VAULT
    address: the identity shared by all of a prime's proxies, and what the
    reconciler writes. When it does not, the event carries the unresolved input
    as ``requested_prime`` and ``resource`` stays the request path, so
    ``resource`` is always an OpenFGA object id or a path, never a proxy.

    An unknown prime and one the caller may not view answer the same 404. A
    distinct code tells an unauthorized caller which primes exist, the fact the
    list filtering hides; the decision event keeps the two apart.

    ``not_found_reason`` is for the caller that RESOLVED the prime itself
    rather than receiving it from the request (the pool-level risk reads): an
    untracked largest holder denies every caller indefinitely and would
    otherwise read as an outage, so its decision event carries its own reason
    (ORB-402). Only the event changes — the response body stays byte-identical.
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
        raise ApiRejectionError("malformed prime id") from exc
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
        # The address that resolved to nothing, so triage need not re-run the
        # resolution. Not `resource`: that field is an FGA object id or a path.
        log_auth_event(
            request,
            gate="prime",
            decision="deny",
            reason=not_found_reason,
            status=404,
            principal=principal,
            fields={"requested_prime": address.lower()},
        )
        raise HTTPException(status_code=404, detail=PRIME_DENIED_DETAIL)
    await _check_vault_view(request, fga, principal, vault)


async def _check_vault_view(request: Request, fga: FgaClient, principal: Principal, vault: str) -> None:
    """Ask OpenFGA whether ``principal`` may view the prime owning ``vault``."""
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
            status=404,
            principal=principal,
            resource=resource,
        )
        raise HTTPException(status_code=404, detail=PRIME_DENIED_DETAIL)
    log_auth_event(request, gate="prime", decision="allow", reason="permitted", principal=principal, resource=resource)


def get_prime_resolver(request: Request) -> PrimeResolver:
    """Extract the prime resolver built at startup.

    Routes take the port from here rather than constructing the adapter, so a
    prime-scoped route never names a concrete infrastructure class.
    """
    return request.app.state.prime_resolver


async def resolve_prime(
    identifier: str,
    resolver: PrimeResolver,
    *,
    request: Request | None = None,
    principal: Principal | None = None,
    not_found_reason: str = "prime_not_found",
) -> PrimeIdentity:
    """Return the prime ``identifier`` names, or raise 422, 503 or 404.

    A malformed identifier is 422, a failed lookup is 503 rather than the
    caller's fault, and the 404 detail is the denial body so an unknown prime
    and one the caller may not view stay indistinguishable. ``request`` and
    ``principal`` are supplied by the authz gate alone: they turn each denial
    into an ADR-015 decision event, which is the only place the two 404s are
    told apart.
    """

    def _denied(reason: str, status: int, **fields: object) -> None:
        if request is not None and principal is not None:
            log_auth_event(
                request, gate="prime", decision="deny", reason=reason, status=status, principal=principal, fields=fields
            )

    try:
        prime = await resolver.resolve(identifier)
    except InvalidPrimeIdentifierError as exc:
        _denied("malformed_prime_id", 422)
        raise ApiRejectionError("malformed prime id") from exc
    except ValueError as exc:
        _denied("prime_lookup_unavailable", 503, error=str(exc))
        raise HTTPException(status_code=503, detail="prime lookup unavailable") from exc
    if prime is None:
        _denied(not_found_reason, 404, requested_prime=identifier.lower())
        raise HTTPException(status_code=404, detail=PRIME_DENIED_DETAIL)
    return prime


async def resolve_prime_scope(
    identifier: str,
    resolver: PrimeResolver,
    *,
    request: Request | None = None,
    principal: Principal | None = None,
) -> PrimeScope:
    """Resolve any accepted identifier to the prime's whole wallet set.

    Inherits ``resolve_prime``'s 422/404/503 contract. A failed proxy listing is
    503 and never an empty wallet set: a scope that silently came back empty
    would render as a prime holding nothing, which is the partial total this
    resolution exists to remove.
    """
    identity = await resolve_prime(identifier, resolver, request=request, principal=principal)
    return await _scope_for(identity, resolver)


async def _scope_for(identity: PrimeIdentity, resolver: PrimeResolver) -> PrimeScope:
    """Widen a resolved identity to the prime's whole wallet set."""
    try:
        wallets = await resolver.list_proxies(identity.id)
    except ValueError as exc:
        # The gate's second query fails on its own, and the Loki alert reads one
        # reason, so it emits the event a failed resolve does rather than none.
        if request is not None and principal is not None:
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
    return PrimeScope.build(
        identity,
        wallets,
        unserved_chains=(
            entry.chain for entry in alm_proxies_for_prime(identity.name) if not chain_is_served(entry.chain)
        ),
    )


async def prime_proxy_filter(identifier: str | None, resolver: PrimeResolver) -> tuple[EthAddress, ...] | None:
    """The ALM proxies a query FILTER names, or ``None`` when it names no prime.

    A filter is not a path resource: a well-formed identifier matching no prime
    is an empty result, never a 404, so the pair of answers cannot be read as an
    existence oracle either. Malformed is still 422 and a failed lookup 503.

    An identifier that resolves to nothing yields an EMPTY tuple, which the
    repository contract reads as "matches nothing". ``None`` is returned only
    when no prime was named at all: conflating the two would serve every prime's
    rows to a caller who asked for one.
    """
    if identifier is None:
        return None
    try:
        identity = await resolver.resolve(identifier)
    except InvalidPrimeIdentifierError as exc:
        raise ApiRejectionError("malformed prime id") from exc
    except ValueError as exc:
        raise HTTPException(status_code=503, detail="prime lookup unavailable") from exc
    if identity is None:
        return ()
    return (await _scope_for(identity, resolver)).alm_proxies


async def prime_scope(
    request: Request,
    resolver: PrimeResolver = Depends(get_prime_resolver),
    principal: Principal | None = Depends(get_principal),
) -> PrimeScope:
    """The resolved scope for a route that names the prime in the PATH.

    Reads ``prime_id`` out of ``request.path_params`` rather than declaring it,
    for the reason ``require_prime_view`` already does: a declared parameter is
    merged into the OpenAPI operation and overrides the route's own annotated
    description.

    FastAPI caches a sub-dependency per request, so the gate below and the
    handler share one resolution and cannot disagree about what the identifier
    meant.
    """
    identifier = request.path_params.get("prime_id")
    if identifier is None:
        raise RuntimeError(f"{request.url.path} depends on prime_scope but has no {{prime_id}} path segment")
    return await resolve_prime_scope(identifier, resolver, request=request, principal=principal)


async def require_prime_view(
    request: Request,
    scope: PrimeScope = Depends(prime_scope),
    principal: Principal | None = Depends(get_principal),
) -> None:
    """Per-resource ``prime:can_view`` over an already-resolved scope.

    The vault comes from the resolved identity rather than a second point query,
    so the object id is unchanged in value and the gate costs one lookup less.
    """
    if principal is None:  # auth off
        return
    fga = _fga_or_503(request, gate="prime", principal=principal)
    await _check_vault_view(request, fga, principal, scope.identity.vault_address)


async def require_prime_view_query(request: Request, principal: Principal | None = Depends(get_principal)) -> None:
    """Per-resource check for routes that name the prime in the QUERY STRING.

    ``/v1/risk/*`` scopes to a prime with ``?prime_id=``, never a path segment,
    so the path-param dependency above would silently check nothing and leave
    an analyst able to read any prime's risk through this router (ADR-015 wants
    BOTH the coarse role gate and the per-resource check).
    """
    await check_prime_view(request, principal, request.query_params.get("prime_id"))


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
    await check_prime_view(request, principal, await _body_prime_id(request))


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
        return ReferenceRiskCapitalService(ReferenceRiskCapitalRepository(request.app.state.engine))

    return build


def get_reference_positions_service_factory(
    request: Request,
) -> Callable[[], ReferencePositionsService]:
    """Build the stored-reference balance-sheet service on demand, for the same reason."""

    def build() -> ReferencePositionsService:
        return ReferencePositionsService(ReferencePositionRepository(request.app.state.engine))

    return build


def get_reference_capital_repository_factory(
    request: Request,
) -> Callable[[], ReferenceCapitalRepository]:
    """Build the stored-reference-snapshot reader on demand, for the same reason."""
    return lambda: PrimeCapitalStackRepository(request.app.state.engine)
