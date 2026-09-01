from collections.abc import Callable

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.allocation_position_repository import AllocationRepository
from app.adapters.postgres.pass_through_breakdown_repository import PassThroughBreakdownRepository
from app.adapters.postgres.prime_capital_stack_repository import PrimeCapitalStackRepository
from app.adapters.postgres.reference_position_repository import ReferencePositionRepository
from app.adapters.postgres.reference_risk_capital_repository import ReferenceRiskCapitalRepository
from app.config import get_settings
from app.ports.receipt_token_lookup import ReceiptTokenLookup
from app.ports.reference_capital_repository import ReferenceCapitalRepository
from app.risk_engine.suraf.result import SurafResult
from app.services.crypto_lending_risk_service import CryptoLendingRiskService
from app.services.model_registry import ModelRegistry
from app.services.reference_positions_service import ReferencePositionsService
from app.services.reference_risk_capital_service import ReferenceRiskCapitalService


class Principal:
    """The authenticated caller, derived from a verified JWT.

    Deliberately edge-agnostic: the principal comes from the token the app
    verifies itself, never from proxy-written claim headers, so the same code
    works behind Tailscale today and behind the Envoy edge later without
    resting on a NetworkPolicy staying correct.
    """

    __slots__ = ("subject", "roles", "org")

    def __init__(self, subject: str, roles: frozenset[str], org: str | None):
        self.subject = subject
        self.roles = roles
        self.org = org


def get_principal(request: Request) -> Principal | None:
    """Resolve the caller for the current request.

    Ships dark: with auth_enabled=False (the default) every caller is
    anonymous (None) and no request-time work happens. With the flag on, this
    fails CLOSED until the verifier lands — enabling auth before deploying
    enforcement must reject traffic, not silently wave it through.
    """
    settings = get_settings()
    if not settings.auth_enabled:
        return None
    # Replaced by the JWT verification middleware (pyjwt against Keycloak
    # JWKS) in the enforcement change. Until then the flag must not be on.
    from fastapi import HTTPException

    raise HTTPException(status_code=503, detail="auth_enabled is set but the token verifier is not deployed")


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


def get_pass_through_breakdown_repository_factory(
    request: Request,
) -> Callable[[], PassThroughBreakdownRepository]:
    """Hand out the startup-built pass-through breakdown repository on demand.

    A factory, not the repository, because FastAPI resolves every declared
    dependency on every request and the pass-through fallback is the rare
    path; matches the reference-service factories below.
    """
    return lambda: request.app.state.pass_through_breakdown_repository


def get_allocation_repository_factory(request: Request) -> Callable[[], AllocationRepository]:
    """Hand out the startup-built allocation repository on demand, for the same reason."""
    return lambda: request.app.state.allocation_repository


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
