"""Enforcement as ``create_app`` actually wires it, not as a toy app.

Every other test in this directory mounts its own routes onto a bare FastAPI,
which proves the dependencies work but says nothing about whether they are
attached to the real routers. These go through ``create_app``, so a gate
dropped from one ``include_router`` line fails here and nowhere else.

Nothing here overrides a dependency or patches the settings: the switch itself
is under test, so the app has to enforce what it was BUILT with.

TestClient is deliberately NOT used as a context manager: the lifespan opens a
database connection, and none of this needs one.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest
from fastapi import APIRouter, Depends, FastAPI
from fastapi.routing import APIRoute, _IncludedRouter, iter_route_contexts
from fastapi.testclient import TestClient
from pydantic import SecretStr
from starlette.routing import Route

from app.adapters.postgres.reference_as_of import utc_now
from app.api import deps
from app.api.deps import RequireRole
from app.auth.jwt import Principal, TokenError
from app.config import Settings
from app.main import create_app

ISSUER = "http://keycloak-staging/realms/archon"
PRIME = "0x" + "b" * 40
VIEWER_TOKEN = "viewer-token"
ANALYST_TOKEN = "analyst-token"


def _settings(*, auth_enabled: bool) -> Settings:
    return Settings(
        _env_file=None,  # ty: ignore[unknown-argument]
        log_level="INFO",
        log_format="console",
        database_url=SecretStr("postgresql://u:p@localhost/db"),
        otel_enabled=False,
        otel_exporter_otlp_endpoint="",
        otel_service_name="test",
        auth_enabled=auth_enabled,
        oidc_issuer=ISSUER,
        oidc_audience="python-api",
        openfga_url="http://openfga.auth.svc:8080",
    )


class _StubVerifier:
    """Maps a bearer token straight to a principal, or rejects it."""

    def __init__(self, tokens: dict[str, Principal]) -> None:
        self._tokens = tokens

    async def verify(self, token: str) -> Principal:
        try:
            return self._tokens[token]
        except KeyError:
            raise TokenError("Signature verification failed") from None


class _StubResult:
    """An empty result set, whichever accessor the caller reaches for."""

    def fetchall(self):
        return []

    def all(self):
        return []

    def first(self):
        return None

    def scalar(self):
        return None

    def mappings(self):
        return self

    def __iter__(self):
        return iter(())


class _StubConnection:
    async def execute(self, _sql, params=None):  # noqa: ARG002
        return _StubResult()


class _StubEngine:
    """Enough engine for the readiness probe's SELECT 1, with no database."""

    @asynccontextmanager
    async def connect(self):
        yield _StubConnection()


def _principal(*roles: str) -> Principal:
    return Principal(subject="u1", roles=frozenset(roles), organizations=frozenset(), client_id=None)


def _client(settings: Settings) -> TestClient:
    app = create_app(settings)
    app.state.engine = _StubEngine()
    app.state.reference_effective_at = utc_now
    app.state.verifier = _StubVerifier(
        {
            VIEWER_TOKEN: _principal("org:viewer"),
            ANALYST_TOKEN: _principal("org:viewer", "org:analyst"),
        }
    )
    return TestClient(app)


@pytest.fixture
def client() -> TestClient:
    return _client(_settings(auth_enabled=True))


@pytest.mark.parametrize("path", ["/v1/status", "/v1/ready"])
def test_probes_answer_unauthenticated(client: TestClient, path: str) -> None:
    """kubelet reaches these directly and never carries a token; gating them
    would 401 the probes and CrashLoop the Deployment."""
    assert client.get(path).status_code == 200


def test_unauthenticated_data_route_is_401(client: TestClient) -> None:
    response = client.get("/v1/primes")
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


def test_unverifiable_token_is_401(client: TestClient) -> None:
    assert client.get("/v1/primes", headers={"Authorization": "Bearer forged"}).status_code == 401


def test_non_bearer_scheme_is_401(client: TestClient) -> None:
    assert client.get("/v1/primes", headers={"Authorization": "Basic dXNlcjpwdw=="}).status_code == 401


def test_viewer_token_reaches_the_viewer_routers(client: TestClient) -> None:
    """Not a 401 or 403: the role gate passes and the request goes on to the
    handler (which then wants a database this test does not give it)."""
    response = client.get("/v1/primes", headers={"Authorization": f"Bearer {VIEWER_TOKEN}"})
    assert response.status_code not in (401, 403)


def test_risk_router_is_analyst_only(client: TestClient) -> None:
    response = client.get(
        f"/v1/risk/rrc?asset_id=1&prime_id={PRIME}", headers={"Authorization": f"Bearer {VIEWER_TOKEN}"}
    )
    assert response.status_code == 403


def test_analyst_clears_the_role_gate_and_reaches_the_prime_check(client: TestClient) -> None:
    """Past the role gate the per-resource check runs, and with no OpenFGA
    client wired it fails CLOSED instead of serving the prime."""
    response = client.get(
        f"/v1/risk/rrc?asset_id=1&prime_id={PRIME}", headers={"Authorization": f"Bearer {ANALYST_TOKEN}"}
    )
    assert response.status_code == 503


def test_dark_app_serves_data_routes_unauthenticated() -> None:
    """The E1 contract at the factory level: none of the above changes
    behaviour while AUTH_ENABLED is false."""
    client = _client(_settings(auth_enabled=False))
    assert client.get("/v1/status").status_code == 200
    assert client.get("/v1/primes").status_code == 200


# --- the switch is the app's, not the environment's -------------------------


def test_the_gates_read_the_settings_the_app_was_built_with(monkeypatch) -> None:
    """``create_app`` validates the object it is handed and builds the verifier
    from it. A gate re-reading ``get_settings()`` would serve every route
    ungated here while the app advertises auth as on — and no test could drive
    the enforcing path without overriding a dependency."""
    monkeypatch.setattr(deps, "get_settings", lambda: _settings(auth_enabled=False))

    assert _client(_settings(auth_enabled=True)).get("/v1/primes").status_code == 401


def test_a_dark_app_stays_dark_whatever_the_environment_says(monkeypatch) -> None:
    """The same rule in the other direction, which is the one that matters
    while the flag is off in production."""
    monkeypatch.setattr(deps, "get_settings", lambda: _settings(auth_enabled=True))

    assert _client(_settings(auth_enabled=False)).get("/v1/primes").status_code == 200


# --- startup refuses a half-configured auth plane ---------------------------


@pytest.mark.parametrize("blank", ["oidc_issuer", "oidc_audience", "openfga_url", "openfga_store_name"])
def test_a_blank_required_setting_refuses_to_start(blank: str) -> None:
    """Each of these fails at RUNTIME pointing somewhere else — a blank audience
    401s every token, a blank issuer reports "malformed token" — so the
    AUTH_ENABLED flip would be an outage debugged from the wrong error."""
    settings = _settings(auth_enabled=True).model_copy(update={blank: ""})

    with pytest.raises(RuntimeError, match=blank):
        create_app(settings)


def test_the_same_blanks_are_fine_while_auth_is_dark() -> None:
    """Nothing is read while the flag is off, so nothing may block startup."""
    dark = _settings(auth_enabled=False).model_copy(update={"oidc_issuer": "", "oidc_audience": "", "openfga_url": ""})

    assert create_app(dark) is not None


# The pinned /v1 route table: every (path, method) create_app mounts and the
# role each must carry (see the include_router block in create_app). Empty set =
# the two kubelet probes, the only routes allowed to ship ungated.
VIEWER = frozenset({"org:viewer"})
ANALYST = frozenset({"org:analyst"})
EXPECTED_V1_GATES: dict[tuple[str, str], frozenset[str]] = {
    ("/v1/status", "GET"): frozenset(),
    ("/v1/ready", "GET"): frozenset(),
    ("/v1/allocations/activity", "GET"): VIEWER,
    ("/v1/chains", "GET"): VIEWER,
    ("/v1/data-sources", "GET"): VIEWER,
    ("/v1/primes", "GET"): VIEWER,
    ("/v1/primes/{prime_id}/allocations", "GET"): VIEWER,
    ("/v1/primes/{prime_id}/debt", "GET"): VIEWER,
    ("/v1/primes/{prime_id}/exposure", "GET"): VIEWER,
    ("/v1/primes/{prime_id}/risk-capital", "GET"): VIEWER,
    ("/v1/primes/{prime_id}/total-capital", "GET"): VIEWER,
    ("/v1/protocol-events", "GET"): VIEWER,
    ("/v1/protocols", "GET"): VIEWER,
    ("/v1/provenance/available", "GET"): VIEWER,
    ("/v1/tokens", "GET"): VIEWER,
    ("/v1/tokens/{chain_id}/{token_address}", "GET"): VIEWER,
    ("/v1/tokens/{chain_id}/{token_address}/price", "GET"): VIEWER,
    ("/v1/tokens/{token_id}", "GET"): VIEWER,
    ("/v1/tokens/{token_id}/price", "GET"): VIEWER,
    ("/v1/tx/{tx_hash}/events", "GET"): VIEWER,
    ("/v1/risk/rrc", "GET"): ANALYST,
    ("/v1/risk/rrc", "POST"): ANALYST,
    ("/v1/risk/rrc/scenario", "POST"): ANALYST,
    ("/v1/risk/{chain_id}/{token_address}/bad-debt", "GET"): ANALYST,
    ("/v1/risk/{chain_id}/{token_address}/breakdown", "GET"): ANALYST,
    ("/v1/risk/{receipt_token_id}/bad-debt", "GET"): ANALYST,
    ("/v1/risk/{receipt_token_id}/breakdown", "GET"): ANALYST,
}

# FastAPI's own documentation routes: plain Starlette Routes, outside /v1, the
# only non-APIRoute entries the app is allowed to contain.
FRAMEWORK_ROUTES = {
    ("Route", "/openapi.json"),
    ("Route", "/docs"),
    ("Route", "/docs/oauth2-redirect"),
    ("Route", "/redoc"),
}


@pytest.fixture(scope="module")
def enforced_app() -> FastAPI:
    return create_app(_settings(auth_enabled=True))


def _roles_of(rc) -> frozenset[str]:
    """Gates on one route, whether declared in ``dependencies=`` or as an
    endpoint parameter default; the merged context only carries the former."""
    declared = (d.dependency for d in rc.dependencies)
    in_signature = (d.call for d in rc.original_route.dependant.dependencies)
    return frozenset(d.required_role for d in (*declared, *in_signature) if isinstance(d, RequireRole))


def _walk_v1_routes(app: FastAPI) -> dict[tuple[str, str], frozenset[str]]:
    """The live /v1 route table, keyed like EXPECTED_V1_GATES.

    Walks iter_route_contexts, not app.routes: on FastAPI 0.141 include_router
    stores whole routers as _IncludedRouter entries without flattening, so
    app.routes holds zero /v1 APIRoutes. The contexts merge router-level and
    route-level ``dependencies=``.
    """
    table = {}
    for rc in iter_route_contexts(app.routes):
        if not isinstance(rc.original_route, APIRoute) or not (rc.path or "").startswith("/v1"):
            continue
        for method in rc.methods or ():
            table[(rc.path, method)] = _roles_of(rc)
    return table


def _unclassifiable_routes(app: FastAPI) -> list[str]:
    """Every route entry that is not an APIRoute, minus FastAPI's own docs.

    Deny by default: a Mount serves a sub-app that inherits none of the
    including router's gates, and a plain Route or websocket reaches the
    contexts with an empty path, so a /v1 filter would let them through. Any
    route type the contexts DO enumerate therefore fails here until someone
    teaches the walk about it; what they never enumerate is guarded by
    test_no_low_priority_routes.
    """
    found = []
    for rc in iter_route_contexts(app.routes):
        route = rc.original_route
        kind = type(route).__name__
        if isinstance(route, APIRoute) or (kind, rc.path) in FRAMEWORK_ROUTES:
            continue
        found.append(f"{kind} {rc.path or getattr(route, 'path', '')}")
    return sorted(found)


def test_the_v1_route_table_is_exactly_the_pinned_one(enforced_app: FastAPI) -> None:
    """One assertion covers three regressions: a route shipped ungated, a route
    whose gate was downgraded, and a router dropping out of the walk entirely.
    The last is why this is an equality and not a subset check."""
    assert _walk_v1_routes(enforced_app) == EXPECTED_V1_GATES


def test_no_route_type_the_walk_cannot_classify(enforced_app: FastAPI) -> None:
    unclassifiable = _unclassifiable_routes(enforced_app)

    assert not unclassifiable, f"route types the /v1 gate walk cannot classify: {unclassifiable}"


def _frontend_paths(group: object, prefix: str) -> list[str]:
    """A frontend group carries no path of its own; its mounts do."""
    paths = [prefix + getattr(route, "path", "") for route in getattr(group, "routes", [])]
    return [f"{type(group).__name__} {path}" for path in paths or [prefix]]


def _low_priority_routes(app: FastAPI) -> list[str]:
    """Frontend mounts, which iter_route_contexts never enumerates.

    app.frontend() lands in the app router's own _low_priority_routes.
    router.frontend() behind include_router(prefix="/v1") does not: it sits in
    that router's list and only surfaces through the _IncludedRouter's
    effective_low_priority_routes(), which also recurses into nested includes.
    Private attributes, and the only handles there are.
    """
    found = []
    for group in app.router._low_priority_routes:  # noqa: SLF001
        found.extend(_frontend_paths(group, ""))
    for route in app.routes:
        if isinstance(route, _IncludedRouter):
            for rc in route.effective_low_priority_routes():
                found.extend(_frontend_paths(rc.original_route, rc.frontend_prefix))
    return sorted(found)


def test_no_low_priority_routes(enforced_app: FastAPI) -> None:
    """Neither walk above can see a frontend mount, so this one has to."""
    assert _low_priority_routes(enforced_app) == []


def test_an_unknown_v1_path_is_not_served_by_the_spa(enforced_app: FastAPI) -> None:
    """The static catch-all claims every unmatched URL; only a reserved-prefix
    list keeps it off /v1. A route-table walk cannot see that, so probe it."""
    response = TestClient(enforced_app).get("/v1/definitely-not-a-route")

    assert response.status_code == 404
    assert "text/html" not in response.headers.get("content-type", "")


# --- self-tests of the two helpers -------------------------------------------


def _v1_app(router: APIRouter) -> FastAPI:
    app = FastAPI()
    app.include_router(router, prefix="/v1")
    return app


def test_the_walk_reports_a_bare_router_as_ungated() -> None:
    router = APIRouter()

    @router.get("/naked")
    async def naked() -> dict:  # pragma: no cover — never called
        return {}

    assert _walk_v1_routes(_v1_app(router)) == {("/v1/naked", "GET"): frozenset()}


def test_the_walk_sees_a_gate_declared_as_a_parameter_default() -> None:
    router = APIRouter()

    @router.get("/in-signature")
    async def gated(_: None = Depends(deps.require_viewer)) -> dict:  # pragma: no cover — never called
        return {}

    assert _walk_v1_routes(_v1_app(router)) == {("/v1/in-signature", "GET"): VIEWER}


class _Impostor:
    """Carries the marker without being a gate."""

    required_role = "org:viewer"

    async def __call__(self) -> None:  # pragma: no cover — never called
        return None


def test_the_walk_ignores_a_marker_that_is_not_a_gate() -> None:
    router = APIRouter(dependencies=[Depends(_Impostor())])

    @router.get("/faked")
    async def faked() -> dict:  # pragma: no cover — never called
        return {}

    assert _walk_v1_routes(_v1_app(router)) == {("/v1/faked", "GET"): frozenset()}


def test_a_websocket_under_v1_is_unclassifiable() -> None:
    """A websocket does inherit router gates at runtime; they just are not
    readable from the context, so the walk refuses rather than guesses."""
    router = APIRouter()

    @router.websocket("/socket")
    async def socket(websocket) -> None:  # pragma: no cover — never called
        return None

    assert _unclassifiable_routes(_v1_app(router)) == ["APIWebSocketRoute /socket"]


def test_a_top_level_mount_is_unclassifiable() -> None:
    app = FastAPI()
    app.mount("/v1/sub", FastAPI())

    assert _unclassifiable_routes(app) == ["Mount /v1/sub"]


def test_a_mount_inside_an_included_router_is_unclassifiable() -> None:
    """The shape a /v1 filter would miss: the context carries an empty path."""
    router = APIRouter()
    router.mount("/sub", FastAPI())

    assert _unclassifiable_routes(_v1_app(router)) == ["Mount /sub"]


def test_a_plain_starlette_route_under_v1_is_unclassifiable() -> None:
    router = APIRouter()
    router.routes.append(Route("/plain", lambda request: None))  # pragma: no cover — never called

    assert _unclassifiable_routes(_v1_app(router)) == ["Route /plain"]


def test_a_frontend_on_the_app_is_a_low_priority_route(tmp_path) -> None:
    app = FastAPI()
    app.frontend("/ui", directory=tmp_path)

    assert _low_priority_routes(app) == ["_FrontendRouteGroup /ui"]


def test_a_frontend_behind_an_included_router_is_a_low_priority_route(tmp_path) -> None:
    """The shape the app router's own list misses: the mount lives in the
    included router and, with the /v1 catch-all gone, would serve ungated."""
    router = APIRouter()
    router.frontend("/ui", directory=tmp_path)

    assert _low_priority_routes(_v1_app(router)) == ["_FrontendRouteGroup /v1/ui"]


def test_a_new_app_contains_only_the_framework_routes() -> None:
    """Pins FRAMEWORK_ROUTES to what FastAPI actually registers, so an upgrade
    that adds a route type shows up here rather than being silently allowed."""
    assert _unclassifiable_routes(FastAPI()) == []
