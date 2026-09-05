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
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.adapters.postgres.reference_as_of import utc_now
from app.api import deps
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
