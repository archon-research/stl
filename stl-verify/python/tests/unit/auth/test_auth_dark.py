"""The E1 contract: with auth_enabled=False (the default) nothing changes.

These tests pin the dark-ship behaviour so the enforcement follow-up can only
change behaviour behind the flag, and pin the fail-closed semantics of turning
the flag on before the verifier exists.
"""

from pydantic import SecretStr

from app.api import deps
from app.config import Settings


def _settings(*, auth_enabled: bool = False, oidc_issuer: str = "") -> Settings:
    """Dark by default; enabling auth also fills in what create_app demands."""
    return Settings(
        _env_file=None,  # ty: ignore[unknown-argument]
        log_level="INFO",
        log_format="console",
        database_url=SecretStr("postgresql://u:p@localhost/db"),
        otel_enabled=False,
        otel_exporter_otlp_endpoint="",
        otel_service_name="test",
        auth_enabled=auth_enabled,
        oidc_issuer=oidc_issuer,
        oidc_audience="python-api" if auth_enabled else "",
        openfga_url="http://openfga.auth.svc:8080" if auth_enabled else "",
    )


def test_auth_is_off_by_default() -> None:
    """Constructs Settings WITHOUT passing auth_enabled, so this fails if the
    production default ever changes — not just if the test helper's does."""
    settings = Settings(
        _env_file=None,  # ty: ignore[unknown-argument]
        log_level="INFO",
        log_format="console",
        database_url=SecretStr("postgresql://u:p@localhost/db"),
        otel_enabled=False,
        otel_exporter_otlp_endpoint="",
        otel_service_name="test",
    )
    assert settings.auth_enabled is False


def test_anonymous_principal_when_dark() -> None:
    """No verifier on app.state (auth off) → every caller is anonymous, no 401."""
    from fastapi import Depends, FastAPI
    from fastapi.testclient import TestClient

    app = FastAPI()

    @app.get("/who")
    async def who(principal: deps.Principal | None = Depends(deps.get_principal)) -> dict[str, bool]:
        return {"anonymous": principal is None}

    assert TestClient(app).get("/who").json() == {"anonymous": True}


def test_openapi_has_no_security_scheme_when_dark() -> None:
    from app.main import create_app

    app = create_app(_settings())
    schema = app.openapi()
    assert "securitySchemes" not in schema.get("components", {})
    assert "security" not in schema


def test_openapi_declares_oidc_when_enabled() -> None:
    from app.main import create_app

    app = create_app(_settings(auth_enabled=True, oidc_issuer="https://kc/realms/archon"))
    schema = app.openapi()
    flows = schema["components"]["securitySchemes"]["oidc"]["flows"]["authorizationCode"]
    assert flows["authorizationUrl"].startswith("https://kc/realms/archon/")
    # Declaring the scheme is not enough — Swagger only ATTACHES the token to
    # operations that carry a security requirement (review L2-B3).
    assert schema["security"] == [{"oidc": []}]


def test_oauth_redirect_route_registered_when_enabled() -> None:
    """docs_url=None means FastAPI does not auto-register the redirect page;
    without it the OAuth flow dead-ends silently after login."""
    from app.main import create_app

    app = create_app(_settings(auth_enabled=True, oidc_issuer="https://kc/realms/archon"))
    assert any(getattr(r, "path", None) == "/docs/oauth2-redirect" for r in app.routes)
