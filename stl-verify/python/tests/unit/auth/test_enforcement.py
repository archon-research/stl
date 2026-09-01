"""Wiring tests: dark = unchanged; on = 401/403/404/503 at the right places."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app.api import deps
from app.auth.jwt import Principal


def _principal(roles: set[str], sub: str = "u1") -> Principal:
    return Principal(subject=sub, roles=frozenset(roles), organizations=frozenset(), client_id=None)


def _settings_on(monkeypatch: pytest.MonkeyPatch) -> None:
    real = deps.get_settings()
    on = real.model_copy(update={"auth_enabled": True})
    monkeypatch.setattr(deps, "get_settings", lambda: on)


def _app(*, verifier=None, fga=None, principal=None) -> TestClient:
    """A tiny app exercising the real dependencies without the service graph."""
    app = FastAPI()
    if verifier is not None:
        app.state.verifier = verifier
    if fga is not None:
        app.state.fga = fga
    if principal is not None:
        app.dependency_overrides[deps.get_principal] = lambda: principal

    @app.get("/v1/status")
    async def status():
        return {"status": "ok"}

    @app.get("/v1/things", dependencies=[Depends(deps.require_viewer)])
    async def things():
        return ["t"]

    @app.get("/v1/risk/x", dependencies=[Depends(deps.require_analyst)])
    async def risk():
        return {"rrc": 1}

    @app.get("/v1/primes/{prime_id}/debt")
    async def debt(prime_id: str, _authz: None = Depends(deps.require_prime_view)):
        return {"prime": prime_id}

    return TestClient(app)


def test_dark_everything_is_open():
    # auth_enabled defaults False → anonymous everywhere, no state touched
    c = _app()
    assert c.get("/v1/things").status_code == 200
    assert c.get("/v1/risk/x").status_code == 200


def test_probe_route_never_gated():
    c = _app(principal=_principal(set()))
    assert c.get("/v1/status").status_code == 200


def test_missing_bearer_is_401_when_enabled(monkeypatch):
    _settings_on(monkeypatch)
    r = _app(verifier=AsyncMock()).get("/v1/things")
    assert r.status_code == 401
    assert r.headers["www-authenticate"] == "Bearer"


def test_enabled_without_verifier_fails_closed(monkeypatch):
    # settings on but the lifespan never built a verifier: 503, never anonymous
    _settings_on(monkeypatch)
    assert _app().get("/v1/things").status_code == 503


def test_viewer_gate_and_analyst_gate():
    viewer = _app(principal=_principal({"org:viewer"}))
    assert viewer.get("/v1/things").status_code == 200
    assert viewer.get("/v1/risk/x").status_code == 403
    analyst = _app(principal=_principal({"org:analyst", "org:viewer"}))
    assert analyst.get("/v1/risk/x").status_code == 200


@pytest.mark.parametrize("allowed,expected", [(True, 200), (False, 403)])
def test_prime_check_uses_the_resolved_vault(monkeypatch, allowed, expected):
    fga = AsyncMock()
    fga.check.return_value = allowed
    c = _app(fga=fga, principal=_principal({"org:viewer"}))
    # any of the prime's addresses resolves to the VAULT via one indexed query
    monkeypatch.setattr(deps, "_vault_for", AsyncMock(return_value="0xVAULT"))
    r = c.get("/v1/primes/0xproxy/debt")
    assert r.status_code == expected
    fga.check.assert_awaited_once_with("user:u1", "can_view", "prime:0xvault")


def test_unknown_prime_is_404_not_403(monkeypatch):
    c = _app(fga=AsyncMock(), principal=_principal({"org:viewer"}))
    monkeypatch.setattr(deps, "_vault_for", AsyncMock(return_value=None))
    assert c.get("/v1/primes/0xnope/debt").status_code == 404


def test_openfga_down_fails_closed(monkeypatch):
    from app.auth.fga import FgaError

    fga = AsyncMock()
    fga.check.side_effect = FgaError("down")
    c = _app(fga=fga, principal=_principal({"org:viewer"}))
    monkeypatch.setattr(deps, "_vault_for", AsyncMock(return_value="0xv"))
    assert c.get("/v1/primes/0xv/debt").status_code == 503


def test_list_filter_truncation_is_500():
    from app.auth.fga import FgaTruncated

    fga = AsyncMock()
    fga.list_objects.side_effect = FgaTruncated("ceiling")
    app = FastAPI()
    app.state.fga = fga
    app.dependency_overrides[deps.get_principal] = lambda: _principal({"org:viewer"})

    @app.get("/v1/primes")
    async def primes(allowed: frozenset[str] | None = Depends(deps.allowed_prime_vaults)):
        return sorted(allowed or [])

    assert TestClient(app).get("/v1/primes").status_code == 500
