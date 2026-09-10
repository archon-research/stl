"""Wiring tests: dark = unchanged; on = 401/403/404/422/503 at the right places."""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app.adapters.postgres.reference_as_of import utc_now
from app.api import deps
from app.auth.jwt import Principal

VAULT = "0x" + "a" * 40
PROXY = "0x" + "b" * 40


def _principal(roles: set[str], sub: str = "u1") -> Principal:
    return Principal(subject=sub, roles=frozenset(roles), organizations=frozenset(), client_id=None)


def _settings_on(monkeypatch: pytest.MonkeyPatch) -> None:
    real = deps.get_settings()
    on = real.model_copy(update={"auth_enabled": True})
    monkeypatch.setattr(deps, "get_settings", lambda: on)


def _app(*, verifier=None, fga=None, principal=None, engine=None) -> TestClient:
    """A tiny app exercising the real dependencies without the service graph."""
    app = FastAPI()
    if verifier is not None:
        app.state.verifier = verifier
    if fga is not None:
        app.state.fga = fga
    if engine is not None:
        app.state.engine = engine
        app.state.reference_effective_at = utc_now
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


def test_jwks_outage_is_503_not_401(monkeypatch):
    """A Keycloak we cannot reach is our failure, not a bad token: telling
    every caller to go re-authenticate would only add load to it."""
    from app.auth.jwt import JwksUnavailable

    _settings_on(monkeypatch)
    verifier = AsyncMock()
    verifier.verify.side_effect = JwksUnavailable("connect timeout")
    assert _app(verifier=verifier).get("/v1/things", headers={"Authorization": "Bearer t"}).status_code == 503


def test_invalid_token_is_401(monkeypatch):
    from app.auth.jwt import TokenError

    _settings_on(monkeypatch)
    verifier = AsyncMock()
    verifier.verify.side_effect = TokenError("Signature has expired")
    assert _app(verifier=verifier).get("/v1/things", headers={"Authorization": "Bearer t"}).status_code == 401


def test_viewer_gate_and_analyst_gate():
    viewer = _app(principal=_principal({"org:viewer"}))
    assert viewer.get("/v1/things").status_code == 200
    assert viewer.get("/v1/risk/x").status_code == 403
    analyst = _app(principal=_principal({"org:analyst", "org:viewer"}))
    assert analyst.get("/v1/risk/x").status_code == 200


@pytest.mark.parametrize("allowed,expected", [(True, 200), (False, 404)])
def test_prime_check_uses_the_resolved_vault(monkeypatch, allowed, expected):
    fga = AsyncMock()
    fga.check.return_value = allowed
    c = _app(fga=fga, principal=_principal({"org:viewer"}))
    # any of the prime's addresses resolves to the VAULT via one indexed query
    monkeypatch.setattr(deps, "_vault_for", AsyncMock(return_value=VAULT.upper()))
    r = c.get(f"/v1/primes/{PROXY}/debt")
    assert r.status_code == expected
    fga.check.assert_awaited_once_with("user:u1", "can_view", f"prime:{VAULT}")


@pytest.mark.parametrize("vault,allowed", [(None, True), (VAULT, False)], ids=["unknown", "not-permitted"])
def test_unknown_and_unpermitted_are_indistinguishable(monkeypatch, vault, allowed):
    """A different code for a prime that does not exist would tell an
    unauthorized caller which ones do — the fact the list filtering hides."""
    fga = AsyncMock()
    fga.check.return_value = allowed
    c = _app(fga=fga, principal=_principal({"org:viewer"}))
    monkeypatch.setattr(deps, "_vault_for", AsyncMock(return_value=vault))

    response = c.get(f"/v1/primes/{PROXY}/debt")

    assert (response.status_code, response.json()) == (404, {"detail": "prime not found"})


def test_malformed_prime_id_is_422_not_500(monkeypatch):
    """This dependency resolves before the route's own validator, so without
    its own parse it would raise ValueError out of a 500."""
    c = _app(fga=AsyncMock(), principal=_principal({"org:viewer"}))
    monkeypatch.setattr(deps, "_vault_for", AsyncMock(return_value=VAULT))
    assert c.get("/v1/primes/not-an-address/debt").status_code == 422


def test_enabled_without_fga_client_fails_closed(monkeypatch):
    """Auth on but no OpenFGA client on state: 503, mirroring the verifier
    guard — an unguarded read would be an AttributeError 500."""
    c = _app(principal=_principal({"org:viewer"}))
    monkeypatch.setattr(deps, "_vault_for", AsyncMock(return_value=VAULT))
    assert c.get(f"/v1/primes/{PROXY}/debt").status_code == 503


def test_openfga_down_fails_closed(monkeypatch):
    from app.auth.fga import FgaError

    fga = AsyncMock()
    fga.check.side_effect = FgaError("down")
    c = _app(fga=fga, principal=_principal({"org:viewer"}))
    monkeypatch.setattr(deps, "_vault_for", AsyncMock(return_value=VAULT))
    assert c.get(f"/v1/primes/{VAULT}/debt").status_code == 503


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


# --- the REAL _vault_for path ----------------------------------------------
#
# Every test above stubs _vault_for, which is precisely how a change to
# AllocationRepository's constructor (main #822 made reference_effective_at
# required) stayed invisible: the gate raised TypeError -> 500 on every
# prime-scoped request while the suite stayed green. These build the real
# repository against a fake engine, so the constructor and the query both run.


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row


class _FakeRow:
    def __init__(self, vault_hex: str):
        self.vault = vault_hex


class _FakeConnection:
    def __init__(self, row):
        self._row = row
        self.params: dict | None = None

    async def execute(self, _sql, params):
        self.params = params
        return _FakeResult(self._row)


class _FakeEngine:
    """Just enough engine for one point query, with no database behind it."""

    def __init__(self, row):
        self.connection = _FakeConnection(row)

    @asynccontextmanager
    async def connect(self):
        yield self.connection


def _real_path_client(row, fga) -> TestClient:
    return _app(fga=fga, principal=_principal({"org:viewer"}), engine=_FakeEngine(row))


def test_real_vault_lookup_builds_the_repository_the_way_the_app_does():
    """Regression guard for the merge break: AllocationRepository takes the
    process-wide reference provider as a second argument."""
    fga = AsyncMock()
    fga.check.return_value = True
    c = _real_path_client(_FakeRow("a" * 40), fga)
    assert c.get(f"/v1/primes/{PROXY}/debt").status_code == 200
    fga.check.assert_awaited_once_with("user:u1", "can_view", f"prime:{VAULT}")


def test_real_vault_lookup_returns_404_for_an_unknown_prime():
    c = _real_path_client(None, AsyncMock())
    assert c.get(f"/v1/primes/{PROXY}/debt").status_code == 404


def test_real_vault_lookup_passes_the_parsed_address_to_the_query():
    fga = AsyncMock()
    fga.check.return_value = True
    engine = _FakeEngine(_FakeRow("a" * 40))
    client = _app(fga=fga, principal=_principal({"org:viewer"}), engine=engine)
    client.get(f"/v1/primes/{PROXY}/debt")
    assert engine.connection.params == {"addr": bytes.fromhex("b" * 40)}


# --- decision events (ADR-015 gate 3) --------------------------------------


def _events(caplog) -> list[dict]:
    """Every authorization decision event captured, as its logged fields."""
    return [
        {key: getattr(record, key) for key in ("event", "gate", "decision", "reason", "principal", "resource")}
        for record in caplog.records
        if getattr(record, "event", None) == deps.AUTHZ_EVENT
    ]


def test_denied_role_emits_a_decision_event(caplog):
    with caplog.at_level(logging.INFO, logger="app.api.deps"):
        _app(principal=_principal({"org:viewer"})).get("/v1/risk/x")
    assert _events(caplog) == [
        {
            "event": deps.AUTHZ_EVENT,
            "gate": "role",
            "decision": "deny",
            "reason": "missing_role",
            "principal": "user:u1",
            "resource": "role:org:analyst",
        }
    ]


def test_missing_bearer_emits_a_decision_event(monkeypatch, caplog):
    _settings_on(monkeypatch)
    with caplog.at_level(logging.INFO, logger="app.api.deps"):
        _app(verifier=AsyncMock()).get("/v1/things")
    assert [(e["gate"], e["decision"], e["reason"], e["principal"]) for e in _events(caplog)] == [
        ("authn", "deny", "missing_bearer", "anonymous")
    ]


@pytest.mark.parametrize(
    "allowed,decision,reason",
    [(True, "allow", "permitted"), (False, "deny", "not_permitted")],
)  # the event still tells the two denials apart; only the response does not
def test_prime_check_emits_a_decision_event_naming_the_resource(monkeypatch, caplog, allowed, decision, reason):
    fga = AsyncMock()
    fga.check.return_value = allowed
    monkeypatch.setattr(deps, "_vault_for", AsyncMock(return_value=VAULT))
    with caplog.at_level(logging.INFO, logger="app.api.deps"):
        _app(fga=fga, principal=_principal({"org:viewer"})).get(f"/v1/primes/{PROXY}/debt")
    assert _events(caplog) == [
        {
            "event": deps.AUTHZ_EVENT,
            "gate": "prime",
            "decision": decision,
            "reason": reason,
            "principal": "user:u1",
            "resource": f"prime:{VAULT}",
        }
    ]


def test_list_filtering_emits_a_count_never_the_allow_list(caplog):
    """An allow-list runs to the ListObjects ceiling; logging it would put
    thousands of addresses on one line."""
    fga = AsyncMock()
    fga.list_objects.return_value = frozenset({VAULT.upper(), PROXY})
    app = FastAPI()
    app.state.fga = fga
    app.dependency_overrides[deps.get_principal] = lambda: _principal({"org:viewer"})

    @app.get("/v1/primes")
    async def primes(allowed: frozenset[str] | None = Depends(deps.allowed_prime_vaults)):
        return sorted(allowed or [])

    with caplog.at_level(logging.INFO, logger="app.api.deps"):
        assert TestClient(app).get("/v1/primes").status_code == 200

    (event,) = [r for r in caplog.records if getattr(r, "event", None) == deps.AUTHZ_EVENT]
    assert (event.gate, event.decision, event.reason) == ("prime_list", "allow", "filtered")
    assert event.prime_count == 2
    assert VAULT not in event.getMessage()


def test_decision_events_reach_the_json_log_as_queryable_fields():
    """The field names are the Loki query surface, so they have to survive the
    formatter rather than being dropped with the rest of `extra`."""
    from app.logging import JsonFormatter

    record = logging.LogRecord("app.api.deps", logging.WARNING, __file__, 1, "authorization deny", None, None)
    record.event = deps.AUTHZ_EVENT
    record.decision = "deny"
    record.principal = "user:u1"
    record.resource = f"prime:{VAULT}"
    emitted = json.loads(JsonFormatter().format(record))
    assert emitted["event"] == deps.AUTHZ_EVENT
    assert emitted["decision"] == "deny"
    assert emitted["principal"] == "user:u1"
    assert emitted["resource"] == f"prime:{VAULT}"
    assert emitted["level"] == "WARNING"


# --- failures behind the gate stay 503, and stay visible --------------------
#
# Each of these used to escape the dependency as a bare 500: no decision event,
# so the Loki alert on that event never fires for what is really an outage.


def test_a_database_outage_behind_the_prime_gate_is_503_not_500(monkeypatch, caplog):
    """AllocationRepository reports a failed query as ValueError. Unhandled,
    that is a 500 on every prime-scoped route the moment the database blips."""
    c = _app(fga=AsyncMock(), principal=_principal({"org:viewer"}))
    monkeypatch.setattr(deps, "_vault_for", AsyncMock(side_effect=ValueError("Database query failed")))

    with caplog.at_level(logging.INFO, logger="app.api.deps"):
        response = c.get(f"/v1/primes/{PROXY}/debt")

    assert response.status_code == 503
    assert [(e["gate"], e["decision"], e["reason"]) for e in _events(caplog)] == [
        ("prime", "deny", "prime_lookup_unavailable")
    ]


def _allow_list_client(objects: frozenset[str]) -> tuple[TestClient, AsyncMock]:
    fga = AsyncMock()
    fga.list_objects.return_value = objects
    app = FastAPI()
    app.state.fga = fga
    app.dependency_overrides[deps.get_principal] = lambda: _principal({"org:viewer"})

    @app.get("/v1/primes")
    async def primes(allowed: frozenset[str] | None = Depends(deps.allowed_prime_vaults)):
        return sorted(allowed or [])

    return TestClient(app), fga


def test_a_malformed_object_id_in_the_tuple_store_does_not_take_the_route_down():
    """The tuple reconciler is a different system. One id that is not an
    address must not 500 /v1/primes for everyone — and cannot grant anything,
    since it matches no vault_address."""
    client, _ = _allow_list_client(frozenset({VAULT, "prime-with-no-address", ""}))

    response = client.get("/v1/primes")

    assert response.status_code == 200
    assert response.json() == [VAULT]


def test_a_dropped_object_id_is_counted_on_the_decision_event(caplog):
    with caplog.at_level(logging.INFO, logger="app.api.deps"):
        client, _ = _allow_list_client(frozenset({VAULT, "not-an-address"}))
        assert client.get("/v1/primes").status_code == 200

    (event,) = [r for r in caplog.records if getattr(r, "event", None) == deps.AUTHZ_EVENT]
    assert (event.prime_count, event.malformed_count) == (1, 1)


def test_an_uppercase_0x_prefix_is_normalised_not_dropped():
    """The address regex is case-sensitive on the `x`, so lowercasing has to
    happen BEFORE the parse or a `0X`-prefixed tuple silently loses access."""
    client, _ = _allow_list_client(frozenset({VAULT.replace("0x", "0X").upper()}))

    assert client.get("/v1/primes").json() == [VAULT]
