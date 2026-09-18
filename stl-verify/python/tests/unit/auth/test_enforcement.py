"""Wiring tests: dark = unchanged; on = 401/403/404/422/503 at the right places."""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from typing import cast
from unittest.mock import AsyncMock

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.prime_resolver_repository import PrimeResolverRepository
from app.adapters.postgres.reference_as_of import utc_now
from app.api import deps
from app.api.errors import register_error_handlers
from app.auth.jwt import Principal
from app.domain.entities.allocation import EthAddress
from app.domain.entities.prime import PrimeIdentity
from app.domain.exceptions import InvalidPrimeIdentifierError

VAULT = "0x" + "a" * 40
PROXY = "0x" + "b" * 40
_SPARK = PrimeIdentity(id=1, name="spark", external_id="4bd9ee3c", vault_address=EthAddress("0x" + "A" * 40))


def _principal(roles: set[str], sub: str = "u1") -> Principal:
    return Principal(subject=sub, roles=frozenset(roles), organizations=frozenset(), client_id=None)


def _settings_on(monkeypatch: pytest.MonkeyPatch) -> None:
    real = deps.get_settings()
    on = real.model_copy(update={"auth_enabled": True})
    monkeypatch.setattr(deps, "get_settings", lambda: on)


def _resolver(identity: PrimeIdentity | None = _SPARK, *, fails: bool = False) -> AsyncMock:
    """A PrimeResolver the gate resolves the path segment through."""
    resolver = AsyncMock()
    resolver.resolve.side_effect = ValueError("Database query failed") if fails else None
    resolver.resolve.return_value = identity
    resolver.list_proxies.return_value = []
    return resolver


def _app(*, verifier=None, fga=None, principal=None, engine=None, resolver=None) -> TestClient:
    """A tiny app exercising the real dependencies without the service graph."""
    app = FastAPI()
    register_error_handlers(app)
    if verifier is not None:
        app.state.verifier = verifier
    if fga is not None:
        app.state.fga = fga
    if engine is not None:
        app.state.engine = engine
        app.state.reference_effective_at = utc_now
    app.state.prime_resolver = resolver if resolver is not None else _resolver()
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

    @app.get("/v1/primes")
    async def primes(allowed: frozenset[str] | None = Depends(deps.allowed_prime_vaults)):
        return sorted(allowed or [])

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
def test_prime_check_uses_the_resolved_vault(allowed, expected):
    fga = AsyncMock()
    fga.check.return_value = allowed
    # any of the prime's identifiers resolves to the VAULT the tuples are keyed on
    c = _app(fga=fga, principal=_principal({"org:viewer"}))
    r = c.get(f"/v1/primes/{PROXY}/debt")
    assert r.status_code == expected
    fga.check.assert_awaited_once_with("user:u1", "can_view", f"prime:{VAULT}")


def test_prime_check_accepts_a_name_where_the_old_gate_parsed_an_address():
    """The gate resolving instead of parsing is what lets a name reach a route
    at all: it runs before the route's own validator."""
    fga = AsyncMock()
    fga.check.return_value = True
    c = _app(fga=fga, principal=_principal({"org:viewer"}))

    assert c.get("/v1/primes/spark/debt").status_code == 200
    fga.check.assert_awaited_once_with("user:u1", "can_view", f"prime:{VAULT}")


@pytest.mark.parametrize("identity,allowed", [(None, True), (_SPARK, False)], ids=["unknown", "not-permitted"])
def test_unknown_and_unpermitted_are_indistinguishable(identity, allowed):
    """A different code for a prime that does not exist would tell an
    unauthorized caller which ones do — the fact the list filtering hides."""
    fga = AsyncMock()
    fga.check.return_value = allowed
    c = _app(fga=fga, principal=_principal({"org:viewer"}), resolver=_resolver(identity))

    response = c.get(f"/v1/primes/{PROXY}/debt")

    assert (response.status_code, response.json()) == (404, {"detail": "prime not found"})


def test_malformed_prime_id_is_422_not_500():
    """This dependency resolves before the route's own validator, so without
    the resolver rejecting it, a bad address would raise out of a 500."""
    resolver = _resolver()
    resolver.resolve.side_effect = InvalidPrimeIdentifierError("Invalid prime identifier: 0xdeadbeef")
    c = _app(fga=AsyncMock(), principal=_principal({"org:viewer"}), resolver=resolver)
    assert c.get("/v1/primes/0xdeadbeef/debt").status_code == 422


def test_enabled_without_fga_client_fails_closed():
    """Auth on but no OpenFGA client on state: 503, mirroring the verifier
    guard — an unguarded read would be an AttributeError 500."""
    c = _app(principal=_principal({"org:viewer"}))
    assert c.get(f"/v1/primes/{PROXY}/debt").status_code == 503


def test_openfga_down_fails_closed():
    from app.auth.fga import FgaError

    fga = AsyncMock()
    fga.check.side_effect = FgaError("down")
    c = _app(fga=fga, principal=_principal({"org:viewer"}))
    assert c.get(f"/v1/primes/{VAULT}/debt").status_code == 503


def test_list_filter_truncation_is_500():
    from app.auth.fga import FgaTruncated

    fga = AsyncMock()
    fga.list_objects.side_effect = FgaTruncated("ceiling")
    c = _app(fga=fga, principal=_principal({"org:viewer"}))
    assert c.get("/v1/primes").status_code == 500


# --- the REAL resolver path -------------------------------------------------
#
# Every test above hands the gate an AsyncMock resolver, which is precisely how
# a change to a repository's constructor (main #822 made reference_effective_at
# required) once stayed invisible: the gate raised TypeError -> 500 on every
# prime-scoped request while the suite stayed green. These build the real
# PrimeResolverRepository against a fake engine, so the constructor, the query
# and the row mapping all run.


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class _FakeRow:
    def __init__(self, **fields):
        self.__dict__.update(fields)


def _prime_row(vault_hex: str) -> _FakeRow:
    return _FakeRow(id=1, name="spark", external_id="4bd9ee3c", vault_hex=vault_hex)


class _FakeConnection:
    def __init__(self, rows):
        self._rows = rows
        # Every statement, not the last: the gate runs two, and asserting on one
        # overwritten field silently moved the address assertion onto the second.
        self.calls: list[dict] = []

    async def execute(self, _sql, params):
        self.calls.append(params)
        # The resolver's second statement lists the prime's wallets; the gate
        # needs none of them, and returning prime rows there would not map.
        return _FakeResult(self._rows if "prime_id" not in params else [])


class _FakeEngine:
    """Just enough engine for the resolver's two statements, no database."""

    def __init__(self, rows):
        self.connection = _FakeConnection(rows)

    @asynccontextmanager
    async def connect(self):
        yield self.connection


def _resolver_over(engine: _FakeEngine) -> PrimeResolverRepository:
    return PrimeResolverRepository(cast(AsyncEngine, engine))


def _real_path_client(rows, fga) -> TestClient:
    return _app(fga=fga, principal=_principal({"org:viewer"}), resolver=_resolver_over(_FakeEngine(rows)))


def test_real_resolver_maps_the_row_the_gate_reads_its_vault_from():
    fga = AsyncMock()
    fga.check.return_value = True
    c = _real_path_client([_prime_row("a" * 40)], fga)
    assert c.get(f"/v1/primes/{PROXY}/debt").status_code == 200
    fga.check.assert_awaited_once_with("user:u1", "can_view", f"prime:{VAULT}")


def test_real_resolver_returns_404_for_an_unknown_prime():
    c = _real_path_client([], AsyncMock())
    assert c.get(f"/v1/primes/{PROXY}/debt").status_code == 404


def test_real_resolver_passes_the_parsed_address_to_the_query():
    fga = AsyncMock()
    fga.check.return_value = True
    engine = _FakeEngine([_prime_row("a" * 40)])
    client = _app(fga=fga, principal=_principal({"org:viewer"}), resolver=_resolver_over(engine))
    client.get(f"/v1/primes/{PROXY}/debt")
    assert engine.connection.calls == [
        {"name": None, "address_hex": "b" * 40},
        {"prime_id": 1},
    ]


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
def test_prime_check_emits_a_decision_event_naming_the_resource(caplog, allowed, decision, reason):
    fga = AsyncMock()
    fga.check.return_value = allowed
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
    client = _allow_list_client(frozenset({VAULT.upper(), PROXY}))

    with caplog.at_level(logging.INFO, logger="app.api.deps"):
        assert client.get("/v1/primes").status_code == 200

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


def test_a_database_outage_behind_the_prime_gate_is_503_not_500(caplog):
    """The resolver reports a failed query as ValueError. Unhandled, that is a
    500 on every prime-scoped route the moment the database blips."""
    c = _app(fga=AsyncMock(), principal=_principal({"org:viewer"}), resolver=_resolver(fails=True))

    with caplog.at_level(logging.INFO, logger="app.api.deps"):
        response = c.get(f"/v1/primes/{PROXY}/debt")

    assert response.status_code == 503
    assert [(e["gate"], e["decision"], e["reason"]) for e in _events(caplog)] == [
        ("prime", "deny", "prime_lookup_unavailable")
    ]


def test_a_failed_wallet_listing_is_the_same_logged_503(caplog):
    """The gate runs a second query to widen the prime to its wallets, and that
    one can fail on its own. Unlogged, half the gate's 503s would be missing
    from the stream the rollout watches."""
    resolver = _resolver()
    resolver.list_proxies.side_effect = ValueError("Database query failed")
    c = _app(fga=AsyncMock(), principal=_principal({"org:viewer"}), resolver=resolver)

    with caplog.at_level(logging.INFO, logger="app.api.deps"):
        response = c.get(f"/v1/primes/{PROXY}/debt")

    assert response.status_code == 503
    assert [(e["gate"], e["decision"], e["reason"]) for e in _events(caplog)] == [
        ("prime", "deny", "prime_lookup_unavailable")
    ]


def _allow_list_client(objects: frozenset[str]) -> TestClient:
    fga = AsyncMock()
    fga.list_objects.return_value = objects
    return _app(fga=fga, principal=_principal({"org:viewer"}))


def test_a_malformed_object_id_in_the_tuple_store_does_not_take_the_route_down():
    """The tuple reconciler is a different system. One id that is not an
    address must not 500 /v1/primes for everyone — and cannot grant anything,
    since it matches no vault_address."""
    client = _allow_list_client(frozenset({VAULT, "prime-with-no-address", ""}))

    response = client.get("/v1/primes")

    assert response.status_code == 200
    assert response.json() == [VAULT]


def test_a_dropped_object_id_is_counted_on_the_decision_event(caplog):
    with caplog.at_level(logging.INFO, logger="app.api.deps"):
        client = _allow_list_client(frozenset({VAULT, "not-an-address"}))
        assert client.get("/v1/primes").status_code == 200

    (event,) = [r for r in caplog.records if getattr(r, "event", None) == deps.AUTHZ_EVENT]
    assert (event.prime_count, event.malformed_count) == (1, 1)


def test_an_uppercase_0x_prefix_is_normalised_not_dropped():
    """The address regex is case-sensitive on the `x`, so lowercasing has to
    happen BEFORE the parse or a `0X`-prefixed tuple silently loses access."""
    client = _allow_list_client(frozenset({VAULT.replace("0x", "0X").upper()}))

    assert client.get("/v1/primes").json() == [VAULT]
