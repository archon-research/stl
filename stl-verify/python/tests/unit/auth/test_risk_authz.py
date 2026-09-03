"""The risk router needs BOTH gates, not just the role gate (ADR-015).

``/v1/risk/*`` is gated to ``org:analyst`` per router, but every route that
scopes to a prime takes the prime id from the QUERY STRING or the BODY, never
a path segment. Without a per-resource check an analyst correctly 403'd on
``/v1/primes/{id}/risk-capital`` can read the same prime here instead.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app.api import deps
from app.api.v1 import risk
from app.auth.jwt import Principal
from app.domain.entities.allocation import EthAddress
from app.domain.entities.receipt_token import ReceiptTokenInfo
from app.domain.entities.risk import GapSweepDetails, ModelName, RrcResult
from app.services.model_registry import ModelRegistry

VAULT = "0x" + "a" * 40
PRIME = "0x" + "b" * 40
OTHER_PRIME = "0x" + "c" * 40
ASSET_ID = 1234
CHAIN_ID = 1
TOKEN_ADDRESS = "0x" + "01" * 20

RECEIPT_TOKEN_INFO = ReceiptTokenInfo(
    receipt_token_id=ASSET_ID,
    protocol_id=10,
    underlying_token_id=20,
    receipt_token_address=bytes.fromhex("01" * 20),
    chain_id=CHAIN_ID,
    protocol_name="aave_v3",
    receipt_token_token_id=30,
)


class _AlwaysApplies:
    risk_model: ModelName = "gap_sweep"

    def applies_to(self, asset_id: int, prime_id: EthAddress) -> bool:  # noqa: ARG002
        return True

    async def compute(self, asset_id: int, prime_id: EthAddress, overrides: Mapping[str, Any]) -> RrcResult:  # noqa: ARG002
        return RrcResult(
            asset_id=asset_id,
            prime_id=prime_id,
            rrc_usd=Decimal("1200.5"),
            comparable_crr_pct=Decimal("12.00"),
            risk_model="gap_sweep",
            details=GapSweepDetails(risk_model="gap_sweep", gap_pct=Decimal("0.15"), loss_usd=Decimal("1200.5")),
        )


ANALYST_ROLES = frozenset({"org:analyst", "org:viewer"})


def _principal(roles: Iterable[str] = ANALYST_ROLES) -> Principal:
    return Principal(subject="u1", roles=frozenset(roles), organizations=frozenset(), client_id=None)


def _client(*, fga, principal: Principal | None) -> TestClient:
    """The real risk router, mounted the way ``create_app`` mounts it."""
    app = FastAPI()
    app.state.fga = fga
    app.include_router(risk.router, prefix="/v1", dependencies=[Depends(deps.require_analyst)])

    lookup = AsyncMock()
    lookup.get = AsyncMock(return_value=RECEIPT_TOKEN_INFO)
    lookup.get_by_chain_and_address = AsyncMock(return_value=RECEIPT_TOKEN_INFO)

    service = AsyncMock()
    service.get_risk_breakdown = AsyncMock(return_value=SimpleNamespace(items=[]))

    app.dependency_overrides[deps.get_receipt_token_lookup] = lambda: lookup
    app.dependency_overrides[deps.get_model_registry] = lambda: ModelRegistry([_AlwaysApplies()])
    app.dependency_overrides[deps.get_crypto_lending_risk_service] = lambda: service
    if principal is not None:
        app.dependency_overrides[deps.get_principal] = lambda: principal
    return TestClient(app)


@pytest.fixture
def resolves_to_vault(monkeypatch):
    monkeypatch.setattr(deps, "_vault_for", AsyncMock(return_value=VAULT))


def _deny() -> AsyncMock:
    fga = AsyncMock()
    fga.check.return_value = False
    return fga


def _allow() -> AsyncMock:
    fga = AsyncMock()
    fga.check.return_value = True
    return fga


# Each case is (method, path, json body) carrying a prime id the caller may not
# view. The role gate passes on all of them — org:analyst is held.
PRIME_SCOPED_ROUTES = [
    pytest.param("GET", f"/v1/risk/rrc?asset_id={ASSET_ID}&prime_id={PRIME}", None, id="get-rrc"),
    pytest.param("POST", "/v1/risk/rrc/scenario", {"asset_id": ASSET_ID, "prime_id": PRIME}, id="post-scenario"),
    pytest.param("POST", "/v1/risk/rrc", {"asset_id": ASSET_ID, "prime_id": PRIME}, id="post-rrc-deprecated"),
    pytest.param("GET", f"/v1/risk/{ASSET_ID}/breakdown?prime_id={PRIME}", None, id="breakdown-by-id"),
    pytest.param(
        "GET",
        f"/v1/risk/{CHAIN_ID}/{TOKEN_ADDRESS}/breakdown?prime_id={PRIME}",
        None,
        id="breakdown-by-address",
    ),
]


@pytest.mark.parametrize("method,path,body", PRIME_SCOPED_ROUTES)
def test_analyst_cannot_read_a_prime_they_may_not_view(resolves_to_vault, method, path, body):
    fga = _deny()
    response = _client(fga=fga, principal=_principal()).request(method, path, json=body)
    assert response.status_code == 403
    fga.check.assert_awaited_once_with("user:u1", "can_view", f"prime:{VAULT}")


@pytest.mark.parametrize("method,path,body", PRIME_SCOPED_ROUTES)
def test_permitted_prime_reaches_the_handler(resolves_to_vault, method, path, body):
    fga = _allow()
    response = _client(fga=fga, principal=_principal()).request(method, path, json=body)
    assert response.status_code == 200
    fga.check.assert_awaited_once_with("user:u1", "can_view", f"prime:{VAULT}")


@pytest.mark.parametrize("method,path,body", PRIME_SCOPED_ROUTES)
def test_unknown_prime_is_404(monkeypatch, method, path, body):
    monkeypatch.setattr(deps, "_vault_for", AsyncMock(return_value=None))
    fga = _allow()
    response = _client(fga=fga, principal=_principal()).request(method, path, json=body)
    assert response.status_code == 404
    fga.check.assert_not_awaited()


@pytest.mark.parametrize("method,path,body", PRIME_SCOPED_ROUTES)
def test_no_check_runs_while_auth_is_dark(resolves_to_vault, method, path, body):
    fga = _deny()  # would refuse if it were ever consulted
    response = _client(fga=fga, principal=None).request(method, path, json=body)
    assert response.status_code == 200
    fga.check.assert_not_awaited()


def test_viewer_is_still_stopped_by_the_role_gate(resolves_to_vault):
    """The per-resource check is added to the coarse gate, not instead of it."""
    fga = _allow()
    response = _client(fga=fga, principal=_principal({"org:viewer"})).get(
        f"/v1/risk/rrc?asset_id={ASSET_ID}&prime_id={PRIME}"
    )
    assert response.status_code == 403
    fga.check.assert_not_awaited()


@pytest.mark.parametrize(
    "path",
    [
        f"/v1/risk/{ASSET_ID}/breakdown",
        f"/v1/risk/{CHAIN_ID}/{TOKEN_ADDRESS}/breakdown",
    ],
)
def test_pool_level_breakdown_is_not_prime_scoped(resolves_to_vault, path):
    """``prime_id`` is optional on the breakdown routes; omitted, the response
    is pool-level and there is no per-resource object to check."""
    fga = _deny()
    assert _client(fga=fga, principal=_principal()).get(path).status_code == 200
    fga.check.assert_not_awaited()


def test_bad_debt_routes_take_no_prime_and_stay_role_gated_only(resolves_to_vault):
    fga = _deny()
    client = _client(fga=fga, principal=_principal())
    client.app.dependency_overrides[deps.get_crypto_lending_risk_service]().get_bad_debt_legacy = AsyncMock(
        return_value=Decimal("5")
    )
    assert client.get(f"/v1/risk/{ASSET_ID}/bad-debt?gap_pct=0.1").status_code == 200
    fga.check.assert_not_awaited()


def test_malformed_prime_id_in_the_query_is_422_not_500(resolves_to_vault):
    fga = _allow()
    response = _client(fga=fga, principal=_principal()).get(f"/v1/risk/rrc?asset_id={ASSET_ID}&prime_id=nonsense")
    assert response.status_code == 422


def test_malformed_prime_id_in_the_body_is_422_not_500(resolves_to_vault):
    fga = _allow()
    response = _client(fga=fga, principal=_principal()).post(
        "/v1/risk/rrc/scenario", json={"asset_id": ASSET_ID, "prime_id": "nonsense"}
    )
    assert response.status_code == 422


def test_a_body_that_will_not_parse_is_left_to_the_route_validator():
    """Deciding authorization on a body nobody could read is worse than a 422
    from the validator a moment later."""
    fga = _allow()
    response = _client(fga=fga, principal=_principal()).post(
        "/v1/risk/rrc/scenario", content=b"{not json", headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 422
    fga.check.assert_not_awaited()


def test_openfga_outage_fails_closed_on_a_risk_route(resolves_to_vault):
    from app.auth.fga import FgaError

    fga = AsyncMock()
    fga.check.side_effect = FgaError("down")
    response = _client(fga=fga, principal=_principal()).get(f"/v1/risk/rrc?asset_id={ASSET_ID}&prime_id={OTHER_PRIME}")
    assert response.status_code == 503
