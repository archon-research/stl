"""The no-``prime_id`` risk routes, run against a real database.

They read as pool-level and were gated as such. They are not: with no prime_id
an Aave-like share is resolved by ``_WALLET_LOOKUP_SQL``, which orders the
current holders by source rank then balance and takes the top row, and every
returned amount is then scaled by THAT holder's share. Two primes holding the
same receipt token is all it takes for the response to be one of them.

The claim is about SQL, so it is asserted against Postgres: the seed leaves
spark and grove both holding aUSDC, grove larger, and the gate has to name
grove's vault. A mocked service cannot get this wrong, which is why the unit
tests that pinned these routes as unscoped were wrong for two years.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from pydantic import SecretStr

from app.auth.jwt import Principal
from app.config import Settings
from app.main import create_app
from tests.integration.seed import GROVE_MAINNET_ALM_HEX, insert_allocation_position, seed_prime_fan_out

# Declared by 20260305_120000_create_prime_debts.sql, and what the OpenFGA
# object id is keyed on.
SPARK_VAULT = "0x691a6c29e9e96dd897718305427ad5d534db16ba"
GROVE_VAULT = "0x26512a41c8406800f21094a7a7a0f980f6e25d43"

TOKEN = "analyst-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}
ANALYST = Principal(
    subject="u1", roles=frozenset({"org:viewer", "org:analyst"}), organizations=frozenset(), client_id=None
)


class _StubVerifier:
    async def verify(self, token: str) -> Principal:
        assert token == TOKEN
        return ANALYST


class _RecordingFga:
    """Answers from a fixed allow-list and remembers every object id asked about."""

    def __init__(self) -> None:
        self.allowed: frozenset[str] = frozenset()
        self.checked: list[str] = []

    async def check(self, user: str, relation: str, obj: str) -> bool:  # noqa: ARG002
        self.checked.append(obj)
        return obj in self.allowed

    async def list_objects(self, user: str, relation: str, obj_type: str) -> frozenset[str]:  # noqa: ARG002
        return self.allowed


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def seeded(db_url: str) -> dict[str, str]:
    """spark and grove holding the same receipt token, grove the larger."""
    await seed_prime_fan_out(db_url)
    conn = await asyncpg.connect(db_url)
    try:
        row = await conn.fetchrow(
            "SELECT id, encode(receipt_token_address, 'hex') AS addr "
            "FROM receipt_token WHERE symbol = 'aUSDC' AND chain_id = 1"
        )
        token_id = await conn.fetchval(
            "SELECT id FROM token WHERE chain_id = 1 AND address = decode($1, 'hex')", row["addr"]
        )
        grove_id = await conn.fetchval("SELECT id FROM prime WHERE name = 'grove'")
        # The fan-out seed leaves both primes on 1000; break the tie so the
        # winner is the balance ordering rather than whatever the plan returns.
        await insert_allocation_position(
            conn,
            token_id=token_id,
            prime_id=grove_id,
            proxy_hex=GROVE_MAINNET_ALM_HEX,
            balance=Decimal("2500"),
            block=2000,
            tx="2b" * 32,
            direction="in",
        )
        # A supply row at or after the newest position, so the share resolves
        # and the routes answer 200 rather than share_data_missing.
        await conn.execute(
            "INSERT INTO token_total_supply "
            "(chain_id, token_id, total_supply, block_number, block_timestamp, source) "
            "VALUES (1, $1, 10000, 2000, NOW(), 'sweep')",
            token_id,
        )
    finally:
        await conn.close()
    return {"id": str(row["id"]), "address": "0x" + row["addr"]}


def _settings(async_db_url: str, mapping: Path, *, auth_enabled: bool) -> Settings:
    return Settings.model_validate(
        {
            "database_url": SecretStr(async_db_url),
            "suraf_mappings_file": mapping,
            "core_model_mappings_file": mapping,
            "auth_enabled": auth_enabled,
            "oidc_issuer": "http://keycloak.invalid/realms/archon",
            "oidc_audience": "python-api",
            "openfga_url": "http://openfga.invalid:8080",
        }
    )


@pytest.fixture(scope="module")
def empty_mapping(tmp_path_factory) -> Path:
    path = tmp_path_factory.mktemp("mappings") / "empty_mapping.json"
    path.write_text("{}")
    return path


@contextmanager
def _app(async_db_url: str, mapping: Path, *, auth_enabled: bool) -> Iterator[TestClient]:
    with TestClient(create_app(_settings(async_db_url, mapping, auth_enabled=auth_enabled))) as client:
        yield client


@pytest.fixture(scope="module")
def lit(async_db_url: str, empty_mapping: Path) -> Iterator[TestClient]:
    """The real factory with AUTH_ENABLED true — no dependency overridden.

    The verifier and OpenFGA client the lifespan built talk to hosts that do
    not exist; they are swapped for stubs, everything else is the real app.
    """
    with _app(async_db_url, empty_mapping, auth_enabled=True) as client:
        client.app.state.verifier = _StubVerifier()
        yield client


@pytest.fixture(scope="module")
def dark(async_db_url: str, empty_mapping: Path) -> Iterator[TestClient]:
    with _app(async_db_url, empty_mapping, auth_enabled=False) as client:
        yield client


@pytest.fixture
def fga(lit: TestClient) -> _RecordingFga:
    stub = _RecordingFga()
    lit.app.state.fga = stub
    return stub


# Every route that takes no prime_id, by id and by (chain, address).
_TEMPLATES = {
    "bad-debt-by-id": "/v1/risk/{id}/bad-debt?gap_pct=0.1",
    "bad-debt-by-address": "/v1/risk/1/{address}/bad-debt?gap_pct=0.1",
    "breakdown-by-id": "/v1/risk/{id}/breakdown",
    "breakdown-by-address": "/v1/risk/1/{address}/breakdown",
}


@pytest.fixture(params=sorted(_TEMPLATES))
def path(request, seeded: dict[str, str]) -> str:
    return _TEMPLATES[request.param].format(**seeded)


def test_the_gate_names_the_largest_holders_prime(lit: TestClient, fga: _RecordingFga, path: str) -> None:
    """grove holds more aUSDC than spark, so the figures would be grove's."""
    response = lit.get(path, headers=AUTH)

    assert response.status_code == 404
    assert fga.checked == [f"prime:{GROVE_VAULT}"]


def test_access_to_the_other_holder_is_not_access_to_this_one(lit: TestClient, fga: _RecordingFga, path: str) -> None:
    """The exact leak: an analyst cleared for spark reads grove's exposure and
    its modelled bad debt at any gap they choose."""
    fga.allowed = frozenset({f"prime:{SPARK_VAULT}"})

    response = lit.get(path, headers=AUTH)

    assert response.status_code == 404
    assert fga.checked == [f"prime:{GROVE_VAULT}"]


def test_the_permitted_holder_gets_through_the_gate(lit: TestClient, fga: _RecordingFga, path: str) -> None:
    fga.allowed = frozenset({f"prime:{GROVE_VAULT}"})

    response = lit.get(path, headers=AUTH)

    assert response.status_code == 200
    assert fga.checked == [f"prime:{GROVE_VAULT}"]


def test_the_permitted_answer_is_byte_for_byte_the_dark_one(
    lit: TestClient, dark: TestClient, fga: _RecordingFga, path: str
) -> None:
    """Gating these routes must not move a number: the share is still the
    largest holder's, resolved once and reused rather than looked up twice."""
    fga.allowed = frozenset({f"prime:{GROVE_VAULT}"})

    permitted = lit.get(path, headers=AUTH)
    unauthenticated = dark.get(path)

    assert (permitted.status_code, permitted.json()) == (unauthenticated.status_code, unauthenticated.json())


def test_an_unauthenticated_caller_never_reaches_the_lookup(lit: TestClient, fga: _RecordingFga, path: str) -> None:
    response = lit.get(path)

    assert response.status_code == 401
    assert fga.checked == []
