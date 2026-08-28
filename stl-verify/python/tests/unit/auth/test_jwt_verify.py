"""Token verification: signature, issuer, audience, expiry, principal shape."""

from __future__ import annotations

import time

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

from app.auth.jwt import TokenError, TokenVerifier

ISS = "https://kc.example/realms/archon"
AUD = "python-api"


@pytest.fixture(scope="module")
def keypair():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _jwks(private_key, kid: str = "k1") -> dict:
    import json

    pub = json.loads(RSAAlgorithm.to_jwk(private_key.public_key()))
    pub.update({"kid": kid, "alg": "RS256", "use": "sig"})
    return {"keys": [pub]}


def _token(private_key, *, kid: str = "k1", **over) -> str:
    now = int(time.time())
    claims = {
        "iss": ISS,
        "aud": AUD,
        "sub": "user-123",
        "iat": now,
        "exp": now + 300,
        "realm_access": {"roles": ["org:admin", "org:analyst", "org:viewer"]},
        "organization": {"acme": {}},
    }
    claims.update(over)
    return jwt.encode(claims, private_key, algorithm="RS256", headers={"kid": kid})


def _verifier(private_key, jwks=None) -> TokenVerifier:
    payload = jwks or _jwks(private_key)
    transport = httpx.MockTransport(lambda req: httpx.Response(200, json=payload))
    return TokenVerifier(issuer=ISS, audience=AUD, http=httpx.AsyncClient(transport=transport))


async def test_valid_token_yields_principal(keypair):
    p = await _verifier(keypair).verify(_token(keypair))
    assert p.subject == "user-123"
    assert "org:viewer" in p.roles and "org:admin" in p.roles
    assert p.organizations == frozenset({"acme"})
    assert p.fga_user == "user:user-123"


async def test_machine_token_keys_by_client_id(keypair):
    p = await _verifier(keypair).verify(_token(keypair, client_id="tuple-reconciler", sub="svc-account"))
    assert p.fga_user == "service:tuple-reconciler"


@pytest.mark.parametrize(
    "over",
    [{"aud": "someone-else"}, {"iss": "https://evil/realms/x"}, {"exp": int(time.time()) - 10}],
)
async def test_wrong_aud_iss_or_expired_rejected(keypair, over):
    with pytest.raises(TokenError):
        await _verifier(keypair).verify(_token(keypair, **over))


async def test_unknown_kid_rejected_after_refresh(keypair):
    with pytest.raises(TokenError, match="unknown signing key"):
        await _verifier(keypair).verify(_token(keypair, kid="rotated-away"))


async def test_token_signed_by_other_key_rejected(keypair):
    other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    with pytest.raises(TokenError):
        await _verifier(keypair).verify(_token(other))  # same kid, wrong key
