"""Token verification: signature, issuer, audience, expiry, principal shape."""

from __future__ import annotations

import asyncio
import time

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm

from app.auth.jwt import (
    _CLOCK_SKEW_LEEWAY_SECONDS,
    _JWKS_MAX_STALE_SECONDS,
    _JWKS_TTL_SECONDS,
    JwksUnavailable,
    TokenError,
    TokenVerifier,
)

ISS = "https://kc.example/realms/archon"
AUD = "python-api"


@pytest.fixture(scope="module")
def keypair():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _jwk(private_key, kid: str = "k1", **over) -> dict:
    import json

    pub = json.loads(RSAAlgorithm.to_jwk(private_key.public_key()))
    pub.update({"kid": kid, "alg": "RS256", "use": "sig"})
    pub.update(over)
    return pub


def _jwks(private_key, kid: str = "k1") -> dict:
    return {"keys": [_jwk(private_key, kid)]}


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


class _CountingJwks:
    """A JWKS endpoint that records how often it was actually called."""

    def __init__(self, payload: dict, *, fail_after: int | None = None) -> None:
        self.payload = payload
        self.calls = 0
        self._fail_after = fail_after

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        if self._fail_after is not None and self.calls > self._fail_after:
            return httpx.Response(503, text="keycloak is unwell")
        return httpx.Response(200, json=self.payload)

    def verifier(self) -> TokenVerifier:
        return TokenVerifier(issuer=ISS, audience=AUD, http=httpx.AsyncClient(transport=httpx.MockTransport(self)))

    def start_failing(self) -> None:
        self._fail_after = 0


async def test_valid_token_yields_principal(keypair):
    p = await _verifier(keypair).verify(_token(keypair))
    assert p.subject == "user-123"
    assert "org:viewer" in p.roles and "org:admin" in p.roles
    assert p.organizations == frozenset({"acme"})
    assert p.fga_user == "user:user-123"


async def test_machine_token_keys_by_client_id(keypair):
    """Keycloak names the service account user `service-account-<client_id>`."""
    p = await _verifier(keypair).verify(
        _token(
            keypair,
            client_id="tuple-reconciler",
            sub="svc-account",
            preferred_username="service-account-tuple-reconciler",
        )
    )
    assert p.fga_user == "service:tuple-reconciler"


async def test_user_token_carrying_a_client_id_still_keys_by_sub(keypair):
    """A realm may map `client_id` into USER tokens. Keying on its presence
    alone would collapse every one of that client's users onto one FGA
    subject, handing each of them the union of the others' primes."""
    p = await _verifier(keypair).verify(_token(keypair, client_id="ui", preferred_username="alice"))
    assert p.client_id is None
    assert p.fga_user == "user:user-123"


async def test_client_id_without_the_service_account_username_is_ignored(keypair):
    """No `preferred_username` at all is not the service-account shape either."""
    p = await _verifier(keypair).verify(_token(keypair, client_id="ui"))
    assert p.fga_user == "user:user-123"


@pytest.mark.parametrize(
    "over",
    [{"aud": "someone-else"}, {"iss": "https://evil/realms/x"}, {"exp": int(time.time()) - 3600}],
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


# --- clock skew -------------------------------------------------------------


async def test_token_issued_slightly_in_the_future_is_accepted(keypair):
    """Keycloak's clock ahead of the API's is skew, not an attack.

    With PyJWT's default leeway of 0 this raises ImmatureSignatureError and
    every token 401s until the two clocks converge.
    """
    ahead = int(time.time()) + (_CLOCK_SKEW_LEEWAY_SECONDS - 15)
    p = await _verifier(keypair).verify(_token(keypair, iat=ahead, nbf=ahead))
    assert p.subject == "user-123"


async def test_token_expired_within_the_leeway_is_accepted(keypair):
    just_expired = int(time.time()) - (_CLOCK_SKEW_LEEWAY_SECONDS - 15)
    p = await _verifier(keypair).verify(_token(keypair, exp=just_expired))
    assert p.subject == "user-123"


async def test_token_beyond_the_leeway_is_still_rejected(keypair):
    """The leeway absorbs skew; it does not extend a token's life."""
    long_gone = int(time.time()) - (_CLOCK_SKEW_LEEWAY_SECONDS + 60)
    with pytest.raises(TokenError):
        await _verifier(keypair).verify(_token(keypair, exp=long_gone))


# --- JWKS availability ------------------------------------------------------


async def test_jwks_unreachable_with_no_cache_is_not_a_token_error(keypair):
    """503, not 401: our dependency failed, the caller's token did not."""
    endpoint = _CountingJwks(_jwks(keypair), fail_after=0)
    with pytest.raises(JwksUnavailable):
        await endpoint.verifier().verify(_token(keypair))


def _age_cache(verifier: TokenVerifier, seconds: float, monkeypatch) -> None:
    """Backdate the cached key set so the next verify tries to refresh."""
    monkeypatch.setattr(verifier, "_jwks_fetched_at", time.monotonic() - seconds)


async def test_cached_keys_are_served_when_a_refresh_fails(keypair, monkeypatch):
    endpoint = _CountingJwks(_jwks(keypair), fail_after=1)
    verifier = endpoint.verifier()
    assert (await verifier.verify(_token(keypair))).subject == "user-123"

    _age_cache(verifier, _JWKS_TTL_SECONDS + 60, monkeypatch)
    assert (await verifier.verify(_token(keypair))).subject == "user-123"
    assert endpoint.calls == 2  # it did try


async def test_cached_keys_past_the_staleness_ceiling_are_refused(keypair, monkeypatch):
    """The bound on the fallback above. Without it `_jwks_fetched_at` is never
    advanced on a failed refresh, so the branch re-enters forever and a key
    REVOKED in Keycloak keeps verifying tokens for the whole outage."""
    endpoint = _CountingJwks(_jwks(keypair), fail_after=1)
    verifier = endpoint.verifier()
    await verifier.verify(_token(keypair))

    _age_cache(verifier, _JWKS_MAX_STALE_SECONDS + 1, monkeypatch)
    with pytest.raises(JwksUnavailable, match="stale"):
        await verifier.verify(_token(keypair))


async def test_a_failed_refresh_does_not_restart_the_staleness_clock(keypair, monkeypatch):
    """Serving from cache must not look like a successful fetch: if it did, an
    outage could be ridden out indefinitely one blip at a time."""
    endpoint = _CountingJwks(_jwks(keypair), fail_after=1)
    verifier = endpoint.verifier()
    await verifier.verify(_token(keypair))

    _age_cache(verifier, _JWKS_TTL_SECONDS + 60, monkeypatch)
    fetched_at = verifier._jwks_fetched_at
    await verifier.verify(_token(keypair))  # served from cache; the refresh failed

    assert verifier._jwks_fetched_at == fetched_at


async def test_a_recovered_refresh_resets_the_staleness_window(keypair, monkeypatch):
    """The ceiling bounds an OUTAGE, not the process: once Keycloak answers
    again the cache is fresh and the fallback is available from scratch."""
    endpoint = _CountingJwks(_jwks(keypair))
    verifier = endpoint.verifier()
    await verifier.verify(_token(keypair))

    _age_cache(verifier, _JWKS_MAX_STALE_SECONDS + 1, monkeypatch)
    assert (await verifier.verify(_token(keypair))).subject == "user-123"

    endpoint.start_failing()  # the just-refreshed cache still serves
    _age_cache(verifier, _JWKS_TTL_SECONDS + 60, monkeypatch)
    assert (await verifier.verify(_token(keypair))).subject == "user-123"


async def test_unknown_kid_does_not_refetch_on_every_request(keypair):
    """An unknown kid is attacker-controlled; without a cooldown each one is a
    free unauthenticated fetch against Keycloak."""
    endpoint = _CountingJwks(_jwks(keypair))
    verifier = endpoint.verifier()
    for _ in range(5):
        with pytest.raises(TokenError, match="unknown signing key"):
            await verifier.verify(_token(keypair, kid="junk"))
    assert endpoint.calls == 1  # the cold-cache fetch, and nothing more


async def test_concurrent_cold_start_fetches_the_jwks_once(keypair):
    endpoint = _CountingJwks(_jwks(keypair))
    verifier = endpoint.verifier()
    results = await asyncio.gather(*(verifier.verify(_token(keypair)) for _ in range(8)))
    assert {p.subject for p in results} == {"user-123"}
    assert endpoint.calls == 1


# --- key selection ----------------------------------------------------------


async def test_encryption_key_sharing_the_kid_is_ignored(keypair):
    """Keycloak publishes RSA-OAEP encryption keys beside the signing keys.

    Matching one on kid alone would reject a perfectly good token.
    """
    enc = _jwk(keypair, "k1", use="enc")
    enc.pop("alg")  # an RSA-OAEP alg would already be unusable; `use` is the signal under test
    jwks = {"keys": [enc, _jwk(keypair, "k1")]}
    assert (await _verifier(keypair, jwks).verify(_token(keypair))).subject == "user-123"


def _forge_hs256(public_pem: bytes, claims: dict, kid: str = "k1") -> str:
    """Assemble an HS256 token keyed by the RSA PUBLIC key, by hand.

    PyJWT refuses to *encode* this, which is exactly why it has to be built
    the way an attacker would.
    """
    import base64
    import hashlib
    import hmac
    import json

    def seg(obj: dict) -> bytes:
        return base64.urlsafe_b64encode(json.dumps(obj, separators=(",", ":")).encode()).rstrip(b"=")

    signing_input = seg({"alg": "HS256", "typ": "JWT", "kid": kid}) + b"." + seg(claims)
    signature = hmac.new(public_pem, signing_input, hashlib.sha256).digest()
    return (signing_input + b"." + base64.urlsafe_b64encode(signature).rstrip(b"=")).decode()


async def test_algorithm_is_pinned_not_read_from_the_token(keypair):
    """Classic alg-confusion: an HS256 token keyed by the RSA PUBLIC key, which
    is public by definition. Verifying under whatever algorithm the key set
    advertises is what makes this forgery work; ``algorithms=["RS256"]`` is
    what stops it."""
    public_pem = keypair.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    now = int(time.time())
    forged = _forge_hs256(public_pem, {"iss": ISS, "aud": AUD, "sub": "attacker", "iat": now, "exp": now + 300})
    with pytest.raises(TokenError):
        await _verifier(keypair).verify(forged)
