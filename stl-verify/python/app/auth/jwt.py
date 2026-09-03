"""JWT verification against the Keycloak realm's JWKS.

Verifies signature, ``iss``, ``aud`` and ``exp``; extracts the principal.
The JWKS is cached and refreshed on an unknown ``kid`` so key rotation in
Keycloak does not need a redeploy.

Two failure modes, deliberately different: a bad token is the caller's
problem (``TokenError`` -> 401), while keys we cannot fetch are ours
(``JwksUnavailable`` -> 503, matching how an OpenFGA outage surfaces). A
token is never rejected as invalid because our own dependency was down.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import httpx
import jwt
from jwt.api_jwk import PyJWK, PyJWKSet

from app.logging import get_logger

logger = get_logger(__name__)

_JWKS_TTL_SECONDS = 600
# Floor between two *forced* refreshes. An unknown `kid` is the rotation
# signal, but it is also attacker-controlled: without a floor, a stream of
# tokens carrying junk kids turns every unauthenticated request into a fetch
# against Keycloak. Well under the TTL, so a real rotation is still picked up
# within seconds.
_JWKS_MIN_REFRESH_SECONDS = 30
# Keycloak and the API are different pods on different nodes. PyJWT's default
# leeway is 0, so any positive skew makes `iat`/`nbf` land in the future and
# raises ImmatureSignatureError on a token that is perfectly valid.
_CLOCK_SKEW_LEEWAY_SECONDS = 45
# Only RS256 is accepted. Read from the JWKS entry instead, a key served with
# a weaker `alg` would decide how its own signature is checked.
_ALGORITHMS = ["RS256"]


class TokenError(Exception):
    """The token is missing, malformed, expired, or not for us."""


class JwksUnavailable(Exception):
    """The signing keys could not be fetched and nothing usable is cached.

    Our dependency failed, not the caller's token — surfaces as 503.
    """


@dataclass(frozen=True, slots=True)
class Principal:
    """The authenticated caller, derived from a verified token.

    ``subject`` is Keycloak's ``sub`` — the same value the tuple reconciler
    writes as ``user:<sub>``. Never email: it changes and is re-assignable.
    """

    subject: str
    roles: frozenset[str]
    organizations: frozenset[str]
    client_id: str | None

    @property
    def fga_user(self) -> str:
        # Machines authenticate with client credentials; their token has a
        # service-account `sub`, but the graph keys them by client id.
        return f"service:{self.client_id}" if self.client_id else f"user:{self.subject}"


def _is_signing_key(key: PyJWK) -> bool:
    """False for keys Keycloak publishes for encryption, not signature.

    A realm's JWKS carries RSA-OAEP entries alongside the RS256 signing keys.
    They are never valid for verification, and matching one on ``kid`` alone
    would reject a good token. PyJWT exposes no public accessor for the raw
    JWK, hence the guarded private read.
    """
    raw = getattr(key, "_jwk_data", None)
    if not isinstance(raw, dict):
        return True
    return raw.get("use") != "enc"


class TokenVerifier:
    def __init__(self, *, issuer: str, audience: str, http: httpx.AsyncClient, jwks_url: str | None = None) -> None:
        # `issuer` must equal the `iss` CLAIM in tokens (Keycloak derives it
        # from KC_HOSTNAME) and doubles as the browser-facing OAuth base.
        # `jwks_url` is where THIS process fetches keys — in-cluster that is
        # the Service address, which is not the token issuer.
        self._issuer = issuer.rstrip("/")
        self._audience = audience
        self._http = http
        self._jwks_url = jwks_url or f"{self._issuer}/protocol/openid-connect/certs"
        self._jwks: PyJWKSet | None = None
        self._jwks_fetched_at = 0.0
        # Serialises refreshes. Without it, N concurrent requests arriving on a
        # cold cache (or right after a rotation) each fire their own fetch and
        # race on the assignment.
        self._refresh_lock = asyncio.Lock()

    def _needs_fetch(self, *, force: bool) -> bool:
        if self._jwks is None:
            return True
        age = time.monotonic() - self._jwks_fetched_at
        if force:
            return age >= _JWKS_MIN_REFRESH_SECONDS
        return age > _JWKS_TTL_SECONDS

    async def _fetch_jwks(self) -> PyJWKSet:
        resp = await self._http.get(self._jwks_url, timeout=5.0)
        resp.raise_for_status()
        return PyJWKSet.from_dict(resp.json())

    async def _jwks_set(self, *, force: bool = False) -> PyJWKSet:
        if not self._needs_fetch(force=force) and self._jwks is not None:
            return self._jwks
        async with self._refresh_lock:
            # Re-checked under the lock: whoever held it may already have done
            # the work, and a burst must cost Keycloak one fetch, not N.
            if not self._needs_fetch(force=force) and self._jwks is not None:
                return self._jwks
            try:
                jwks = await self._fetch_jwks()
            except (httpx.HTTPError, jwt.PyJWTError, ValueError) as exc:
                if self._jwks is not None:
                    # Keys rotate rarely and the cached set stays valid for the
                    # old kids; refusing every request during a Keycloak blip
                    # would be the worse failure.
                    logger.warning(
                        "jwks refresh failed, serving cached keys",
                        extra={"event": "authn.jwks_refresh_failed", "jwks_url": self._jwks_url, "error": str(exc)},
                    )
                    return self._jwks
                logger.error(
                    "jwks unavailable and nothing cached",
                    extra={"event": "authn.jwks_unavailable", "jwks_url": self._jwks_url, "error": str(exc)},
                )
                raise JwksUnavailable(str(exc)) from exc
            self._jwks = jwks
            self._jwks_fetched_at = time.monotonic()
            return jwks

    async def _key_for(self, token: str) -> PyJWK:
        try:
            kid = jwt.get_unverified_header(token).get("kid")
        except jwt.PyJWTError as exc:
            raise TokenError("malformed token") from exc
        if not kid:
            raise TokenError("token has no kid")
        for attempt in (False, True):  # second pass refreshes the JWKS (rotation)
            jwks = await self._jwks_set(force=attempt)
            for key in jwks.keys:
                if key.key_id == kid and _is_signing_key(key):
                    return key
        raise TokenError("unknown signing key")

    async def verify(self, token: str) -> Principal:
        key = await self._key_for(token)
        try:
            claims = jwt.decode(
                token,
                key.key,
                algorithms=_ALGORITHMS,
                audience=self._audience,
                issuer=self._issuer,
                leeway=_CLOCK_SKEW_LEEWAY_SECONDS,
                options={"require": ["exp", "iat", "sub"]},
            )
        except jwt.PyJWTError as exc:
            raise TokenError(str(exc)) from exc
        orgs = claims.get("organization") or {}
        return Principal(
            subject=claims["sub"],
            roles=frozenset(claims.get("realm_access", {}).get("roles", [])),
            organizations=frozenset(orgs.keys() if isinstance(orgs, dict) else orgs),
            client_id=claims.get("client_id") or claims.get("clientId"),
        )
