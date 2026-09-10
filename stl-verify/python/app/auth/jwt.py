"""JWT verification against the Keycloak realm's JWKS.

Two failure modes, deliberately different: a bad token is the caller's problem
(``TokenError`` -> 401), while keys we cannot fetch are ours
(``JwksUnavailable`` -> 503, matching how an OpenFGA outage surfaces). A token
is never rejected as invalid because our own dependency was down.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import cast

import httpx
import jwt
from jwt.api_jwk import PyJWK, PyJWKSet

from app.logging import get_logger

logger = get_logger(__name__)

_JWKS_TTL_SECONDS = 600
# Floor between two *forced* refreshes. An unknown `kid` is the rotation signal,
# but it is also attacker-controlled: without a floor a stream of junk kids
# turns every unauthenticated request into a fetch against Keycloak.
_JWKS_MIN_REFRESH_SECONDS = 30
# Ceiling on the fallback below, which never advances `_jwks_fetched_at` and so
# would re-enter forever. Six refresh intervals: rides out a Keycloak restart,
# but a key REVOKED there stops verifying within the hour, not for the outage.
_JWKS_MAX_STALE_SECONDS = 3600
# Keycloak and the API are different pods on different nodes. PyJWT's default
# leeway is 0, so any positive skew makes `iat`/`nbf` land in the future and
# raises ImmatureSignatureError on a token that is perfectly valid.
_CLOCK_SKEW_LEEWAY_SECONDS = 45
# Pinned, never read from the token or the JWKS entry: a key served with a
# weaker `alg` would otherwise decide how its own signature is checked.
_ALGORITHMS = ["RS256"]


class TokenError(Exception):
    """The token is missing, malformed, expired, or not for us."""


class JwksUnavailable(Exception):
    """Keys could not be fetched, and the cache is empty or too stale to serve.

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
    #: Set only for a client-credentials token — see ``_service_account_client``.
    client_id: str | None

    @property
    def fga_user(self) -> str:
        return f"service:{self.client_id}" if self.client_id else f"user:{self.subject}"


def _string_set(container: object, key: str, label: str) -> frozenset[str]:
    """Names from a claim that is a list of strings, or a dict keyed by them.

    A bare string is refused rather than iterated: "acme" would otherwise
    become four single-character orgs and feed authorization.
    """
    if container is None:
        return frozenset()
    if not isinstance(container, dict):
        raise TokenError(f"{label} is not an object")
    value: object = cast("dict[str, object]", container).get(key)
    if value is None:
        return frozenset()
    if isinstance(value, dict):
        value = list(value.keys())
    if not isinstance(value, list):
        raise TokenError(f"{label} is not a list of strings")
    names: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise TokenError(f"{label} is not a list of strings")
        names.append(item)
    return frozenset(names)


def _service_account_client(claims: dict) -> str | None:
    """The client id, but only for a token issued to the CLIENT itself.

    A ``client_id`` claim alone is not that signal — a realm may map it into
    user tokens too, and keying on it there collapses all of that client's
    users onto one FGA subject. Keycloak's ``service-account-<client_id>``
    username does distinguish them.
    """
    client_id = claims.get("client_id") or claims.get("clientId")
    if not isinstance(client_id, str) or not client_id:
        return None
    # preferred_username alone is user-controlled, so a user named
    # service-account-<client> could claim that client's identity. A
    # client-credentials token also carries no interactive session and its azp
    # is the client itself; neither is settable by the end user.
    if claims.get("sid") or claims.get("session_state"):
        return None
    if claims.get("azp") not in (None, client_id):
        return None
    return client_id if claims.get("preferred_username") == f"service-account-{client_id}" else None


def _is_signing_key(key: PyJWK) -> bool:
    """False for keys Keycloak publishes for encryption, not signature.

    Matching an RSA-OAEP entry on ``kid`` alone would reject a good token.
    PyJWT exposes no public accessor for the raw JWK, hence the private read.
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
        # Separate from _jwks_fetched_at, which only advances on success so the
        # staleness ceiling measures the outage. This one advances on every
        # attempt, so a failing Keycloak costs one fetch per interval, not one
        # per request.
        self._jwks_attempted_at = 0.0
        # Serialises refreshes: without it, N requests on a cold cache each fire
        # their own fetch and race on the assignment.
        self._refresh_lock = asyncio.Lock()

    def _needs_fetch(self, *, force: bool) -> bool:
        if self._jwks is None:
            return True
        since_attempt = time.monotonic() - self._jwks_attempted_at
        if since_attempt < _JWKS_MIN_REFRESH_SECONDS:
            return False
        if force:
            return True
        return time.monotonic() - self._jwks_fetched_at > _JWKS_TTL_SECONDS

    async def _fetch_jwks(self) -> PyJWKSet:
        resp = await self._http.get(self._jwks_url, timeout=5.0)
        resp.raise_for_status()
        body = resp.json()
        if not isinstance(body, dict):
            raise ValueError(f"expected a JSON object from the JWKS endpoint, got {type(body).__name__}")
        return PyJWKSet.from_dict(body)

    def _serve_cached(self, jwks: PyJWKSet) -> PyJWKSet:
        age = time.monotonic() - self._jwks_fetched_at
        if age > _JWKS_MAX_STALE_SECONDS:
            raise JwksUnavailable(f"cached keys are {int(age)}s stale, past the {_JWKS_MAX_STALE_SECONDS}s ceiling")
        return jwks

    async def _jwks_set(self, *, force: bool = False) -> PyJWKSet:
        if not self._needs_fetch(force=force) and self._jwks is not None:
            return self._serve_cached(self._jwks)
        async with self._refresh_lock:
            # Re-checked under the lock: whoever held it may already have done
            # the work, and a burst must cost Keycloak one fetch, not N.
            if not self._needs_fetch(force=force) and self._jwks is not None:
                return self._serve_cached(self._jwks)
            self._jwks_attempted_at = time.monotonic()
            try:
                jwks = await self._fetch_jwks()
            except (httpx.HTTPError, jwt.PyJWTError, ValueError, AttributeError, TypeError) as exc:
                age = time.monotonic() - self._jwks_fetched_at
                if self._jwks is not None and age <= _JWKS_MAX_STALE_SECONDS:
                    # The cached set stays valid for the old kids, and refusing
                    # every request during a Keycloak blip is the worse failure.
                    logger.warning(
                        "jwks refresh failed, serving cached keys",
                        extra={
                            "event": "authn.jwks_refresh_failed",
                            "jwks_url": self._jwks_url,
                            "stale_seconds": int(age),
                            "error": str(exc),
                        },
                    )
                    return self._jwks
                cached = self._jwks is not None
                logger.error(
                    "jwks unavailable",
                    extra={
                        "event": "authn.jwks_unavailable",
                        "jwks_url": self._jwks_url,
                        "stale_seconds": int(age) if cached else None,
                        "error": str(exc),
                    },
                )
                if cached:
                    raise JwksUnavailable(
                        f"cached keys are {int(age)}s stale, past the {_JWKS_MAX_STALE_SECONDS}s ceiling: {exc}"
                    ) from exc
                raise JwksUnavailable(str(exc)) from exc
            self._jwks = jwks
            self._jwks_fetched_at = time.monotonic()
            return jwks

    async def _keys_for(self, token: str) -> list[PyJWK]:
        try:
            kid = jwt.get_unverified_header(token).get("kid")
        except jwt.PyJWTError as exc:
            raise TokenError("malformed token") from exc
        if not kid:
            raise TokenError("token has no kid")
        for attempt in (False, True):  # second pass refreshes the JWKS (rotation)
            jwks = await self._jwks_set(force=attempt)
            candidates = [k for k in jwks.keys if k.key_id == kid and _is_signing_key(k)]
            if candidates:
                return candidates
        raise TokenError("unknown signing key")

    async def verify(self, token: str) -> Principal:
        # Keycloak can publish more than one key under a kid across a rotation,
        # so a failure on the first is not a failure on the token.
        keys = await self._keys_for(token)
        last: Exception | None = None
        claims = None
        for key in keys:
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
                break
            except jwt.PyJWTError as exc:
                last = exc
        if claims is None:
            raise TokenError(str(last)) from last
        return Principal(
            subject=claims["sub"],
            roles=_string_set(claims.get("realm_access"), "roles", "realm_access.roles"),
            organizations=_string_set(claims, "organization", "organization"),
            client_id=_service_account_client(claims),
        )
