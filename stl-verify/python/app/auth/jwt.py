"""JWT verification against the Keycloak realm's JWKS.

Verifies signature, ``iss``, ``aud`` and ``exp``; extracts the principal.
The JWKS is cached and refreshed on an unknown ``kid`` so key rotation in
Keycloak does not need a redeploy.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import httpx
import jwt
from jwt.api_jwk import PyJWKSet

_JWKS_TTL_SECONDS = 600


class TokenError(Exception):
    """The token is missing, malformed, expired, or not for us."""


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


class TokenVerifier:
    def __init__(self, *, issuer: str, audience: str, http: httpx.AsyncClient) -> None:
        self._issuer = issuer.rstrip("/")
        self._audience = audience
        self._http = http
        self._jwks_url = f"{self._issuer}/protocol/openid-connect/certs"
        self._jwks: PyJWKSet | None = None
        self._jwks_fetched_at = 0.0

    async def _jwks_set(self, *, force: bool = False) -> PyJWKSet:
        stale = time.monotonic() - self._jwks_fetched_at > _JWKS_TTL_SECONDS
        if self._jwks is None or stale or force:
            resp = await self._http.get(self._jwks_url, timeout=5.0)
            resp.raise_for_status()
            self._jwks = PyJWKSet.from_dict(resp.json())
            self._jwks_fetched_at = time.monotonic()
        return self._jwks

    async def _key_for(self, token: str):
        try:
            kid = jwt.get_unverified_header(token).get("kid")
        except jwt.PyJWTError as exc:
            raise TokenError("malformed token") from exc
        if not kid:
            raise TokenError("token has no kid")
        for attempt in (False, True):  # second pass refreshes the JWKS (rotation)
            jwks = await self._jwks_set(force=attempt)
            for key in jwks.keys:
                if key.key_id == kid:
                    return key
        raise TokenError("unknown signing key")

    async def verify(self, token: str) -> Principal:
        key = await self._key_for(token)
        try:
            claims = jwt.decode(
                token,
                key.key,
                algorithms=[key.algorithm_name or "RS256"],
                audience=self._audience,
                issuer=self._issuer,
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
