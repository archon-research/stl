"""Minimal OpenFGA HTTP client: Check and ListObjects, with a preshared key.

The store is resolved by *name* at first use rather than plumbed in as an id,
because store and model ids are server-assigned and live in a ConfigMap in
another namespace. Omitting ``authorization_model_id`` makes OpenFGA use the
latest model — the CI drift gate (authz/) is what keeps that safe.
"""

from __future__ import annotations

import httpx


class FgaError(Exception):
    """OpenFGA was unreachable or returned an error — callers fail CLOSED."""


class FgaTruncated(FgaError):
    """ListObjects hit its result ceiling; a partial allow-list must not be used."""


def _as_object(resp: httpx.Response) -> dict:
    """The response body as a JSON object, or ValueError.

    Both callers turn that into FgaError -> 503. Unhandled, a 200 carrying an
    ingress error page escapes as a 500 with no decision event, so the Loki
    alert never fires for what is really an OpenFGA outage.
    """
    body = resp.json()
    if not isinstance(body, dict):
        raise ValueError(f"expected a JSON object from OpenFGA, got {type(body).__name__}")
    return body


class FgaClient:
    def __init__(
        self, *, base_url: str, api_key: str, store_name: str, http: httpx.AsyncClient, list_ceiling: int = 1000
    ) -> None:
        self._base = base_url.rstrip("/")
        self._store_name = store_name
        self._http = http
        self._headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        self._store_id: str | None = None
        self._ceiling = list_ceiling

    async def _post(self, path: str, body: dict) -> dict:
        try:
            resp = await self._http.post(f"{self._base}{path}", json=body, headers=self._headers, timeout=5.0)
            resp.raise_for_status()
            return _as_object(resp)
        except (httpx.HTTPError, ValueError) as exc:
            raise FgaError(str(exc)) from exc

    async def store_id(self) -> str:
        if self._store_id is None:
            try:
                resp = await self._http.get(f"{self._base}/stores", headers=self._headers, timeout=5.0)
                resp.raise_for_status()
                stores = _as_object(resp).get("stores", [])
            except (httpx.HTTPError, ValueError) as exc:
                raise FgaError(str(exc)) from exc
            match = [s for s in stores if isinstance(s, dict) and s.get("name") == self._store_name]
            if not match:
                raise FgaError(f"OpenFGA store {self._store_name!r} not found")
            sid = match[0].get("id")
            if not isinstance(sid, str) or not sid:
                raise FgaError(f"OpenFGA store {self._store_name!r} has no usable id")
            self._store_id = sid
        return self._store_id

    async def check(self, user: str, relation: str, obj: str) -> bool:
        sid = await self.store_id()
        body = {"tuple_key": {"user": user, "relation": relation, "object": obj}}
        data = await self._post(f"/stores/{sid}/check", body)
        # `is True`, not bool(): a body that stringifies the field would make
        # "false" truthy and turn a deny into an allow.
        return data.get("allowed") is True

    async def list_objects(self, user: str, relation: str, obj_type: str) -> frozenset[str]:
        """Object ids (without the ``type:`` prefix) the user has ``relation`` on.

        Raises FgaTruncated at the ceiling: the result feeds a SQL ``WHERE``,
        so a silently partial list would be a correctness bug that looks like
        missing data.
        """
        sid = await self.store_id()
        data = await self._post(f"/stores/{sid}/list-objects", {"user": user, "relation": relation, "type": obj_type})
        if "objects" not in data:
            raise FgaError("OpenFGA ListObjects returned no objects field")
        objs = data["objects"]
        if not isinstance(objs, list) or not all(isinstance(o, str) for o in objs):
            raise FgaError("OpenFGA ListObjects returned a malformed objects list")
        if len(objs) >= self._ceiling:
            raise FgaTruncated(f"ListObjects returned {len(objs)} objects — at the {self._ceiling} ceiling")
        prefix = f"{obj_type}:"
        return frozenset(o[len(prefix) :] if o.startswith(prefix) else o for o in objs)
