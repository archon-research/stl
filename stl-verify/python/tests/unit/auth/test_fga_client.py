"""OpenFGA client: store-by-name, check, list-objects, and the truncation guard."""

from __future__ import annotations

import httpx
import pytest

from app.auth.fga import FgaClient, FgaError, FgaTruncated


def _client(handler, ceiling: int = 1000) -> FgaClient:
    return FgaClient(
        base_url="http://fga",
        api_key="k",
        store_name="auth",
        http=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        list_ceiling=ceiling,
    )


def _routes(objects: list[str], allowed: bool = True):
    def handler(req: httpx.Request) -> httpx.Response:
        assert req.headers["authorization"] == "Bearer k"
        if req.url.path == "/stores":
            return httpx.Response(200, json={"stores": [{"id": "S1", "name": "auth"}, {"id": "S2", "name": "other"}]})
        if req.url.path == "/stores/S1/check":
            return httpx.Response(200, json={"allowed": allowed})
        if req.url.path == "/stores/S1/list-objects":
            return httpx.Response(200, json={"objects": objects})
        return httpx.Response(404)

    return handler


async def test_resolves_store_by_name_and_checks():
    c = _client(_routes([], allowed=True))
    assert await c.store_id() == "S1"
    assert await c.check("user:u", "can_view", "prime:0xabc") is True


async def test_list_objects_strips_type_prefix():
    c = _client(_routes(["prime:0xabc", "prime:0xdef"]))
    assert await c.list_objects("user:u", "can_view", "prime") == frozenset({"0xabc", "0xdef"})


async def test_list_objects_at_ceiling_raises_truncated():
    c = _client(_routes([f"prime:{i}" for i in range(3)]), ceiling=3)
    with pytest.raises(FgaTruncated):
        await c.list_objects("user:u", "can_view", "prime")


async def test_unreachable_fails_closed():
    def boom(req):
        raise httpx.ConnectError("down")

    with pytest.raises(FgaError):
        await _client(boom).check("user:u", "can_view", "prime:x")


async def test_missing_store_is_an_error():
    def handler(req):
        return httpx.Response(200, json={"stores": [{"id": "S9", "name": "other"}]})

    with pytest.raises(FgaError, match="not found"):
        await _client(handler).store_id()
