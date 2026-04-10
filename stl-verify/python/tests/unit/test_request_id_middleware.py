import uuid

import httpx
import pytest
from fastapi import FastAPI

from app.middleware.request_id import RequestIdMiddleware, get_request_id


def _create_test_app() -> FastAPI:
    test_app = FastAPI()
    test_app.add_middleware(RequestIdMiddleware)

    @test_app.get("/ping")
    async def ping():
        return {"msg": "pong"}

    @test_app.get("/request-id")
    async def request_id_route():
        return {"request_id": get_request_id()}

    return test_app


@pytest.fixture
def test_app() -> FastAPI:
    return _create_test_app()


@pytest.fixture
def client(test_app: FastAPI) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=test_app)
    return httpx.AsyncClient(transport=transport, base_url="http://testserver")


@pytest.mark.asyncio
async def test_request_id_set_in_response_header(client: httpx.AsyncClient):
    async with client:
        response = await client.get("/ping")

    assert response.status_code == 200
    rid = response.headers.get("x-request-id")
    assert rid is not None
    # Validate it is a valid UUID4
    parsed = uuid.UUID(rid)
    assert parsed.version == 4
    assert str(parsed) == rid


@pytest.mark.asyncio
async def test_request_id_available_in_context(client: httpx.AsyncClient):
    async with client:
        response = await client.get("/request-id")

    assert response.status_code == 200
    body_rid = response.json()["request_id"]
    header_rid = response.headers.get("x-request-id")
    assert body_rid is not None
    assert body_rid == header_rid


@pytest.mark.asyncio
async def test_inbound_request_id_is_preserved(client: httpx.AsyncClient):
    inbound_id = "upstream-trace-id-abc-123"
    async with client:
        response = await client.get("/request-id", headers={"X-Request-ID": inbound_id})

    assert response.headers.get("x-request-id") == inbound_id
    assert response.json()["request_id"] == inbound_id


@pytest.mark.asyncio
async def test_request_id_isolated_per_request(client: httpx.AsyncClient):
    async with client:
        response1 = await client.get("/ping")
        response2 = await client.get("/ping")

    rid1 = response1.headers.get("x-request-id")
    rid2 = response2.headers.get("x-request-id")
    assert rid1 is not None
    assert rid2 is not None
    assert rid1 != rid2
