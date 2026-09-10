import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app.api.provenance import get_requested_provenance, resolve_or_422
from app.domain.provenance import Provenance


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()

    @app.get("/probe")
    def probe(requested: Provenance | None = Depends(get_requested_provenance)):
        return {"requested": None if requested is None else requested.value}

    @app.get("/indexed-only")
    def indexed_only(requested: Provenance | None = Depends(get_requested_provenance)):
        effective = resolve_or_422(requested, available=frozenset({Provenance.INDEXED}))
        return {"source": effective.value}

    return TestClient(app)


def test_absent_parameters_request_nothing_so_the_endpoint_defaults(client: TestClient):
    assert client.get("/probe").json() == {"requested": None}


@pytest.mark.parametrize("value", ["indexed", "reference", "both"])
def test_accepts_each_provenance_by_name(client: TestClient, value: str):
    assert client.get(f"/probe?source={value}").json() == {"requested": value}


def test_rejects_an_unknown_provenance(client: TestClient):
    assert client.get("/probe?source=sky").status_code == 422


@pytest.mark.parametrize(
    ("query", "expected"),
    [("reference=true", "reference"), ("reference=false", "indexed")],
)
def test_honours_the_superseded_boolean(client: TestClient, query: str, expected: str):
    assert client.get(f"/probe?{query}").json() == {"requested": expected}


def test_accepts_the_two_parameters_when_they_agree(client: TestClient):
    response = client.get("/probe?source=reference&reference=true")

    assert response.json() == {"requested": "reference"}


def test_rejects_the_two_parameters_when_they_disagree(client: TestClient):
    # Silently preferring one would answer a different question than one of the
    # two the caller asked.
    response = client.get("/probe?source=indexed&reference=true")

    assert response.status_code == 422
    assert "conflicts" in response.json()["detail"]


def test_narrows_the_default_to_the_only_provenance_available(client: TestClient):
    assert client.get("/indexed-only").json() == {"source": "indexed"}


def test_refuses_a_provenance_the_endpoint_cannot_serve(client: TestClient):
    response = client.get("/indexed-only?source=reference")

    assert response.status_code == 422
    assert "not available here" in response.json()["detail"]
