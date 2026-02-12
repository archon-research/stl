from fastapi.testclient import TestClient

from app.main import app


def test_status_endpoint_returns_ok():
    client = TestClient(app)

    response = client.get("/v1/status")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
