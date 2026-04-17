from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.main import configure_static_hosting


def test_static_hosting_serves_index_and_assets(tmp_path) -> None:
    static_dir = tmp_path / "static"
    static_dir.mkdir()
    (static_dir / "index.html").write_text("<html>ui</html>", encoding="utf-8")
    (static_dir / "assets").mkdir()
    (static_dir / "assets" / "app.js").write_text("console.log('ui')", encoding="utf-8")

    app = FastAPI()
    configure_static_hosting(app, static_dir)
    client = TestClient(app)

    root_response = client.get("/")
    asset_response = client.get("/assets/app.js")
    route_response = client.get("/portfolio/overview")
    dotted_route_response = client.get("/users/john.doe")

    assert root_response.status_code == 200
    assert root_response.text == "<html>ui</html>"
    assert asset_response.status_code == 200
    assert asset_response.text == "console.log('ui')"
    assert route_response.status_code == 200
    assert route_response.text == "<html>ui</html>"
    assert dotted_route_response.status_code == 200
    assert dotted_route_response.text == "<html>ui</html>"


def test_static_hosting_returns_404_for_missing_asset(tmp_path) -> None:
    static_dir = tmp_path / "static"
    static_dir.mkdir()
    (static_dir / "index.html").write_text("<html>ui</html>", encoding="utf-8")
    (static_dir / "assets").mkdir()

    app = FastAPI()
    configure_static_hosting(app, static_dir)
    client = TestClient(app)

    response = client.get("/assets/missing.js")

    assert response.status_code == 404
    assert response.json() == {"detail": "Not Found"}


def test_static_hosting_keeps_reserved_prefixes_unhandled(tmp_path) -> None:
    static_dir = tmp_path / "static"
    static_dir.mkdir()
    (static_dir / "index.html").write_text("<html>ui</html>", encoding="utf-8")

    app = FastAPI()

    configure_static_hosting(app, static_dir)
    client = TestClient(app)

    response = client.get("/v1/missing")

    assert response.status_code == 404
    assert response.json() == {"detail": "Not Found"}


def test_static_hosting_is_disabled_without_index(tmp_path) -> None:
    static_dir = tmp_path / "static"
    static_dir.mkdir()

    app = FastAPI()
    configure_static_hosting(app, static_dir)
    client = TestClient(app)

    response = client.get("/")

    assert response.status_code == 404
