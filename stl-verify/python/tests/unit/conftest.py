import pytest

from app.main import app


@pytest.fixture(autouse=True)
def clear_dependency_overrides():
    yield
    app.dependency_overrides.clear()


@pytest.fixture
def authz_events(caplog):
    """Authorization decision events as logged, oldest first, read after the call."""
    import logging

    from app.api import deps

    caplog.set_level(logging.INFO, logger="app.api.deps")

    def _read() -> list[dict]:
        keys = ("gate", "decision", "reason", "principal", "resource", "status", "requested_prime")
        return [
            {k: getattr(r, k) for k in keys if hasattr(r, k)}
            for r in caplog.records
            if getattr(r, "event", None) == deps.AUTHZ_EVENT
        ]

    return _read
