"""HTTP client factories for unit and integration tests.

Both return ``httpx.AsyncClient`` over ``ASGITransport`` so async test bodies can
drive the FastAPI app without spinning up uvicorn.

* ``unit_client_factory`` — for unit endpoint tests. Lifespan is **off** so
  no DB engine is created. The caller supplies dependency overrides (typically
  ``AsyncMock`` services) keyed by the dependency function from
  ``app.api.deps``.
* ``integration_client_factory`` — for endpoint integration tests. Wires the
  per-test ``wrap_engine`` into ``get_engine`` and skips the real lifespan,
  since lifespan would otherwise create a fresh engine bypassing the savepoint.
  Callers must supply overrides for any service/repo dependency the tested
  endpoint reads from ``app.state``.
"""

from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine

from app.api.deps import get_engine
from app.config import Settings
from app.main import create_app

DependencyOverrides = Mapping[Callable[..., object], Callable[..., object]]


def _apply_overrides(app: FastAPI, overrides: DependencyOverrides | None) -> None:
    if overrides is None:
        return
    for dep, override in overrides.items():
        app.dependency_overrides[dep] = override


@asynccontextmanager
async def unit_client_factory(
    settings: Settings,
    *,
    dependency_overrides: DependencyOverrides | None = None,
) -> AsyncIterator[AsyncClient]:
    """Build a unit-test ``AsyncClient`` with mocked service dependencies.

    Lifespan is disabled — endpoints under unit test must not touch the DB.
    """
    app = create_app(settings)
    _apply_overrides(app, dependency_overrides)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


@asynccontextmanager
async def integration_client_factory(
    settings: Settings,
    wrap_engine: AsyncEngine,
    *,
    dependency_overrides: DependencyOverrides | None = None,
) -> AsyncIterator[AsyncClient]:
    """Build an integration-test ``AsyncClient`` backed by ``wrap_engine``.

    The real lifespan is skipped (it would build a fresh ``AsyncEngine`` that
    bypasses the wrap-engine's savepoint). ``app.state.engine`` is set directly
    so ``app.api.deps.get_engine`` resolves to the wrap-engine, but the other
    ``app.state`` slots normally populated by ``create_app``'s lifespan remain
    **unset**:

    * ``app.state.suraf_ratings``
    * ``app.state.asset_to_rating``
    * ``app.state.crypto_lending_risk_service``
    * ``app.state.model_registry``
    * ``app.state.receipt_token_lookup``

    Any endpoint reading those via ``app.api.deps`` will raise ``AttributeError``
    (FastAPI surfaces this as an opaque ``500``). Tests must pass
    ``dependency_overrides`` for every such dep they exercise — typically a
    ``lambda: AsyncMock(spec=...)`` or a fixture-built fake. This factory is
    **not** suitable for tests that need the real ``create_app`` lifespan
    (e.g. ``test_suraf_startup.py``).
    """
    app = create_app(settings)
    app.state.engine = wrap_engine
    app.dependency_overrides[get_engine] = lambda: wrap_engine
    _apply_overrides(app, dependency_overrides)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client
