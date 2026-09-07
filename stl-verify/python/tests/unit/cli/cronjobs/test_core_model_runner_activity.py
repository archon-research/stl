"""Unit tests for the tick wiring: resolve, compose, run, dispose."""

import os

import pytest

from cli.cronjobs.core_model_runner import activity as activity_module


class _StubEngine:
    def __init__(self) -> None:
        self.disposed = False

    async def dispose(self) -> None:
        self.disposed = True


@pytest.fixture()
def wired(monkeypatch):
    """Stub the adapters and record what run_tick wires together."""
    engine = _StubEngine()
    captured: dict = {}

    monkeypatch.setattr(os, "environ", {"DATABASE_URL": "postgresql://u:p@host:5432/db"})

    def _fake_engine_factory(url):
        captured["db_url"] = url
        return engine

    monkeypatch.setattr(activity_module, "create_db_engine", _fake_engine_factory)
    monkeypatch.setattr(activity_module.RunnerConfig, "resolve", classmethod(lambda cls, key: [f"cfg-{key}"]))

    async def _fake_run_markets(configs, writer, make_data_reader):
        captured["configs"] = configs
        captured["writer"] = writer
        if captured.get("fail"):
            raise RuntimeError("tick failed")

    monkeypatch.setattr(activity_module, "run_markets", _fake_run_markets)
    monkeypatch.setattr(activity_module, "PostgresCoreModelResultsWriter", lambda eng: ("writer", eng))
    return {"engine": engine, "captured": captured}


async def test_run_tick_wires_the_resolved_configs_to_a_postgres_writer(wired):
    await activity_module.run_tick("sparklend_usdt")
    assert wired["captured"]["configs"] == ["cfg-sparklend_usdt"]
    assert wired["captured"]["writer"] == ("writer", wired["engine"])
    # The entry point normalizes the bare DATABASE_URL to the asyncpg driver.
    assert wired["captured"]["db_url"] == "postgresql+asyncpg://u:p@host:5432/db"


async def test_run_tick_disposes_the_engine_on_success(wired):
    await activity_module.run_tick("all")
    assert wired["engine"].disposed is True


async def test_run_tick_disposes_the_engine_when_the_tick_fails(wired):
    wired["captured"]["fail"] = True
    with pytest.raises(RuntimeError):
        await activity_module.run_tick("all")
    assert wired["engine"].disposed is True
