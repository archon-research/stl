"""Unit tests for the CORE model runner service (the body of one cronjob tick)."""

import pytest

from app.services.core_model_runner import service as service_module
from app.services.core_model_runner.service import async_db_url, run_markets


def _cfg(market_key: str):
    class _Cfg:
        database_url = "postgresql://u:p@host:5432/db"
        inputs_dir = "/inputs"
        market_key: str = ""
        params: dict = {}

    cfg = _Cfg()
    cfg.market_key = market_key
    cfg.params = {"PROTOCOL": "SPARKLEND", "N_MC": 10000}
    return cfg


class _StubEngine:
    def __init__(self) -> None:
        self.disposed = False

    async def dispose(self) -> None:
        self.disposed = True


@pytest.fixture()
def collected(monkeypatch):
    """Capture which markets ran, with the engine and model stubbed out."""
    engine = _StubEngine()
    ran: list[str] = []
    failing: set[str] = set()

    monkeypatch.setattr(service_module, "create_async_engine", lambda *a, **k: engine)

    class _Service:
        def __init__(self, _engine): ...

        async def run_market(self, cfg):
            if cfg.market_key in failing:
                raise RuntimeError(f"boom {cfg.market_key}")
            ran.append(cfg.market_key)

    monkeypatch.setattr(service_module, "CoreModelRunnerService", _Service)
    return {"ran": ran, "failing": failing, "engine": engine}


async def test_runs_every_configured_market(collected):
    await run_markets([_cfg("sparklend_usdt"), _cfg("sparklend_dai")])
    assert collected["ran"] == ["sparklend_usdt", "sparklend_dai"]


async def test_a_failing_market_does_not_stop_its_siblings(collected):
    collected["failing"].add("galaxy")
    with pytest.raises(RuntimeError):
        await run_markets([_cfg("galaxy"), _cfg("sparklend_usdt")])
    assert collected["ran"] == ["sparklend_usdt"]


async def test_failed_markets_are_reported_in_the_error(collected):
    collected["failing"].update({"galaxy", "anchorage"})
    with pytest.raises(RuntimeError, match="galaxy"):
        await run_markets([_cfg("galaxy"), _cfg("anchorage")])


async def test_engine_is_disposed_even_when_a_market_fails(collected):
    collected["failing"].add("galaxy")
    with pytest.raises(RuntimeError):
        await run_markets([_cfg("galaxy")])
    assert collected["engine"].disposed is True


async def test_empty_config_list_is_rejected():
    with pytest.raises(ValueError, match="no market configs"):
        await run_markets([])


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("postgresql://u:p@h:5432/db", "postgresql+asyncpg://u:p@h:5432/db"),
        ("postgres://u:p@h:5432/db", "postgresql+asyncpg://u:p@h:5432/db"),
        # asyncpg takes `ssl`, not libpq's `sslmode`, so the parameter is dropped.
        ("postgresql://u:p@h:5432/db?sslmode=require", "postgresql+asyncpg://u:p@h:5432/db"),
    ],
)
def test_async_db_url_normalisation(raw, expected):
    assert async_db_url(raw) == expected
