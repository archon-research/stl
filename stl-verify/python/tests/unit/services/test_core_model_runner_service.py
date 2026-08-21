"""Unit tests for the CORE model runner service (the body of one cronjob tick)."""

from typing import cast

import pytest

from app.ports.core_model_data_reader import CoreModelDataReader
from app.ports.core_model_results_writer import CoreModelResultsWriter
from app.services.core_model_runner import service as service_module
from app.services.core_model_runner.service import run_markets


def _cfg(market_key: str):
    class _Cfg:
        inputs_dir = "/inputs"
        market_key: str = ""
        params: dict = {}

    cfg = _Cfg()
    cfg.market_key = market_key
    cfg.params = {"PROTOCOL": "SPARKLEND", "N_MC": 10000}
    return cfg


@pytest.fixture()
def collected(monkeypatch):
    """Capture which markets ran, with the model pipeline stubbed out."""
    ran: list[str] = []
    failing: set[str] = set()

    async def _fake_run_market(cfg, writer, data_reader):
        if cfg.market_key in failing:
            raise RuntimeError(f"boom {cfg.market_key}")
        ran.append(cfg.market_key)

    monkeypatch.setattr(service_module, "_run_market", _fake_run_market)
    return {"ran": ran, "failing": failing}


def _writer() -> CoreModelResultsWriter:
    return cast(CoreModelResultsWriter, object())


def _reader_factory(cfg) -> CoreModelDataReader:
    return cast(CoreModelDataReader, object())


async def test_runs_every_configured_market(collected):
    await run_markets([_cfg("sparklend_usdt"), _cfg("sparklend_dai")], _writer(), _reader_factory)
    assert collected["ran"] == ["sparklend_usdt", "sparklend_dai"]


async def test_a_failing_market_does_not_stop_its_siblings(collected):
    collected["failing"].add("galaxy")
    with pytest.raises(RuntimeError):
        await run_markets([_cfg("galaxy"), _cfg("sparklend_usdt")], _writer(), _reader_factory)
    assert collected["ran"] == ["sparklend_usdt"]


async def test_failed_markets_are_reported_in_the_error(collected):
    collected["failing"].update({"galaxy", "anchorage"})
    with pytest.raises(RuntimeError, match="galaxy"):
        await run_markets([_cfg("galaxy"), _cfg("anchorage")], _writer(), _reader_factory)


async def test_a_failing_reader_factory_counts_as_a_failed_market(collected):
    def _broken_factory(cfg):
        raise RuntimeError("no reader for this market")

    with pytest.raises(RuntimeError, match="sparklend_usdt"):
        await run_markets([_cfg("sparklend_usdt")], _writer(), _broken_factory)
    assert collected["ran"] == []


async def test_empty_config_list_is_rejected():
    with pytest.raises(ValueError, match="no market configs"):
        await run_markets([], _writer(), _reader_factory)
