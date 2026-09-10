"""Unit tests for the CORE model runner service (the body of one cronjob tick)."""

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import cast

import pytest

from app.ports.core_model_data_reader import CoreModelDataReader
from app.ports.core_model_results_writer import CoreModelResultsWriter
from app.risk_engine.core_model.convergence import MonteCarloDiagnostics
from app.risk_engine.core_model.runner import CoreModelPipelineResult
from app.services.core_model_runner import service as service_module
from app.services.core_model_runner.service import run_markets


def _unconverged_result(market_key: str) -> CoreModelPipelineResult:
    return CoreModelPipelineResult(
        market_key=market_key,
        crr_el_pct=Decimal("0.004485"),
        crr_es_pct=Decimal("0.175909"),
        crr_var_pct=Decimal("0"),
        hhi=None,
        protocol="SPARKLEND",
        forecast_step=14,
        n_mc=100,
        copula_type="T-COPULA",
        computed_at=datetime(2026, 9, 7, tzinfo=timezone.utc),
        params={"PROTOCOL": "SPARKLEND", "N_MC": 100},
        mc_diagnostics=MonteCarloDiagnostics(
            n_scenarios=100,
            n_loss_scenarios=3,
            n_catastrophic_scenarios=1,
            catastrophic_loss_pct=1.0,
            crr_el_se_pct=0.0042,
            crr_el_rel_se=0.94,
        ),
    )


class _RecordingWriter:
    def __init__(self) -> None:
        self.inserted: list[CoreModelPipelineResult] = []

    async def insert(self, result: CoreModelPipelineResult) -> None:
        self.inserted.append(result)


async def test_an_unconverged_market_is_written_and_its_diagnostics_logged(monkeypatch, caplog):
    async def _fake_run(config, data_reader, inputs_dir):
        return _unconverged_result(config.market_key)

    monkeypatch.setattr(service_module, "run", _fake_run)
    writer = _RecordingWriter()

    with caplog.at_level(logging.INFO, logger=service_module.logger.name):
        await run_markets([_cfg("sparklend_usdc")], writer, _reader_factory)

    assert [r.market_key for r in writer.inserted] == ["sparklend_usdc"]
    assert "crr_el_se_pct=0.0042 crr_el_rel_se=0.94" in caplog.text
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 2
    assert all(w.startswith("crr_el not converged market_key=sparklend_usdc") for w in warnings)


def _cfg(market_key: str):
    class _Cfg:
        market_key: str = ""
        params: dict = {}
        orderbook_source = "parquet"
        price_source = "parquet"
        position_source = "parquet"

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
