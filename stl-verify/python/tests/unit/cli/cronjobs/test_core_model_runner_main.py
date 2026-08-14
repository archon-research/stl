"""Unit tests for the core-model-runner entry point: mode selection and scheduling."""

import os
from datetime import timedelta

import pytest

from cli.cronjobs.core_model_runner import main as main_module


@pytest.fixture()
def modes(monkeypatch):
    """Record which mode the entry point picked, without running either."""
    called: list[str] = []

    async def _once():
        called.append("once")

    async def _worker():
        called.append("worker")

    monkeypatch.setattr(main_module, "run_once", _once)
    monkeypatch.setattr(main_module, "run_worker", _worker)
    return called


def test_defaults_to_the_temporal_worker(modes):
    main_module.main([])
    assert modes == ["worker"]


def test_once_flag_runs_a_single_pass(modes):
    main_module.main(["--once"])
    assert modes == ["once"]


def test_interval_defaults_to_daily(monkeypatch):
    monkeypatch.setattr(os, "environ", {})
    assert main_module._interval() == timedelta(hours=24)


def test_interval_is_overridable(monkeypatch):
    monkeypatch.setattr(os, "environ", {"CORE_MODEL_RUN_INTERVAL_HOURS": "6"})
    assert main_module._interval() == timedelta(hours=6)


def test_missing_market_key_is_a_hard_failure(monkeypatch):
    monkeypatch.setattr(os, "environ", {})
    with pytest.raises(KeyError):
        main_module._market_key()


async def test_worker_registers_under_the_shared_cronjob_name(monkeypatch):
    monkeypatch.setattr(os, "environ", {"CORE_MODEL_MARKET_KEY": "all"})
    captured = {}

    async def _run_cronjob(spec, **kwargs):
        captured["spec"] = spec

    monkeypatch.setattr(main_module, "run_cronjob", _run_cronjob)
    await main_module.run_worker()

    spec = captured["spec"]
    assert spec.name == "core-model-runner"
    assert spec.workflow_args == ["all"]
    assert spec.interval == timedelta(hours=24)
