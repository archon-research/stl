"""Unit tests for the 3-layer RunnerConfig inheritance.

Inheritance chain (lowest wins):
  default_params.json  ->  market_configs.json[market_key]  ->  env var overrides
"""

import json
import os
from pathlib import Path

import pytest

from app.services.core_model_runner.config import (
    _MARKET_CONFIGS_DEFAULT,
    RunnerConfig,
    _load_market_configs,
)


@pytest.fixture
def market_configs_path(tmp_path: Path) -> Path:
    cfg = {
        "sparklend_usdt": {"PROTOCOL": "SPARKLEND", "LOAN_TOKEN": "USDT"},
        "morpho_cbbtc-usdc": {"PROTOCOL": "MORPHO", "MORPHO_MARKET": "CBBTC", "LOAN_TOKEN": "USDC"},
    }
    p = tmp_path / "market_configs.json"
    p.write_text(json.dumps(cfg))
    return p


def _env(extra: dict | None = None) -> dict:
    base = {"DATABASE_URL": "postgresql://localhost/test"}
    if extra:
        base.update(extra)
    return base


def _one(market_key: str, market_configs_path: Path) -> RunnerConfig:
    (cfg,) = RunnerConfig.resolve(market_key, market_configs_path=market_configs_path)
    return cfg


def test_loads_protocol_from_market_config(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env())
    cfg = _one("sparklend_usdt", market_configs_path)
    assert cfg.params["PROTOCOL"] == "SPARKLEND"
    assert cfg.params["LOAN_TOKEN"] == "USDT"


def test_default_params_applied_for_non_market_specific_values(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env())
    cfg = _one("sparklend_usdt", market_configs_path)
    # FORECAST_STEP default is 14 — market config doesn't override it
    assert cfg.params["FORECAST_STEP"] == 14


def test_env_var_overrides_market_config_and_defaults(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env({"CORE_MODEL_N_MC": "100"}))
    cfg = _one("sparklend_usdt", market_configs_path)
    assert cfg.params["N_MC"] == 100


def test_env_var_can_override_protocol_from_market_config(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env({"CORE_MODEL_PROTOCOL": "AAVE"}))
    cfg = _one("sparklend_usdt", market_configs_path)
    assert cfg.params["PROTOCOL"] == "AAVE"


def test_market_key_stored_on_config(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env())
    cfg = _one("sparklend_usdt", market_configs_path)
    assert cfg.market_key == "sparklend_usdt"


def test_morpho_market_loaded_from_market_config(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env())
    cfg = _one("morpho_cbbtc-usdc", market_configs_path)
    assert cfg.params["MORPHO_MARKET"] == "CBBTC"


# resolve() -- the single entry point shared by the CLI and the Temporal activity


def test_resolve_returns_one_config_for_a_single_market(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env())
    configs = RunnerConfig.resolve("sparklend_usdt", market_configs_path=market_configs_path)
    assert [c.market_key for c in configs] == ["sparklend_usdt"]


def test_resolve_expands_all_to_every_market(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env())
    configs = RunnerConfig.resolve("all", market_configs_path=market_configs_path)
    assert {c.market_key for c in configs} == {"sparklend_usdt", "morpho_cbbtc-usdc"}


def test_resolve_applies_env_overrides_to_every_market(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env({"CORE_MODEL_N_MC": "100"}))
    configs = RunnerConfig.resolve("all", market_configs_path=market_configs_path)
    assert all(c.params["N_MC"] == 100 for c in configs)


def test_resolve_preserves_market_specific_params_across_all(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env())
    configs = RunnerConfig.resolve("all", market_configs_path=market_configs_path)
    morpho = next(c for c in configs if c.market_key == "morpho_cbbtc-usdc")
    assert morpho.params["MORPHO_MARKET"] == "CBBTC"


def test_resolve_rejects_an_unknown_market(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env())
    with pytest.raises(ValueError, match="unknown market_key"):
        RunnerConfig.resolve("nope", market_configs_path=market_configs_path)


# Every test above runs against a synthetic tmp_path config. These two load the
# file the service actually ships, which is how a trailing comma in
# market_configs.json once left the cronjob unstartable for every market while
# the whole suite stayed green.


def test_shipped_market_configs_parse_and_every_market_declares_a_protocol():
    configs = _load_market_configs(_MARKET_CONFIGS_DEFAULT)
    assert configs
    assert [key for key, params in configs.items() if "PROTOCOL" not in params] == []


def test_every_shipped_market_builds_a_runner_config(monkeypatch):
    monkeypatch.setattr(os, "environ", _env())
    configs = RunnerConfig.resolve("all")
    assert {c.market_key for c in configs} == set(_load_market_configs(_MARKET_CONFIGS_DEFAULT))
