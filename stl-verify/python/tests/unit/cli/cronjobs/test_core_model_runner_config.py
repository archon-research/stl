"""Unit tests for the 3-layer RunnerConfig inheritance.

Inheritance chain (lowest wins):
  default_params.json  ->  market_configs.json[market_key]  ->  env var overrides
"""

import json
import os
from pathlib import Path

import pytest

from cli.cronjobs.core_model_runner.config import RunnerConfig


@pytest.fixture
def market_configs_path(tmp_path: Path) -> Path:
    cfg = {
        "sparklend_usdt": {"PROTOCOL": "SPARKLEND", "LOAN_TOKEN": "USDT"},
        "morpho_cbbtc-usdc": {"PROTOCOL": "MORPHO", "MORPHO_MARKET": "CBBTC", "LOAN_TOKEN": "USDC"},
    }
    p = tmp_path / "market_configs.json"
    p.write_text(json.dumps(cfg))
    return p


def _env(market_key: str, extra: dict | None = None) -> dict:
    base = {
        "DATABASE_URL": "postgresql://localhost/test",
        "CORE_MODEL_MARKET_KEY": market_key,
    }
    if extra:
        base.update(extra)
    return base


def test_loads_protocol_from_market_config(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env("sparklend_usdt"))
    cfg = RunnerConfig.from_env(market_configs_path=market_configs_path)
    assert cfg.params["PROTOCOL"] == "SPARKLEND"
    assert cfg.params["LOAN_TOKEN"] == "USDT"


def test_default_params_applied_for_non_market_specific_values(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env("sparklend_usdt"))
    cfg = RunnerConfig.from_env(market_configs_path=market_configs_path)
    # FORECAST_STEP default is 14 — market config doesn't override it
    assert cfg.params["FORECAST_STEP"] == 14


def test_env_var_overrides_market_config_and_defaults(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env("sparklend_usdt", {"CORE_MODEL_N_MC": "100"}))
    cfg = RunnerConfig.from_env(market_configs_path=market_configs_path)
    assert cfg.params["N_MC"] == 100


def test_env_var_can_override_protocol_from_market_config(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env("sparklend_usdt", {"CORE_MODEL_PROTOCOL": "AAVE"}))
    cfg = RunnerConfig.from_env(market_configs_path=market_configs_path)
    assert cfg.params["PROTOCOL"] == "AAVE"


def test_market_key_stored_on_config(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env("sparklend_usdt"))
    cfg = RunnerConfig.from_env(market_configs_path=market_configs_path)
    assert cfg.market_key == "sparklend_usdt"


def test_unknown_market_key_raises(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env("unknown_market"))
    with pytest.raises(ValueError, match="unknown_market"):
        RunnerConfig.from_env(market_configs_path=market_configs_path)


def test_missing_market_key_env_var_raises(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", {"DATABASE_URL": "postgresql://localhost/test"})
    with pytest.raises((KeyError, ValueError)):
        RunnerConfig.from_env(market_configs_path=market_configs_path)


def test_morpho_market_loaded_from_market_config(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env("morpho_cbbtc-usdc"))
    cfg = RunnerConfig.from_env(market_configs_path=market_configs_path)
    assert cfg.params["MORPHO_MARKET"] == "CBBTC"


# ---------------------------------------------------------------------------
# all_from_env -- runs every market in market_configs.json
# ---------------------------------------------------------------------------


def test_all_from_env_returns_one_config_per_market(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env("all"))
    configs = RunnerConfig.all_from_env(market_configs_path=market_configs_path)
    assert len(configs) == 2
    keys = {c.market_key for c in configs}
    assert keys == {"sparklend_usdt", "morpho_cbbtc-usdc"}


def test_all_from_env_applies_env_overrides_to_every_market(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env("all", {"CORE_MODEL_N_MC": "100"}))
    configs = RunnerConfig.all_from_env(market_configs_path=market_configs_path)
    assert all(c.params["N_MC"] == 100 for c in configs)


def test_all_from_env_preserves_market_specific_params(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env("all"))
    configs = RunnerConfig.all_from_env(market_configs_path=market_configs_path)
    morpho = next(c for c in configs if c.market_key == "morpho_cbbtc-usdc")
    assert morpho.params["MORPHO_MARKET"] == "CBBTC"
