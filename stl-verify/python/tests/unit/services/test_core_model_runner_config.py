"""Unit tests for the 3-layer RunnerConfig inheritance.

Inheritance chain (lowest wins):
  default_params.json  ->  market_configs.json[market_key]  ->  env var overrides
"""

import json
import os
from pathlib import Path

import pytest

from app.services.core_model_runner.config import RunnerConfig


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
    return dict(extra) if extra else {}


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


# Boolean env overrides are strict: the advisory-bounds decision covers
# out-of-range values, not unparseable strings — a typo'd boolean must not
# silently flip a model feature off.


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("true", True), ("TRUE", True), ("1", True), ("yes", True), ("false", False), ("0", False), ("no", False)],
)
def test_boolean_env_override_parses_known_spellings(market_configs_path, monkeypatch, raw, expected):
    monkeypatch.setattr(os, "environ", _env({"CORE_MODEL_JUMPS": raw}))
    cfg = _one("sparklend_usdt", market_configs_path)
    assert cfg.params["JUMPS"] is expected


def test_typoed_boolean_env_override_raises(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env({"CORE_MODEL_JUMPS": "ture"}))
    with pytest.raises(ValueError, match="invalid boolean for JUMPS"):
        _one("sparklend_usdt", market_configs_path)


def test_optional_float_env_override_parses(market_configs_path, monkeypatch):
    monkeypatch.setattr(os, "environ", _env({"CORE_MODEL_MC_TARGET_LTV": "0.8"}))
    cfg = _one("sparklend_usdt", market_configs_path)
    assert cfg.params["MC_TARGET_LTV"] == 0.8


def test_typoed_optional_float_env_override_raises(market_configs_path, monkeypatch):
    # A typo must not silently become None -- None is a meaningful model input
    # (no managed-close target) and params is recorded for auditability.
    monkeypatch.setattr(os, "environ", _env({"CORE_MODEL_MC_TARGET_LTV": "0.8x"}))
    with pytest.raises(ValueError, match="invalid float for MC_TARGET_LTV"):
        _one("sparklend_usdt", market_configs_path)


def test_stray_market_config_key_is_dropped_from_params(tmp_path, monkeypatch):
    # load_params filters unknown keys, so a mistyped key in
    # market_configs.json cannot leak into the recorded params audit trail.
    monkeypatch.setattr(os, "environ", _env())
    path = tmp_path / "market_configs.json"
    path.write_text(json.dumps({"sparklend_usdt": {"PROTOCOL": "SPARKLEND", "N_MCC": 25}}))
    cfg = _one("sparklend_usdt", path)
    assert "N_MCC" not in cfg.params


# Every test above runs against a synthetic tmp_path config. This one loads the
# file the service actually ships, which is how a trailing comma in
# market_configs.json once left the cronjob unstartable for every market while
# the whole suite stayed green.


def test_every_shipped_market_builds_a_runner_config(monkeypatch):
    monkeypatch.setattr(os, "environ", _env())
    configs = RunnerConfig.resolve("all")
    assert configs
    assert all("PROTOCOL" in c.params for c in configs)
