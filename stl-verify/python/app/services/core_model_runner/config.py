"""Environment-variable config for the core-model-runner cronjob.

Params are resolved in three layers (lowest wins):
  1. default_params.json          -- canonical defaults for all markets
  2. market_configs.json[key]     -- market-specific required overrides
  3. CORE_MODEL_* env vars        -- runtime overrides (e.g. N_MC=100 for a quick test)

The entry point (cli/cronjobs/core_model_runner) owns DATABASE_URL and the
market-key selection; this module only resolves model params.
"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from app.risk_engine.core_model.config import DEFAULTS, INPUTS_DIR, load_params

_INPUTS_DEFAULT = Path(INPUTS_DIR)
_MARKET_CONFIGS_DEFAULT = _INPUTS_DEFAULT / "market_configs.json"


def _load_market_configs(path: Path) -> dict[str, dict]:
    with open(path) as f:
        data = json.load(f)
    return {k: v for k, v in data.items() if not k.startswith("_")}


@dataclass(frozen=True)
class RunnerConfig:
    market_key: str
    inputs_dir: Path
    params: dict = field(default_factory=dict)

    @classmethod
    def resolve(
        cls,
        market_key: str,
        *,
        market_configs_path: Path = _MARKET_CONFIGS_DEFAULT,
    ) -> "list[RunnerConfig]":
        """Resolve a market key -- or the literal "all" -- to the configs to run.

        Both the one-shot CLI and the Temporal activity go through here so a
        scheduled run and a hand-run cover exactly the same ground.
        """
        market_configs = _load_market_configs(market_configs_path)
        if market_key == "all":
            return [cls._build(key, market_configs) for key in market_configs]
        if market_key not in market_configs:
            available = sorted(market_configs)
            raise ValueError(f"unknown market_key {market_key!r}; available markets: {available}")
        return [cls._build(market_key, market_configs)]

    @classmethod
    def _build(cls, market_key: str, market_configs: dict[str, dict]) -> "RunnerConfig":
        env_overrides = {k: _coerce(k, os.environ[env_key]) for k, env_key in _ENV_MAP.items() if env_key in os.environ}
        # load_params layers defaults -> overrides AND drops unknown keys, so a
        # stray key in market_configs.json cannot leak into the audit trail
        # (params is recorded verbatim in the results table).
        params = load_params(overrides={**market_configs[market_key], **env_overrides})
        return cls(market_key=market_key, inputs_dir=_INPUTS_DEFAULT, params=params)


# Maps CORE param name -> env var name
_ENV_MAP: dict[str, str] = {
    "PROTOCOL": "CORE_MODEL_PROTOCOL",
    "NETWORK": "CORE_MODEL_NETWORK",
    "MORPHO_MARKET": "CORE_MODEL_MORPHO_MARKET",
    "GALAXY_TYPE": "CORE_MODEL_GALAXY_TYPE",
    "LOAN_TOKEN": "CORE_MODEL_LOAN_TOKEN",
    "N_MC": "CORE_MODEL_N_MC",
    "FORECAST_STEP": "CORE_MODEL_FORECAST_STEP",
    "TRAIN_SIZE": "CORE_MODEL_TRAIN_SIZE",
    "COPULA_TYPE": "CORE_MODEL_COPULA_TYPE",
    "SEED": "CORE_MODEL_SEED",
    "LIQ_ANALYSIS": "CORE_MODEL_LIQ_ANALYSIS",
    "JUMPS": "CORE_MODEL_JUMPS",
    "HOURLY_CONV": "CORE_MODEL_HOURLY_CONV",
    "USE_LOG_RETURNS": "CORE_MODEL_USE_LOG_RETURNS",
    "FOCUS_ON_NEGATIVE": "CORE_MODEL_FOCUS_ON_NEGATIVE",
    "WORST_CASE": "CORE_MODEL_WORST_CASE",
    "PERC": "CORE_MODEL_PERC",
    "VOL_FLOOR_PCT": "CORE_MODEL_VOL_FLOOR_PCT",
    "GAS_FEE_USD": "CORE_MODEL_GAS_FEE_USD",
    "SWAP_FEE_USD": "CORE_MODEL_SWAP_FEE_USD",
    "MC_TRIGGER": "CORE_MODEL_MC_TRIGGER",
    "MC_TARGET_LTV": "CORE_MODEL_MC_TARGET_LTV",
    "MC_CURE_PROB": "CORE_MODEL_MC_CURE_PROB",
}


def _coerce(param: str, raw: str) -> object:
    """Coerce a string env var to the type of the corresponding DEFAULTS entry.

    Strict on purpose: the advisory-bounds decision covers out-of-range VALUES,
    not unparseable strings. A typo'd override silently changing a model input
    (JUMPS=ture -> False, MC_TARGET_LTV=0.8x -> None) is a typo, not an
    override, and params is recorded in the results table for auditability.
    """
    default = DEFAULTS.get(param)
    if isinstance(default, bool):
        low = raw.lower()
        if low in ("true", "1", "yes"):
            return True
        if low in ("false", "0", "no"):
            return False
        raise ValueError(f"invalid boolean for {param}: {raw!r}")
    if isinstance(default, int):
        return int(raw)
    if isinstance(default, float):
        return float(raw)
    if default is None:
        # Optional params (MC_TARGET_LTV): only a float override makes sense --
        # None is the default, so there is no reason to set the var to get it.
        try:
            return float(raw)
        except ValueError:
            raise ValueError(f"invalid float for {param}: {raw!r}") from None
    return raw
