"""Environment-variable config for the core-model-runner cronjob.

Params are resolved in three layers (lowest wins):
  1. default_params.json          -- canonical defaults for all markets
  2. market_configs.json[key]     -- market-specific required overrides
  3. CORE_MODEL_* env vars        -- runtime overrides (e.g. N_MC=100 for a quick test)

Only DATABASE_URL and CORE_MODEL_MARKET_KEY are required env vars. All other
params come from the config files and can be selectively overridden via env.
"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from app.risk_engine.core_model.config import DEFAULTS

_INPUTS_DEFAULT = Path(__file__).resolve().parents[3] / "app" / "risk_engine" / "core_model" / "inputs"
_MARKET_CONFIGS_DEFAULT = _INPUTS_DEFAULT / "market_configs.json"


def _load_market_configs(path: Path) -> dict[str, dict]:
    with open(path) as f:
        data = json.load(f)
    return {k: v for k, v in data.items() if not k.startswith("_")}


_DATA_SOURCES = ("parquet", "postgres")

# Source keys resolve per market: default -> market_configs.json entry -> env
# var (global override, e.g. forcing parquet on a cluster with no indexed
# data). Per-market matters because coverage is per market: SparkLend can run
# on live tables while markets whose readers or feeds do not exist yet stay on
# parquet, without the daily "all" tick failing on them.
_SOURCE_KEYS = {
    "ORDERBOOK_SOURCE": "CORE_MODEL_ORDERBOOK_SOURCE",
    "PRICE_SOURCE": "CORE_MODEL_PRICE_SOURCE",
    "POSITION_SOURCE": "CORE_MODEL_POSITION_SOURCE",
}


def _resolve_source(key: str, market_config: dict) -> str:
    source = os.environ.get(_SOURCE_KEYS[key], market_config.get(key, "parquet"))
    if source not in _DATA_SOURCES:
        raise ValueError(f"invalid {key} {source!r}; allowed: {list(_DATA_SOURCES)}")
    return source


@dataclass(frozen=True)
class RunnerConfig:
    database_url: str
    market_key: str
    inputs_dir: Path
    params: dict = field(default_factory=dict)
    # Which source serves get_orderbooks / get_prices: "parquet" (static
    # snapshots) or "postgres" (live tables). Positions stay on parquet until
    # their adapter lands — see DATA_GAPS.md.
    orderbook_source: str = "parquet"
    price_source: str = "parquet"
    position_source: str = "parquet"

    @classmethod
    def from_env(
        cls,
        *,
        market_configs_path: Path = _MARKET_CONFIGS_DEFAULT,
    ) -> "RunnerConfig":
        market_key = os.environ["CORE_MODEL_MARKET_KEY"]
        market_configs = _load_market_configs(market_configs_path)
        if market_key not in market_configs:
            available = sorted(market_configs)
            raise ValueError(f"unknown market_key {market_key!r}; available markets: {available}")
        return cls._build(market_key, market_configs, market_configs_path)

    @classmethod
    def all_from_env(
        cls,
        *,
        market_configs_path: Path = _MARKET_CONFIGS_DEFAULT,
    ) -> "list[RunnerConfig]":
        """Return one RunnerConfig per market defined in market_configs.json.

        Env var overrides (e.g. CORE_MODEL_N_MC=100) are applied to every market,
        making it easy to run all markets with a shared override for quick testing.
        """
        market_configs = _load_market_configs(market_configs_path)
        return [cls._build(key, market_configs, market_configs_path) for key in market_configs]

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
            return [cls._build(key, market_configs, market_configs_path) for key in market_configs]
        if market_key not in market_configs:
            available = sorted(market_configs)
            raise ValueError(f"unknown market_key {market_key!r}; available markets: {available}")
        return [cls._build(market_key, market_configs, market_configs_path)]

    @classmethod
    def _build(
        cls,
        market_key: str,
        market_configs: dict[str, dict],
        market_configs_path: Path,
    ) -> "RunnerConfig":
        # Layer 1: defaults
        params = dict(DEFAULTS)
        # Layer 2: market-specific overrides from config file
        params.update(market_configs[market_key])
        # Layer 3: env var overrides
        env_overrides = {k: _coerce(k, os.environ[env_key]) for k, env_key in _ENV_MAP.items() if env_key in os.environ}
        params.update(env_overrides)

        market_config = market_configs[market_key]
        orderbook_source = _resolve_source("ORDERBOOK_SOURCE", market_config)
        price_source = _resolve_source("PRICE_SOURCE", market_config)
        position_source = _resolve_source("POSITION_SOURCE", market_config)
        # Recorded in params so every core_model_results row says which
        # sources produced it — live-data and parquet CRRs must never be
        # indistinguishable in the audit trail.
        params["ORDERBOOK_SOURCE"] = orderbook_source
        params["PRICE_SOURCE"] = price_source
        params["POSITION_SOURCE"] = position_source

        return cls(
            database_url=os.environ["DATABASE_URL"],
            market_key=market_key,
            inputs_dir=Path(os.environ.get("CORE_MODEL_INPUTS_DIR", str(_INPUTS_DEFAULT))),
            params=params,
            orderbook_source=orderbook_source,
            price_source=price_source,
            position_source=position_source,
        )


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
    """Coerce a string env var to the type of the corresponding DEFAULTS entry."""
    default = DEFAULTS.get(param)
    if isinstance(default, bool):
        return raw.lower() in ("true", "1", "yes")
    if isinstance(default, int):
        return int(raw)
    if isinstance(default, float):
        return float(raw)
    if default is None:
        # Optional params (MC_TARGET_LTV) -- try float, fall back to None
        try:
            return float(raw)
        except ValueError:
            return None
    return raw
