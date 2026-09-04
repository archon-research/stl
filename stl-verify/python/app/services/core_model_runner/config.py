"""Environment-variable config for the core-model-runner cronjob.

Params are resolved in three layers (lowest wins):
  1. default_params.json          -- canonical defaults for all markets
  2. market_configs.json[key]     -- market-specific required overrides
  3. CORE_MODEL_* env vars        -- runtime overrides (e.g. N_MC=100 for a quick test)

The entry point (cli/cronjobs/core_model_runner) owns DATABASE_URL and the
market-key selection; this module only resolves model params.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

from app.risk_engine.core_model.config import DEFAULTS, INPUTS_DIR, load_commented_json, load_params

_MARKET_CONFIGS_DEFAULT = Path(INPUTS_DIR) / "market_configs.json"


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
    market_key: str
    params: dict = field(default_factory=dict)
    # Which source serves each input: "parquet" (static snapshots) or
    # "postgres" (live tables). Per market — see DATA_GAPS.md for which
    # markets still lack a live source.
    orderbook_source: str = "parquet"
    price_source: str = "parquet"
    position_source: str = "parquet"

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
        market_configs = load_commented_json(market_configs_path)
        if market_key == "all":
            return [cls._build(key, market_configs) for key in market_configs]
        if market_key not in market_configs:
            available = sorted(market_configs)
            raise ValueError(f"unknown market_key {market_key!r}; available markets: {available}")
        return [cls._build(market_key, market_configs)]

    @classmethod
    def _build(cls, market_key: str, market_configs: dict[str, dict]) -> "RunnerConfig":
        # Every param in default_params.json is overridable as CORE_MODEL_<name>;
        # derived from DEFAULTS so a new param gets its env override for free.
        env_overrides = {
            k: _coerce(k, os.environ[f"CORE_MODEL_{k}"]) for k in DEFAULTS if f"CORE_MODEL_{k}" in os.environ
        }
        market_config = market_configs[market_key]
        # load_params layers defaults -> overrides AND drops unknown keys, so a
        # stray key in market_configs.json cannot leak into the audit trail
        # (params is recorded verbatim in the results table).
        params = load_params(overrides={**market_config, **env_overrides})

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
            market_key=market_key,
            params=params,
            orderbook_source=orderbook_source,
            price_source=price_source,
            position_source=position_source,
        )


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
