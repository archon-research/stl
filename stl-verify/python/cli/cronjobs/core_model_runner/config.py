"""Environment-variable config for the core-model-runner cronjob.

All CORE model params default to config.DEFAULTS, which in turn come from
app/risk_engine/core_model/inputs/default_params.json. Override any of them
via environment variables.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

from app.risk_engine.core_model.config import DEFAULTS

_INPUTS_DEFAULT = Path(__file__).resolve().parents[3] / "app" / "risk_engine" / "core_model" / "inputs"


@dataclass(frozen=True)
class RunnerConfig:
    database_url: str
    market_key: str
    inputs_dir: Path
    params: dict = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "RunnerConfig":
        overrides = {k: _coerce(k, os.environ[env_key]) for k, env_key in _ENV_MAP.items() if env_key in os.environ}
        params = {**DEFAULTS, **overrides}
        return cls(
            database_url=os.environ["DATABASE_URL"],
            market_key=os.environ["CORE_MODEL_MARKET_KEY"],
            inputs_dir=Path(os.environ.get("CORE_MODEL_INPUTS_DIR", str(_INPUTS_DEFAULT))),
            params=params,
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
