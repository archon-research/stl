"""
config.py — Default parameter loader for CORE (Collateralized Onchain Risk Engine).

Single source of truth for all model parameters.  Import DEFAULTS for quick
access to flat key→value pairs, or call load_params() to start from defaults
and layer custom overrides on top.

Typical usage
-------------
# 1. Access a single default
from config import DEFAULTS
FORECAST_STEP = DEFAULTS["FORECAST_STEP"]   # 14

# 2. Build a full params dict, overriding a few values
from config import load_params
params = load_params(overrides={"PROTOCOL": "AAVE", "N_MC": 5000})

# 3. Load a custom JSON file (flat {key: value}) and merge with defaults
params = load_params(path="my_run.json")

# 4. Inspect valid choices or numeric bounds from the schema
from config import SCHEMA
SCHEMA["COPULA_TYPE"]["choices"]      # ["T-COPULA", "GAUSSIAN"]
SCHEMA["FORECAST_STEP"]["min"]        # 1
"""

import json
import os
from typing import Any

# Canonical defaults file — lives next to the other input files
_DEFAULTS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "inputs", "default_params.json",
)


def _load_schema(path: str = _DEFAULTS_PATH) -> dict:
    """Return the raw schema dict (each entry has 'value', 'description', …)."""
    with open(path, "r") as f:
        raw = json.load(f)
    # Drop the documentation-only _comment key if present
    return {k: v for k, v in raw.items() if not k.startswith("_")}


def _flatten(schema: dict) -> dict[str, Any]:
    """Extract the 'value' field from each schema entry → flat {param: value}."""
    return {k: v["value"] for k, v in schema.items()}


# ── Module-level singletons ────────────────────────────────────────────────────
# Import these directly:
#   from config import DEFAULTS, SCHEMA

SCHEMA: dict = _load_schema()
"""Full schema dict.  Keys are param names; values are dicts with at least
'value' and 'description', plus optional 'choices', 'type', 'min', 'max'."""

DEFAULTS: dict[str, Any] = _flatten(SCHEMA)
"""Flat {param_name: default_value} dict — the primary import target."""


# ── Helper ─────────────────────────────────────────────────────────────────────

def load_params(
    path: str | None = None,
    overrides: dict | None = None,
) -> dict[str, Any]:
    """
    Build a complete parameter dict, starting from DEFAULTS.

    Parameters
    ----------
    path
        Optional path to a JSON file.  Accepted formats:

        * **Flat**   ``{"FORECAST_STEP": 7, "N_MC": 5000}``
        * **Schema** ``{"FORECAST_STEP": {"value": 7, ...}, ...}``

        Keys that are not recognised CORE parameters are silently ignored.
    overrides
        Optional dict of ``{param: value}`` applied *after* the JSON file.
        Unknown keys are silently ignored.

    Returns
    -------
    dict
        All CORE parameters at their resolved values.  Unknown keys from
        *path* or *overrides* are dropped so callers get a clean dict.
    """
    params: dict[str, Any] = dict(DEFAULTS)  # start from canonical defaults

    if path is not None:
        with open(path, "r") as f:
            custom: dict = json.load(f)
        for k, v in custom.items():
            if k in params:
                # Accept both flat value and schema-style {"value": ...}
                params[k] = v["value"] if (isinstance(v, dict) and "value" in v) else v

    if overrides:
        for k, v in overrides.items():
            if k in params:
                params[k] = v

    return params
