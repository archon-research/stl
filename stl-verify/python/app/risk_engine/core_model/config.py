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

# The packaged input files (parquet snapshots, param/market configs). Exported
# so callers resolve it here instead of hand-walking parents from their own
# file depth.
INPUTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "inputs")

# Canonical defaults file — lives next to the other input files
_DEFAULTS_PATH = os.path.join(INPUTS_DIR, "default_params.json")


def load_commented_json(path) -> dict:
    """Load a JSON object, dropping documentation-only keys (leading underscore).

    Both input files lean on this convention: default_params.json carries a
    _comment entry, market_configs.json carries _comment and _galaxy_disabled.
    """
    with open(path, "r") as f:
        raw = json.load(f)
    return {k: v for k, v in raw.items() if not k.startswith("_")}


def _load_schema(path: str = _DEFAULTS_PATH) -> dict:
    """Return the raw schema dict (each entry has 'value', 'description', …)."""
    return load_commented_json(path)


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


# The closed set of value kinds a param may declare (schema 'type') or infer
# (the default's Python type). _build_expected_kinds rejects anything else at
# import time, so a schema typo cannot silently reroute validation.
_KNOWN_KINDS = frozenset({"bool", "int", "float", "str", "float | None"})


def _build_expected_kinds(schema: dict, defaults: dict[str, Any]) -> dict[str, str]:
    kinds: dict[str, str] = {}
    for name in defaults:
        kind = schema[name].get("type") or type(defaults[name]).__name__
        if kind not in _KNOWN_KINDS:
            raise ValueError(f"parameter {name!r} has unknown kind {kind!r}; known kinds: {sorted(_KNOWN_KINDS)}")
        kinds[name] = kind
    return kinds


EXPECTED_KINDS: dict[str, str] = _build_expected_kinds(SCHEMA, DEFAULTS)
"""Per-param value kind — the single source for load_params validation and the
runner's env-var coercion (_coerce)."""


def _value_matches(kind: str, value: Any) -> bool:
    # bool is a subclass of int, so numeric kinds must exclude it explicitly.
    if kind == "bool":
        return isinstance(value, bool)
    if kind == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if kind == "float":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if kind == "float | None":
        return value is None or (isinstance(value, (int, float)) and not isinstance(value, bool))
    if kind == "str":
        return isinstance(value, str)
    raise ValueError(f"unknown parameter kind {kind!r}")


def _validate_types(params: dict[str, Any]) -> None:
    """Reject values of the wrong type instead of letting them reach simulation.

    A JSON or dict override like '"WORST_CASE": "false"' is a non-empty string
    and would activate the truthy branch in the runner (audit T-01). Only the
    types are enforced: the schema's min/max/choices stay advisory, matching
    upstream's convention (documented in default_params.json).
    """
    bad = {
        k: f"expected {EXPECTED_KINDS[k]}, got {type(v).__name__} {v!r}"
        for k, v in params.items()
        if not _value_matches(EXPECTED_KINDS[k], v)
    }
    if bad:
        raise ValueError(f"invalid CORE parameter types: {bad}")


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

    _validate_types(params)
    return params
