"""Unit tests for CORE parameter type validation (VEC-768 / audit T-01).

load_params is the single funnel for defaults, market_configs.json, env
overrides (already coerced) and hand-passed params — a wrongly-typed value
must fail there instead of reaching simulation.
"""

import pytest

from app.risk_engine.core_model.config import load_params


def test_defaults_pass_validation():
    params = load_params()
    assert params["N_MC"] > 0


def test_string_false_for_a_bool_param_raises():
    # The audit's exact example: a non-empty string is truthy, so upstream
    # would have activated the WORST_CASE branch.
    with pytest.raises(ValueError, match="WORST_CASE.*expected bool.*'false'"):
        load_params(overrides={"WORST_CASE": "false"})


@pytest.mark.parametrize(
    ("param", "bad_value", "expected_kind"),
    [
        ("N_MC", "1000", "int"),
        ("N_MC", True, "int"),
        ("PERC", "0.975", "float"),
        ("JUMPS", 1, "bool"),
        ("PROTOCOL", 3, "str"),
        ("MC_TARGET_LTV", "0.7", "float | None"),
    ],
)
def test_wrong_type_raises(param, bad_value, expected_kind):
    with pytest.raises(ValueError, match=f"{param}.*expected {expected_kind.split(' ')[0]}"):
        load_params(overrides={param: bad_value})


@pytest.mark.parametrize(
    ("param", "value"),
    [
        ("MIN_BORROW_USD", 100),  # int for a float param is fine
        ("MC_TARGET_LTV", None),  # nullable float accepts null
        ("MC_TARGET_LTV", 0.7),
        ("N_MC", 50),
    ],
)
def test_compatible_values_pass(param, value):
    assert load_params(overrides={param: value})[param] == value


def test_error_names_every_offending_param():
    with pytest.raises(ValueError) as exc:
        load_params(overrides={"WORST_CASE": "false", "N_MC": "1000"})
    assert "WORST_CASE" in str(exc.value)
    assert "N_MC" in str(exc.value)
