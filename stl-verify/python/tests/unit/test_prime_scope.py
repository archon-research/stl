"""The additive-versus-shared rule, which is the whole point of VEC-722."""

from decimal import Decimal

import pytest

from app.domain.prime_scope import QUANTITIES, Quantity, fold, kind


def test_kind_reports_the_declared_kind():
    assert kind("exposure_usd") is Quantity.ADDITIVE
    assert kind("total_risk_capital_usd") is Quantity.SHARED


def test_kind_names_an_undeclared_quantity_rather_than_guessing():
    with pytest.raises(KeyError, match="cash_sweep_usd"):
        kind("cash_sweep_usd")


def test_every_declared_quantity_has_a_kind():
    """The table is the module's public surface; a None slips through review."""
    assert all(isinstance(value, Quantity) for value in QUANTITIES.values())


def test_additive_fold_sums_across_the_primes_wallets():
    assert fold("exposure_usd", [Decimal("1"), Decimal("2"), Decimal("4")]) == Decimal("7")


def test_additive_fold_skips_an_unobserved_wallet_rather_than_zeroing_it():
    assert fold("exposure_usd", [Decimal("5"), None]) == Decimal("5")


def test_additive_fold_of_nothing_observed_is_null_not_zero():
    """A `"0"` would assert the prime holds nothing where the truth is that
    nothing was indexed — the understatement the unserved-chain rule exists for."""
    assert fold("exposure_usd", [None, None]) is None
    assert fold("exposure_usd", []) is None


def test_shared_fold_counts_one_agreeing_figure_once():
    treasury = Decimal("36359440.25")

    assert fold("total_risk_capital_usd", [treasury, treasury, treasury]) == treasury


def test_shared_fold_reads_an_unobserved_wallet_as_silent_not_as_disagreement():
    assert fold("total_risk_capital_usd", [None, Decimal("10"), None]) == Decimal("10")


def test_shared_fold_raises_when_wallets_disagree():
    """A shared quantity fanned out per wallet is the 7x error this prevents;
    it fails in CI rather than serving a plausible number."""
    with pytest.raises(ValueError, match="total_risk_capital_usd"):
        fold("total_risk_capital_usd", [Decimal("10"), Decimal("11")])


def test_folding_an_undeclared_quantity_raises():
    with pytest.raises(KeyError, match="cash_sweep_usd"):
        fold("cash_sweep_usd", [Decimal("1")])
