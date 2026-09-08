"""Unit tests for the Liquidator's cumulative order-book slippage.

The book is a bid ladder, best price first, with ``liquidity`` in USD. A
liquidation consumes the slice between ``already_consumed`` and
``already_consumed + amount``; the slippage is the USD-weighted price impact
of that slice plus the share of the amount the book could not absorb.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from app.risk_engine.core_model.config import INPUTS_DIR
from app.risk_engine.core_model.liquidator import Liquidator

_SIM_PRICE = 100.0


def _book(*levels: tuple[float, float]) -> pd.DataFrame:
    """Bid ladder from ``(price, liquidity_usd)`` levels, in the given order."""
    return pd.DataFrame(levels, columns=["price", "liquidity"])


# Four ticks worth 100, 200, 300, 400 USD; cumulative boundaries at 100, 300, 600, 1000.
_FOUR_TICKS = _book((100.0, 100.0), (99.0, 200.0), (98.0, 300.0), (97.0, 400.0))


def _walk_book(book: pd.DataFrame, sim_price: float, amount: float, already_consumed: float) -> float:
    """Reference slippage: walk the ticks one by one, taking what each has left."""
    prices = book["price"].to_numpy()
    liquidity = book["liquidity"].to_numpy()
    keep = prices <= sim_price
    prices, liquidity = prices[keep], liquidity[keep]
    if prices.size == 0:
        return 1.0
    if amount == 0.0:
        return 0.0
    start, end = already_consumed, already_consumed + amount
    value = filled = 0.0
    tick_lo = 0.0
    for price, liq in zip(prices, liquidity, strict=True):
        tick_hi = tick_lo + liq
        take_lo, take_hi = max(start, tick_lo), min(end, tick_hi)
        if take_hi > take_lo:
            value += (take_hi - take_lo) * price
            filled += take_hi - take_lo
        tick_lo = tick_hi
    price_impact = (sim_price - value / filled) / sim_price if filled > 0 else 0.0
    # Measured from the book total, not from the walked ``filled``, whose rounding noise
    # would otherwise dominate the share for a sub-cent amount.
    unfilled = max(amount - min(amount, max(tick_lo - start, 0.0)), 0.0) / amount
    return min(price_impact + unfilled, 0.9999)


def _slippage(book: pd.DataFrame, amounts: list[float], *, already_consumed: float = 0.0) -> np.ndarray:
    return Liquidator.slippage_calculator_cum(book, np.array(amounts, dtype=np.float64), _SIM_PRICE, already_consumed)


@pytest.mark.parametrize(
    ("already_consumed", "amount", "expected"),
    [
        pytest.param(0.0, 50.0, 0.0, id="first-tick-at-the-simulation-price"),
        pytest.param(350.0, 100.0, 0.02, id="third-tick-priced-98"),
    ],
)
def test_fill_contained_in_one_tick_pays_that_tick_price(already_consumed: float, amount: float, expected: float):
    assert _slippage(_FOUR_TICKS, [amount], already_consumed=already_consumed) == pytest.approx([expected], abs=1e-12)


def test_amount_straddling_a_tick_boundary_averages_the_partial_ticks():
    # 60 USD from position 70: 30 at 100 and 30 at 99 -> average 99.5.
    assert _slippage(_FOUR_TICKS, [60.0], already_consumed=70.0) == pytest.approx([0.005], abs=1e-12)


def test_amount_spanning_full_middle_ticks_counts_them_once():
    # From 50 to 650: 50 at 100, 200 at 99, 300 at 98, 50 at 97 -> 59050 / 600.
    expected = (100.0 - 59_050.0 / 600.0) / 100.0
    assert _slippage(_FOUR_TICKS, [600.0], already_consumed=50.0) == pytest.approx([expected], abs=1e-12)


def test_amount_ending_exactly_on_a_tick_boundary_does_not_touch_the_next_tick():
    # 300 USD from 0 ends exactly at the second boundary: 100 at 100, 200 at 99.
    expected = (100.0 - 29_800.0 / 300.0) / 100.0
    assert _slippage(_FOUR_TICKS, [300.0]) == pytest.approx([expected], abs=1e-12)


def test_amount_starting_exactly_on_a_tick_boundary_starts_in_the_next_tick():
    # From position 300 (end of tick two) 100 USD is filled entirely at 98.
    assert _slippage(_FOUR_TICKS, [100.0], already_consumed=300.0) == pytest.approx([0.02], abs=1e-12)


def test_amount_between_two_boundaries_takes_exactly_the_whole_ticks_between():
    # From 100 to 600: 200 at 99 and 300 at 98 -> 49200 / 500.
    expected = (100.0 - 49_200.0 / 500.0) / 100.0
    assert _slippage(_FOUR_TICKS, [500.0], already_consumed=100.0) == pytest.approx([expected], abs=1e-12)


def test_zero_liquidity_tick_is_skipped():
    book = _book((100.0, 100.0), (99.5, 0.0), (99.0, 200.0))
    # From 100 exactly: the empty 99.5 level holds nothing, the fill is all at 99.
    assert _slippage(book, [50.0], already_consumed=100.0) == pytest.approx([0.01], abs=1e-12)


def test_overflow_adds_the_unfilled_share():
    # Book holds 1000; 1200 from 0 fills 1000 at the book average (98,000 / 1000) and 200 go unfilled.
    expected = (100.0 - 98.0) / 100.0 + 200.0 / 1200.0
    assert _slippage(_FOUR_TICKS, [1200.0]) == pytest.approx([expected], abs=1e-12)


@pytest.mark.parametrize("already_consumed", [1000.0, 5000.0], ids=["exhausted", "over-consumed"])
def test_book_with_nothing_left_gives_the_capped_slippage(already_consumed: float):
    assert _slippage(_FOUR_TICKS, [10.0], already_consumed=already_consumed) == pytest.approx([0.9999])


def test_zero_amount_has_zero_slippage_even_on_an_exhausted_book():
    assert _slippage(_FOUR_TICKS, [0.0], already_consumed=1000.0) == pytest.approx([0.0])


def test_no_level_at_or_below_the_simulation_price_means_no_fill():
    book = _book((101.0, 100.0), (100.5, 100.0))
    assert _slippage(book, [10.0]).tolist() == [1.0]


def test_zero_amount_on_a_book_with_no_usable_level_keeps_the_no_fill_result():
    # Upstream order of checks: the empty-book return runs before the zero-amount override.
    book = _book((101.0, 100.0), (100.5, 100.0))
    assert _slippage(book, [0.0]).tolist() == [1.0]


def test_levels_above_the_simulation_price_are_not_sold_into():
    book = _book((101.0, 100.0), (100.0, 100.0), (99.0, 100.0))
    assert _slippage(book, [100.0]) == pytest.approx([0.0], abs=1e-12)


def test_already_consumed_is_an_offset_into_the_masked_book():
    book = _book((101.0, 100.0), (100.0, 100.0), (99.0, 100.0))
    # The 101 level is dropped, so 100 consumed means the 100 level is gone and the fill is at 99.
    assert _slippage(book, [50.0], already_consumed=100.0) == pytest.approx([0.01], abs=1e-12)


@pytest.mark.parametrize(
    "book",
    [
        pytest.param(_book((100.0, 100.0), (99.0, float("nan")), (98.0, 100.0)), id="nan-liquidity"),
        pytest.param(_book((100.0, 100.0), (float("nan"), 50.0)), id="nan-price"),
        pytest.param(_book((100.0, 100.0), (99.0, -50.0), (98.0, 100.0)), id="negative-liquidity"),
    ],
)
def test_corrupt_book_level_raises_instead_of_pricing_it(book: pd.DataFrame):
    with pytest.raises(ValueError, match="non-finite or negative"):
        _slippage(book, [50.0])


def test_non_finite_already_consumed_raises():
    with pytest.raises(ValueError, match="already_consumed"):
        _slippage(_FOUR_TICKS, [50.0], already_consumed=float("nan"))


def test_amounts_are_evaluated_independently_from_the_same_offset():
    amounts = [50.0, 100.0, 600.0, 1200.0, 0.0]
    expected = [_walk_book(_FOUR_TICKS, _SIM_PRICE, a, 70.0) for a in amounts]
    assert _slippage(_FOUR_TICKS, amounts, already_consumed=70.0) == pytest.approx(expected, abs=1e-12)


@pytest.mark.parametrize("seed", range(5))
def test_matches_a_tick_by_tick_walk_on_a_random_book(seed: int):
    rng = np.random.default_rng(seed)
    prices = np.sort(rng.uniform(80.0, 100.0, size=40))[::-1]
    liquidity = rng.uniform(0.0, 500.0, size=40)
    liquidity[rng.integers(0, 40, size=3)] = 0.0
    book = _book(*zip(prices, liquidity, strict=True))
    cum_liq = np.cumsum(liquidity)
    total = float(cum_liq[-1])
    already_consumed = float(rng.uniform(0.0, total / 2))
    remaining = total - already_consumed
    boundaries = [float(c - already_consumed) for c in cum_liq if c > already_consumed][:6]
    amounts = np.concatenate(
        [rng.uniform(0.0, remaining, size=48), boundaries, [0.0, 1e-6, remaining, remaining + 1.0, total * 2]]
    )

    expected = [_walk_book(book, _SIM_PRICE, float(a), already_consumed) for a in amounts]
    assert _slippage(book, list(amounts), already_consumed=already_consumed) == pytest.approx(expected, abs=1e-9)


def test_sequential_consumption_pays_the_same_as_one_liquidation():
    first = _slippage(_FOUR_TICKS, [250.0])[0]
    second = _slippage(_FOUR_TICKS, [250.0], already_consumed=250.0)[0]
    whole = _slippage(_FOUR_TICKS, [500.0])[0]
    assert first * 250.0 + second * 250.0 == pytest.approx(whole * 500.0, abs=1e-9)


def test_from_a_fresh_book_never_exceeds_the_non_cumulative_calculator():
    # slippage_calculator rounds the fill up to the end of the last tick it enters,
    # so it is an upper bound that the exact partial-tick fill meets only at boundaries.
    amounts = np.array([100.0, 150.0, 300.0, 450.0, 600.0, 1000.0])
    cumulative = _slippage(_FOUR_TICKS, list(amounts))
    plain = Liquidator.slippage_calculator(_FOUR_TICKS, amounts, _SIM_PRICE)
    assert np.all(cumulative <= plain + 1e-12)
    on_boundary = np.isin(amounts, [100.0, 300.0, 600.0, 1000.0])
    assert cumulative[on_boundary] == pytest.approx(plain[on_boundary], abs=1e-12)
    assert np.all(cumulative[~on_boundary] < plain[~on_boundary])


def test_price_impact_stays_exact_deep_into_a_large_book():
    # Non-round liquidity so the cumulative sums are inexact floats: differencing them for a
    # 1 USD fill after 4e8 USD consumed would lose the digits, pricing the fill directly does not.
    rng = np.random.default_rng(7)
    liquidity = rng.uniform(49_000.0, 51_000.0, size=10_000)
    prices = 70_000.0 - np.arange(10_000) * 0.01
    book = _book(*zip(prices, liquidity, strict=True))
    cum_liq = np.cumsum(liquidity)
    tick = 8_000
    already_consumed = float(cum_liq[tick - 1] + liquidity[tick] / 2)  # strictly inside tick 8000
    expected = (70_000.0 - prices[tick]) / 70_000.0
    slippage = Liquidator.slippage_calculator_cum(book, np.array([1.0]), 70_000.0, already_consumed)
    assert slippage == pytest.approx([expected], rel=1e-9)


def test_real_btc_book_prices_small_fills_at_the_top_of_the_book():
    # The parquet BTC bid ladder: first tick 663.79 USD, 34,002 levels, 4.6e8 USD in total.
    book = pd.read_parquet(Path(INPUTS_DIR) / "btc_sell_orderbook.parquet")
    best_bid = float(book["price"].iloc[0])
    small = np.array([1.0, 100.0, 486.0, 663.0])

    fresh = Liquidator.slippage_calculator_cum(book, small, best_bid, 0.0)
    after_a_million = Liquidator.slippage_calculator_cum(book, np.array([1.0, 100.0, 10_000.0]), best_bid, 1e6)

    assert np.all(fresh < 1e-6)
    assert np.all((after_a_million > 0) & (after_a_million < 1e-3))
