"""Unit tests for the live orderbook reader: routing and merge logic."""

from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from app.adapters.postgres.core_model_orderbook_reader import (
    PostgresOrderbookReader,
    book_for,
    merge_asks,
)


@pytest.mark.parametrize(
    ("token", "book"),
    [
        # ETH LSTs ride the ETH book (unscaled, matching BA's parquet set)
        ("WETH", "ETH"),
        ("wstETH", "ETH"),
        ("WEETH", "ETH"),
        ("RETH", "ETH"),
        ("RSETH", "ETH"),
        ("EZETH", "ETH"),
        ("STETH", "ETH"),
        # BTC wrappers ride the BTC book
        ("WBTC", "BTC"),
        ("LBTC", "BTC"),
        ("TBTC", "BTC"),
        ("cbBTC", "BTC"),
        ("BTC", "BTC"),
        # everything else maps to its own symbol
        ("XRP", "XRP"),
        ("HYPE", "HYPE"),
    ],
)
def test_routing_matches_the_model_readme_table(token, book):
    assert book_for(token) == book


def test_merge_sorts_across_venues_by_price():
    df = merge_asks(
        [
            [["100.0", "1.0"], ["102.0", "2.0"]],  # venue A
            [["101.0", "3.0"]],  # venue B
        ]
    )
    assert list(df["price"]) == [100.0, 101.0, 102.0]


def test_merge_keeps_same_price_levels_from_different_venues():
    df = merge_asks([[["100.0", "1.0"]], [["100.0", "2.0"]]])
    assert list(df["sz"]) == [1.0, 2.0]  # depth adds, rows stay separate


def test_merge_computes_liquidity_as_price_times_size():
    df = merge_asks([[["200.0", "0.5"]]])
    assert list(df["liquidity"]) == [100.0]


def test_merge_drops_zero_and_negative_levels():
    df = merge_asks([[["100.0", "0"], ["0", "5.0"], ["101.0", "1.0"]]])
    assert list(df["price"]) == [101.0]


def test_merge_rejects_a_book_with_no_usable_levels():
    with pytest.raises(ValueError, match="no ask levels"):
        merge_asks([[["100.0", "0"]]])


async def test_untracked_book_fails_loudly_and_points_at_data_gaps():
    # The engine is never touched on this path, so a null stands in for it.
    reader = PostgresOrderbookReader(engine=cast(AsyncEngine, None))
    with pytest.raises(ValueError, match="DATA_GAPS"):
        await reader.get_orderbooks(["XRP"])
