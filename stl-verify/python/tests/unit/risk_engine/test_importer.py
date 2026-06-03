"""Tests for importer.load_orderbook_data filename normalisation."""

import os
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from app.risk_engine.core_model import importer


def _dummy_df() -> pd.DataFrame:
    return pd.DataFrame({"price": [1.0], "quantity": [100.0]})


def test_load_orderbook_data_uses_lowercase_filename_for_uppercase_symbol() -> None:
    """Uppercase symbol 'WETH' must resolve to 'weth_sell_orderbook.parquet', not 'WETH_sell_orderbook.parquet'.

    The fix is required for Linux (case-sensitive FS). On macOS the lookup
    happens to succeed either way, so we assert the constructed path directly.
    """
    seen_paths: list[str] = []

    def fake_read_parquet(path, **kwargs):
        seen_paths.append(str(path))
        return _dummy_df()

    with patch("app.risk_engine.core_model.importer.pd.read_parquet", side_effect=fake_read_parquet):
        importer.load_orderbook_data(["WETH", "ETH", "WBTC"])

    assert any("weth_sell_orderbook" in p for p in seen_paths), f"Expected weth path, got {seen_paths}"
    assert not any("WETH_sell_orderbook" in p for p in seen_paths), f"Uppercase path found: {seen_paths}"
    assert any("eth_sell_orderbook" in p for p in seen_paths), f"Expected eth path, got {seen_paths}"
    assert any("wbtc_sell_orderbook" in p for p in seen_paths), f"Expected wbtc path, got {seen_paths}"
