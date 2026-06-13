"""Tests for ParquetCoreModelDataReader.get_orderbooks filename normalisation."""

from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from app.adapters.parquet.core_model_data_reader import ParquetCoreModelDataReader


def _dummy_df() -> pd.DataFrame:
    return pd.DataFrame({"price": [1.0], "quantity": [100.0]})


@pytest.fixture
def reader(tmp_path):
    return ParquetCoreModelDataReader(tmp_path)


async def test_get_orderbooks_uses_lowercase_filename_for_uppercase_symbol(reader) -> None:
    """Uppercase symbol 'WETH' must resolve to 'weth_sell_orderbook.parquet', not 'WETH_sell_orderbook.parquet'.

    The fix is required for Linux (case-sensitive FS). On macOS the lookup
    happens to succeed either way, so we assert the constructed path directly.
    """
    seen_paths: list[str] = []

    def fake_read_parquet(path, **kwargs):
        seen_paths.append(str(path))
        return _dummy_df()

    with patch("app.adapters.parquet.core_model_data_reader.pd.read_parquet", side_effect=fake_read_parquet):
        await reader.get_orderbooks(["WETH", "ETH", "WBTC"])

    assert any("weth_sell_orderbook" in p for p in seen_paths), f"Expected weth path, got {seen_paths}"
    assert not any("WETH_sell_orderbook" in p for p in seen_paths), f"Uppercase path found: {seen_paths}"
    assert any("eth_sell_orderbook" in p for p in seen_paths), f"Expected eth path, got {seen_paths}"
    assert any("wbtc_sell_orderbook" in p for p in seen_paths), f"Expected wbtc path, got {seen_paths}"
