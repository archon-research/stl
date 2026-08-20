"""Unit tests for ParquetCoreModelDataReader."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from app.adapters.parquet.core_model_data_reader import ParquetCoreModelDataReader


@pytest.fixture()
def inputs_dir(tmp_path: Path) -> Path:
    """Minimal parquet fixtures for protocol data tests."""
    users = pd.DataFrame(
        {
            "total_borrow_usd": [1000.0],
            "lltv": [0.8],
            "ltv": [0.7],
            "health_factor": [1.2],
            "liquidation_incentive": [1.05],
        }
    )
    market = pd.DataFrame(
        {
            "token_symbol": ["WBTC"],
            "oracle_price": [60000.0],
            "lltv": [0.8],
        }
    )
    prices = pd.DataFrame(
        {"WBTC": [60000.0, 61000.0]},
        index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
    )

    users.to_parquet(tmp_path / "users_sparklend_usdc.parquet")
    market.to_parquet(tmp_path / "market_sparklend_usdc.parquet")
    users.to_parquet(tmp_path / "users_morpho_cbbtc-usdc.parquet")
    market.to_parquet(tmp_path / "market_morpho_cbbtc-usdc.parquet")
    users.to_parquet(tmp_path / "users_galaxy_no-class-a.parquet")
    users.to_parquet(tmp_path / "users_galaxy_w-class-a.parquet")
    market.to_parquet(tmp_path / "market_galaxy.parquet")
    users.to_parquet(tmp_path / "users_anchorage.parquet")
    market.to_parquet(tmp_path / "market_anchorage.parquet")
    prices.to_parquet(tmp_path / "prices_df.parquet")
    return tmp_path


async def test_get_protocol_data_sparklend(inputs_dir: Path):
    reader = ParquetCoreModelDataReader(inputs_dir)
    users_df, market_df = await reader.get_protocol_data(
        protocol="SPARKLEND",
        network="ethereum",
        morpho_market="CBBTC",
        loan_token="USDC",
        galaxy_type="no-class-a",
    )
    assert "total_borrow_usd" in users_df.columns
    assert "token_symbol" in market_df.columns


async def test_get_protocol_data_morpho(inputs_dir: Path):
    reader = ParquetCoreModelDataReader(inputs_dir)
    users_df, market_df = await reader.get_protocol_data(
        protocol="morpho",
        network="ethereum",
        morpho_market="CBBTC",
        loan_token="USDC",
        galaxy_type="no-class-a",
    )
    assert not users_df.empty


async def test_get_protocol_data_galaxy_no_class_a(inputs_dir: Path):
    reader = ParquetCoreModelDataReader(inputs_dir)
    users_df, _ = await reader.get_protocol_data(
        protocol="galaxy",
        network="ethereum",
        morpho_market="CBBTC",
        loan_token="USDC",
        galaxy_type="no-class-a",
    )
    assert not users_df.empty


async def test_get_protocol_data_galaxy_with_class_a(inputs_dir: Path):
    reader = ParquetCoreModelDataReader(inputs_dir)
    users_df, _ = await reader.get_protocol_data(
        protocol="galaxy",
        network="ethereum",
        morpho_market="CBBTC",
        loan_token="USDC",
        galaxy_type="with-class-a",
    )
    assert not users_df.empty


async def test_get_protocol_data_anchorage(inputs_dir: Path):
    reader = ParquetCoreModelDataReader(inputs_dir)
    users_df, _ = await reader.get_protocol_data(
        protocol="anchorage",
        network="ethereum",
        morpho_market="CBBTC",
        loan_token="USDC",
        galaxy_type="no-class-a",
    )
    assert not users_df.empty


async def test_get_prices_filters_columns(inputs_dir: Path):
    reader = ParquetCoreModelDataReader(inputs_dir)
    prices_df = await reader.get_prices(["WBTC"])
    assert list(prices_df.columns) == ["WBTC"]
    assert len(prices_df) == 2


async def test_get_orderbooks_uses_lowercase_filename_for_uppercase_symbol(tmp_path: Path) -> None:
    """Uppercase symbol 'WETH' must resolve to 'weth_sell_orderbook.parquet', not 'WETH_sell_orderbook.parquet'.

    The fix is required for Linux (case-sensitive FS). On macOS the lookup
    happens to succeed either way, so we assert the constructed path directly.
    """
    reader = ParquetCoreModelDataReader(tmp_path)
    seen_paths: list[str] = []

    def fake_read_parquet(path, **kwargs):
        seen_paths.append(str(path))
        return pd.DataFrame({"price": [1.0], "quantity": [100.0]})

    with patch("app.adapters.parquet.core_model_data_reader.pd.read_parquet", side_effect=fake_read_parquet):
        await reader.get_orderbooks(["WETH", "ETH", "WBTC"])

    assert any("weth_sell_orderbook" in p for p in seen_paths), f"Expected weth path, got {seen_paths}"
    assert not any("WETH_sell_orderbook" in p for p in seen_paths), f"Uppercase path found: {seen_paths}"
    assert any("eth_sell_orderbook" in p for p in seen_paths), f"Expected eth path, got {seen_paths}"
    assert any("wbtc_sell_orderbook" in p for p in seen_paths), f"Expected wbtc path, got {seen_paths}"
