"""Composite CoreModelDataReader: swap input sources one at a time.

The parquet-to-live migration happens per data layer (order books first, then
prices, then positions). This wrapper keeps the pipeline on one port while
individual methods move to live adapters.
"""

import pandas as pd

from app.adapters.parquet.core_model_data_reader import ParquetCoreModelDataReader
from app.adapters.postgres.core_model_orderbook_reader import PostgresOrderbookReader


class CompositeCoreModelDataReader:
    """Positions and prices from parquet; order books from live snapshots."""

    def __init__(self, parquet: ParquetCoreModelDataReader, orderbooks: PostgresOrderbookReader) -> None:
        self._parquet = parquet
        self._orderbooks = orderbooks

    async def get_protocol_data(
        self,
        protocol: str,
        network: str,
        morpho_market: str,
        loan_token: str,
        galaxy_type: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        return await self._parquet.get_protocol_data(protocol, network, morpho_market, loan_token, galaxy_type)

    async def get_prices(self, collateral_list: list[str]) -> pd.DataFrame:
        return await self._parquet.get_prices(collateral_list)

    async def get_orderbooks(self, collateral_list: list[str]) -> dict[str, pd.DataFrame]:
        return await self._orderbooks.get_orderbooks(collateral_list)
