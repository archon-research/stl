"""CoreModelDataReader port — input data for the CORE pipeline.

Implementations provide protocol position data, price history, and
sell-side order book depth for each collateral token.
"""

from typing import Protocol

import pandas as pd


class CoreModelDataReader(Protocol):
    async def get_protocol_data(
        self,
        protocol: str,
        network: str,
        morpho_market: str,
        loan_token: str,
        galaxy_type: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Return (users_df, market_df) for the given protocol configuration."""
        ...

    async def get_prices(self, collateral_list: list[str]) -> pd.DataFrame:
        """Return a DataFrame of daily close prices indexed by date.

        Columns are token symbols matching ``collateral_list``.
        Must cover at least TRAIN_SIZE days of history.
        """
        ...

    async def get_orderbooks(self, collateral_list: list[str]) -> dict[str, pd.DataFrame]:
        """Return sell-side order book DataFrames keyed by token symbol."""
        ...
