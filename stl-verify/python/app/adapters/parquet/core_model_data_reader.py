"""Parquet-backed implementation of CoreModelDataReader.

Reads static parquet snapshots from ``inputs_dir``. Mirrors the file
naming conventions of the original ``importer.py`` so the snapshots from
``core_model_copy/inputs/`` work without transformation.
"""

from pathlib import Path

import pandas as pd


class ParquetCoreModelDataReader:
    def __init__(self, inputs_dir: Path) -> None:
        self._inputs_dir = inputs_dir

    def _path(self, filename: str) -> Path:
        return self._inputs_dir / filename

    async def get_protocol_data(
        self,
        protocol: str,
        network: str,
        morpho_market: str,
        loan_token: str,
        galaxy_type: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        protocol = protocol.lower()
        if protocol == "morpho":
            loan_token = f"{morpho_market}-{loan_token}".lower()
            users_df = pd.read_parquet(self._path(f"users_{protocol}_{loan_token}.parquet"))
            market_df = pd.read_parquet(self._path(f"market_{protocol}_{loan_token}.parquet"))
        elif protocol == "galaxy":
            type_ = "no-class-a" if "no" in galaxy_type.lower() else "w-class-a"
            users_df = pd.read_parquet(self._path(f"users_{protocol}_{type_}.parquet"))
            market_df = pd.read_parquet(self._path(f"market_{protocol}.parquet"))
        elif protocol == "anchorage":
            users_df = pd.read_parquet(self._path(f"users_{protocol}.parquet"))
            market_df = pd.read_parquet(self._path(f"market_{protocol}.parquet"))
        else:
            loan_token = loan_token.lower()
            users_df = pd.read_parquet(self._path(f"users_{protocol}_{loan_token}.parquet"))
            market_df = pd.read_parquet(self._path(f"market_{protocol}_{loan_token}.parquet"))
        return users_df, market_df

    async def get_prices(self, collateral_list: list[str]) -> pd.DataFrame:
        prices_df = pd.read_parquet(self._path("prices_df.parquet"))
        return prices_df[list(collateral_list)]
