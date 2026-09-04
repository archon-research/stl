"""CompositeCoreModelDataReader routes each layer to its live adapter or to parquet."""

from typing import Any, cast

import pytest

from app.adapters.composite import CompositeCoreModelDataReader


class _Fake:
    """Answers every port method with its own name, so the test sees who was asked."""

    def __init__(self, name: str) -> None:
        self.name = name

    async def get_protocol_data(self, protocol, network, morpho_market, loan_token, galaxy_type):
        return self.name

    async def get_prices(self, collateral_list):
        return self.name

    async def get_orderbooks(self, collateral_list):
        return self.name


_PARQUET: Any = cast(Any, _Fake("parquet"))
_LIVE: Any = cast(Any, _Fake("live"))
_POSITIONS_ARGS = dict(protocol="SPARKLEND", network="ETHEREUM", morpho_market="", loan_token="USDT", galaxy_type="")


def _ask(reader: CompositeCoreModelDataReader, layer: str):
    return {
        "positions": reader.get_protocol_data(**_POSITIONS_ARGS),
        "prices": reader.get_prices(["WETH"]),
        "orderbooks": reader.get_orderbooks(["WETH"]),
    }[layer]


_LAYERS = ("positions", "prices", "orderbooks")


async def test_every_layer_falls_back_to_parquet_when_no_live_reader_is_given():
    reader = CompositeCoreModelDataReader(parquet=_PARQUET)
    for layer in _LAYERS:
        assert await _ask(reader, layer) == "parquet"


@pytest.mark.parametrize("live_layer", _LAYERS)
async def test_a_live_reader_serves_only_its_own_layer(live_layer):
    reader = CompositeCoreModelDataReader(parquet=_PARQUET, **{live_layer: _LIVE})
    for layer in _LAYERS:
        assert await _ask(reader, layer) == ("live" if layer == live_layer else "parquet")
