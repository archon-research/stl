"""The adapter for Sky's internal balance-sheet feed."""

from decimal import Decimal

import httpx
import pytest

from app.adapters.sky.internal_positions_client import SkyInternalPositionsClient
from app.domain.exceptions import ReferenceDataUnavailableError

_BASE = "https://sky.example/internal"
_PROXY = "0x1601843c5e9bc251a3272907010afa41fa18347e"
_TOKEN = "0x" + "cd" * 20
_V4_POOL_ID = "0x" + "ef" * 32


def _row(**overrides) -> dict:
    return {
        "address": _TOKEN,
        "wallet_address": _PROXY,
        "assets": "787379142.914689187128954387",
        "allocated_assets": "700000000",
        "idle_assets": "87379142.914689187128954387",
        "network": "ethereum",
        "protocol": "sparklend",
        "token_symbol": "spUSDS",
        "token_name": "Spark USDS",
        "allocation_type": "allocation",
        **overrides,
    }


def _payload(rows: list[dict], *, total: int | None = None) -> dict:
    return {
        "success": True,
        "status": 200,
        "data": {
            "results": rows,
            "pagination": {"total": len(rows) if total is None else total},
        },
    }


def _client(handler) -> SkyInternalPositionsClient:
    return SkyInternalPositionsClient(_BASE, httpx.AsyncClient(transport=httpx.MockTransport(handler)))


class _Serving:
    """A mock transport that answers with ``payload`` and records what it was asked."""

    def __init__(self, payload, *, status: int = 200) -> None:
        self.payload = payload
        self.status = status
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self.status, json=self.payload)


@pytest.mark.asyncio
async def test_reads_assets_not_exposure():
    # The whole point of this feed: `assets` is the balance-sheet figure, the
    # same measurement STL's own rows carry. The Star monitor's `exposure` is a
    # different, smaller quantity and must never land in this field.
    handler = _Serving(_payload([_row()]))

    (position,) = await _client(handler).get_positions("spark")

    assert position.assets_usd == Decimal("787379142.914689187128954387")
    assert position.allocated_assets_usd == Decimal("700000000")
    assert position.idle_assets_usd == Decimal("87379142.914689187128954387")


@pytest.mark.asyncio
async def test_asks_for_a_limit_high_enough_to_hold_a_whole_prime():
    # Upstream paginates at 20 by default and spark holds 59 positions, so an
    # unset limit silently serves a third of them as the complete set.
    handler = _Serving(_payload([_row()]))

    await _client(handler).get_positions("spark")

    (request,) = handler.requests
    assert request.url.params["prime"] == "spark"
    assert int(request.url.params["limit"]) >= 1000


@pytest.mark.asyncio
async def test_rejects_a_page_shorter_than_the_reported_total():
    # A truncated page would read as positions the prime does not hold.
    handler = _Serving(_payload([_row()], total=59))

    with pytest.raises(ReferenceDataUnavailableError, match="reported 59 rows but returned 1"):
        await _client(handler).get_positions("spark")


@pytest.mark.asyncio
async def test_carries_the_proxy_that_holds_the_position():
    # Absent from the Star monitor's feed, and the reason this one can be joined
    # to STL's rows at the grain they are stored.
    handler = _Serving(_payload([_row()]))

    (position,) = await _client(handler).get_positions("spark")

    assert position.wallet_address == _PROXY


@pytest.mark.asyncio
async def test_orders_largest_holding_first():
    handler = _Serving(
        _payload(
            [
                _row(assets="5", token_symbol="small"),
                _row(assets="900", token_symbol="large"),
                _row(assets="50", token_symbol="middle"),
            ]
        )
    )

    positions = await _client(handler).get_positions("spark")

    assert [p.symbol for p in positions] == ["large", "middle", "small"]


@pytest.mark.asyncio
async def test_maps_the_vendor_network_name_onto_a_chain_id():
    handler = _Serving(_payload([_row(network="ethereum")]))

    (position,) = await _client(handler).get_positions("spark")

    assert (position.chain_id, position.chain) == (1, "mainnet")


@pytest.mark.asyncio
async def test_leaves_an_unmapped_network_without_a_chain_id():
    # 0 is not available as a placeholder: it already means off-chain custody,
    # so an unmapped EVM position would be served as one.
    handler = _Serving(_payload([_row(network="plume")]))

    (position,) = await _client(handler).get_positions("spark")

    assert (position.chain_id, position.chain, position.network) == (None, None, "plume")


@pytest.mark.asyncio
async def test_keeps_a_uniswap_v4_pool_id_verbatim():
    # Two of spark's 59 rows carry a 32-byte pool id where an address goes. It
    # cannot resolve to a receipt token, but the row is a real position.
    handler = _Serving(_payload([_row(address=_V4_POOL_ID)]))

    (position,) = await _client(handler).get_positions("spark")

    assert position.token_address == _V4_POOL_ID


@pytest.mark.asyncio
async def test_serves_an_empty_list_for_an_unknown_star():
    # Upstream answers an unknown prime with 200 and no rows rather than 404, so
    # the adapter cannot tell coverage from emptiness and must not try. The
    # service gates on the Star monitor's tracked set instead.
    handler = _Serving(_payload([]))

    assert await _client(handler).get_positions("zzznotastar") == ()


@pytest.mark.asyncio
async def test_rejects_a_row_missing_a_field_that_identifies_it():
    handler = _Serving(_payload([_row(token_symbol=None)]))

    with pytest.raises(ReferenceDataUnavailableError, match="token_symbol"):
        await _client(handler).get_positions("spark")


@pytest.mark.asyncio
async def test_rejects_a_non_finite_figure():
    # Decimal accepts "NaN" without complaint; left through it poisons every
    # total it reaches and makes the sort raise.
    handler = _Serving(_payload([_row(assets="NaN")]))

    with pytest.raises(ReferenceDataUnavailableError, match="non-finite"):
        await _client(handler).get_positions("spark")


@pytest.mark.asyncio
async def test_rejects_a_non_success_status():
    handler = _Serving({"detail": "nope"}, status=503)

    with pytest.raises(ReferenceDataUnavailableError, match="status 503"):
        await _client(handler).get_positions("spark")
