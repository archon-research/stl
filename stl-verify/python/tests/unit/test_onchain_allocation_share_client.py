from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.adapters.onchain.allocation_share_client import (
    FixedAllocationShare,
    OnchainAllocationShareClient,
)

SP_TOKEN = bytes.fromhex("e7df13b8e3d6740fe17cbe928c7334243d86c92f")
WALLET   = bytes.fromhex("1601843c5e9bc251a3272907010afa41fa18347e")

# totalSupply = 336_685_941_040000 (6 decimals), balance = 334_800_442_640000
_TOTAL_SUPPLY_HEX = "0x" + (336_685_941_040000).to_bytes(32, "big").hex()
_BALANCE_HEX      = "0x" + (334_800_442_640000).to_bytes(32, "big").hex()


def _mock_http_client(response_json: list[dict]) -> AsyncMock:
    """Create a mock httpx.AsyncClient with a pre-configured POST response."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = response_json

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    return mock_client


@pytest.mark.asyncio
async def test_get_share_returns_correct_ratio() -> None:
    """Client batches two eth_call requests and returns balance/totalSupply."""
    mock_client = _mock_http_client([
        {"id": 1, "result": _TOTAL_SUPPLY_HEX},
        {"id": 2, "result": _BALANCE_HEX},
    ])

    client = OnchainAllocationShareClient(
        receipt_token_address=SP_TOKEN,
        wallet_address=WALLET,
        alchemy_url="https://eth-mainnet.g.alchemy.com/v2/FAKE",
        http_client=mock_client,
    )
    share = await client.get_share()

    expected = Decimal(334_800_442_640000) / Decimal(336_685_941_040000)
    assert share == expected
    mock_client.post.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_share_raises_on_zero_total_supply() -> None:
    """Zero total supply should raise ValueError -- division by zero is a data error."""
    mock_client = _mock_http_client([
        {"id": 1, "result": "0x" + (0).to_bytes(32, "big").hex()},
        {"id": 2, "result": _BALANCE_HEX},
    ])

    client = OnchainAllocationShareClient(
        receipt_token_address=SP_TOKEN,
        wallet_address=WALLET,
        alchemy_url="https://eth-mainnet.g.alchemy.com/v2/FAKE",
        http_client=mock_client,
    )
    with pytest.raises(ValueError, match="total supply is zero"):
        await client.get_share()


@pytest.mark.asyncio
async def test_fixed_allocation_share_returns_configured_value() -> None:
    fixed = FixedAllocationShare(Decimal("1"))
    assert await fixed.get_share() == Decimal("1")


@pytest.mark.asyncio
async def test_reuses_injected_http_client() -> None:
    """Verify the same http_client is used across multiple calls."""
    mock_client = _mock_http_client([
        {"id": 1, "result": _TOTAL_SUPPLY_HEX},
        {"id": 2, "result": _BALANCE_HEX},
    ])

    client = OnchainAllocationShareClient(
        receipt_token_address=SP_TOKEN,
        wallet_address=WALLET,
        alchemy_url="https://eth-mainnet.g.alchemy.com/v2/FAKE",
        http_client=mock_client,
    )
    await client.get_share()
    await client.get_share()

    assert mock_client.post.await_count == 2
