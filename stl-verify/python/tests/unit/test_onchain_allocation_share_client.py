# tests/unit/test_onchain_allocation_share_client.py
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

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


@pytest.mark.asyncio
async def test_get_share_returns_correct_ratio() -> None:
    """Client batches two eth_call requests and returns balance/totalSupply."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = [
        {"id": 1, "result": _TOTAL_SUPPLY_HEX},
        {"id": 2, "result": _BALANCE_HEX},
    ]

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.post = AsyncMock(return_value=mock_response)

    with patch("app.adapters.onchain.allocation_share_client.httpx.AsyncClient", return_value=mock_client):
        client = OnchainAllocationShareClient(
            receipt_token_address=SP_TOKEN,
            wallet_address=WALLET,
            alchemy_url="https://eth-mainnet.g.alchemy.com/v2/FAKE",
        )
        share = await client.get_share()

    expected = Decimal(334_800_442_640000) / Decimal(336_685_941_040000)
    assert share == expected


@pytest.mark.asyncio
async def test_get_share_raises_on_zero_total_supply() -> None:
    """Zero total supply should raise ValueError — division by zero is a data error."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json.return_value = [
        {"id": 1, "result": "0x" + (0).to_bytes(32, "big").hex()},
        {"id": 2, "result": _BALANCE_HEX},
    ]

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.post = AsyncMock(return_value=mock_response)

    with patch("app.adapters.onchain.allocation_share_client.httpx.AsyncClient", return_value=mock_client):
        client = OnchainAllocationShareClient(
            receipt_token_address=SP_TOKEN,
            wallet_address=WALLET,
            alchemy_url="https://eth-mainnet.g.alchemy.com/v2/FAKE",
        )
        with pytest.raises(ValueError, match="total supply is zero"):
            await client.get_share()


@pytest.mark.asyncio
async def test_fixed_allocation_share_returns_configured_value() -> None:
    fixed = FixedAllocationShare(Decimal("1"))
    assert await fixed.get_share() == Decimal("1")
