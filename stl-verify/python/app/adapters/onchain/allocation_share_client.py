# app/adapters/onchain/allocation_share_client.py
from decimal import Decimal

import httpx


_TOTAL_SUPPLY_SELECTOR = "0x18160ddd"
_BALANCE_OF_SELECTOR   = "0x70a08231"


class OnchainAllocationShareClient:
    """Fetch allocation share via JSON-RPC: balanceOf(wallet) / totalSupply().

    Makes a single batched eth_call request to Alchemy to retrieve both values.
    Intended for Aave-like protocols (SparkLend, Aave v2/v3) where the receipt
    token is a standard ERC-20 aToken.
    """

    def __init__(
        self,
        receipt_token_address: bytes,
        wallet_address: bytes,
        alchemy_url: str,
    ) -> None:
        if len(receipt_token_address) != 20:
            raise ValueError(f"receipt_token_address must be 20 bytes, got {len(receipt_token_address)}")
        if len(wallet_address) != 20:
            raise ValueError(f"wallet_address must be 20 bytes, got {len(wallet_address)}")
        self._token_addr = "0x" + receipt_token_address.hex()
        self._wallet_addr = "0x" + wallet_address.hex()
        self._alchemy_url = alchemy_url

    async def get_share(self) -> Decimal:
        """Return wallet's share of the pool: balanceOf / totalSupply."""
        padded_wallet = "000000000000000000000000" + self._wallet_addr[2:]
        payload = [
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_call",
                "params": [{"to": self._token_addr, "data": _TOTAL_SUPPLY_SELECTOR}, "latest"],
            },
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "eth_call",
                "params": [{"to": self._token_addr, "data": _BALANCE_OF_SELECTOR + padded_wallet}, "latest"],
            },
        ]
        async with httpx.AsyncClient() as client:
            response = await client.post(self._alchemy_url, json=payload)
            response.raise_for_status()

        results = {}
        for item in response.json():
            if "error" in item:
                raise ValueError(
                    f"eth_call failed for token {self._token_addr}: {item['error']}"
                )
            results[item["id"]] = item["result"]
        total_supply = int(results[1], 16)
        balance      = int(results[2], 16)

        if total_supply == 0:
            raise ValueError(f"total supply is zero for token {self._token_addr}")

        return Decimal(balance) / Decimal(total_supply)


class FixedAllocationShare:
    """Returns a pre-configured share value.

    Used for protocols where the share is already accounted for in the
    breakdown (e.g. Morpho, where the backed breakdown is vault-scoped).
    """

    def __init__(self, share: Decimal) -> None:
        self._share = share

    async def get_share(self) -> Decimal:
        return self._share
