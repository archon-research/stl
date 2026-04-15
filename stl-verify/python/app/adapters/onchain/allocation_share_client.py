import asyncio
from decimal import Decimal

import httpx

from app.logging import get_logger

logger = get_logger(__name__)

_TOTAL_SUPPLY_SELECTOR = "0x18160ddd"
_BALANCE_OF_SELECTOR = "0x70a08231"

_REQUEST_TIMEOUT = 10.0  # seconds per attempt
_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 1.0  # seconds; doubles each attempt (1s, 2s)


class OnchainAllocationShareClient:
    """Fetch allocation share via JSON-RPC: balanceOf(wallet) / totalSupply().

    Makes a single batched eth_call request to Alchemy to retrieve both values.
    Intended for Aave-like protocols (SparkLend, Aave v2/v3) where the receipt
    token is a standard ERC-20 aToken.

    Transient HTTP errors are retried up to _MAX_RETRIES times with exponential
    backoff. Permanent application-level errors (eth_call error responses, zero
    total supply) are not retried.

    TODO: Remove this client once allocation share data is indexed or can be
    derived from database numbers.
    """

    def __init__(
        self,
        receipt_token_address: bytes,
        wallet_address: bytes,
        alchemy_url: str,
        http_client: httpx.AsyncClient,
    ) -> None:
        if len(receipt_token_address) != 20:
            raise ValueError(f"receipt_token_address must be 20 bytes, got {len(receipt_token_address)}")
        if len(wallet_address) != 20:
            raise ValueError(f"wallet_address must be 20 bytes, got {len(wallet_address)}")
        self._token_addr = "0x" + receipt_token_address.hex()
        self._wallet_addr = "0x" + wallet_address.hex()
        self._alchemy_url = alchemy_url
        self._http_client = http_client

    async def get_share(self) -> Decimal:
        """Return wallet's share of the pool: balanceOf / totalSupply.

        Retries up to _MAX_RETRIES times on transient HTTP errors with
        exponential backoff. Raises IOError if all attempts are exhausted.
        """
        payload = self._build_payload()
        last_exc: Exception | None = None

        for attempt in range(_MAX_RETRIES):
            try:
                response = await self._http_client.post(
                    self._alchemy_url,
                    json=payload,
                    timeout=_REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                return self._parse_response(response)
            except httpx.HTTPStatusError as exc:
                # 4xx errors are client errors — retrying won't help.
                if exc.response.status_code < 500:
                    raise IOError(
                        f"RPC request failed with status {exc.response.status_code} for token {self._token_addr}"
                    ) from exc
                last_exc = exc
                logger.warning(
                    "allocation_share_client: transient HTTP %d for token %s (attempt %d/%d)",
                    exc.response.status_code,
                    self._token_addr,
                    attempt + 1,
                    _MAX_RETRIES,
                )
            except httpx.TransportError as exc:
                last_exc = exc
                logger.warning(
                    "allocation_share_client: transport error for token %s (attempt %d/%d): %s",
                    self._token_addr,
                    attempt + 1,
                    _MAX_RETRIES,
                    exc,
                )

            if attempt < _MAX_RETRIES - 1:
                await asyncio.sleep(_RETRY_BACKOFF_BASE * (2**attempt))

        raise IOError(f"RPC request failed after {_MAX_RETRIES} attempts for token {self._token_addr}") from last_exc

    def _build_payload(self) -> list[dict]:
        padded_wallet = self._wallet_addr[2:].zfill(64)
        return [
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

    def _parse_response(self, response: httpx.Response) -> Decimal:
        results = {}
        for item in response.json():
            if "error" in item:
                raise ValueError(f"eth_call failed for token {self._token_addr}: {item['error']}")
            results[item["id"]] = item["result"]
        total_supply = int(results[1], 16)
        balance = int(results[2], 16)

        if total_supply == 0:
            logger.warning(
                "allocation_share_client: total supply is zero for token %s; returning share=0",
                self._token_addr,
            )
            return Decimal("0")

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
