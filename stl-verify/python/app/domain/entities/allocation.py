import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

_ETH_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


class EthAddress:
    """Value object for a validated Ethereum address (0x-prefixed, 40 hex chars)."""

    __slots__ = ("_hex",)

    def __init__(self, value: str) -> None:
        if not isinstance(value, str) or not _ETH_ADDRESS_RE.match(value):
            raise ValueError(f"Invalid Ethereum address: {value!r} (expected 0x followed by 40 hex characters)")
        self._hex: str = value.removeprefix("0x")

    @property
    def hex(self) -> str:
        """The raw 40-character hex string without 0x prefix."""
        return self._hex

    def __str__(self) -> str:
        return f"0x{self._hex}"

    def __repr__(self) -> str:
        return f"EthAddress('0x{self._hex}')"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, EthAddress):
            return self._hex.lower() == other._hex.lower()
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._hex.lower())


@dataclass(frozen=True)
class ReceiptTokenPosition:
    """A receipt token held by a star, with balance and protocol info."""

    receipt_token_id: int
    symbol: str
    underlying_symbol: str
    protocol_name: str
    balance: Decimal
    token_address: str | None


@dataclass
class Star:
    id: str
    name: str
    address: str


@dataclass
class AllocationPosition:
    chain_id: int
    name: str
    proxy_address: str
    token_address: str
    token_symbol: str | None
    token_decimals: int | None
    balance: Decimal
    scaled_balance: Decimal | None
    block_number: int
    block_version: int
    tx_hash: str
    log_index: int
    tx_amount: Decimal
    direction: str
    created_at: datetime
