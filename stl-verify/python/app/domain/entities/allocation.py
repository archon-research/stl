import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema

_ETH_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


class EthAddress(str):
    """Value object for a validated Ethereum address (0x-prefixed, 40 hex chars).

    Subclasses ``str`` so it interoperates with Pydantic, JSON, and OpenAPI
    natively. Preserves the original case of the input (e.g. EIP-55
    checksummed form) while comparing and hashing case-insensitively.
    """

    __slots__ = ()

    def __new__(cls, value: str) -> "EthAddress":
        if not isinstance(value, str) or not _ETH_ADDRESS_RE.match(value):
            raise ValueError(f"Invalid Ethereum address: {value!r} (expected 0x followed by 40 hex characters)")
        return super().__new__(cls, value)

    @property
    def hex(self) -> str:
        """The raw 40-character hex string without 0x prefix."""
        return self.removeprefix("0x")

    def to_bytes(self) -> bytes:
        """Return the address as raw 20 bytes (matches DB ``BYTEA`` columns)."""
        return bytes.fromhex(self.hex)

    def __repr__(self) -> str:
        return f"EthAddress('{self!s}')"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, EthAddress):
            return str.lower(self) == str.lower(other)
        return NotImplemented

    def __hash__(self) -> int:
        return hash(str.lower(self))

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler: GetCoreSchemaHandler) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, core_schema.str_schema())


@dataclass(frozen=True)
class ReceiptTokenPosition:
    """A receipt token held by a prime, enriched with its underlying token info."""

    chain_id: int
    receipt_token_id: int
    receipt_token_address: str
    underlying_token_id: int
    underlying_token_address: str
    symbol: str
    underlying_symbol: str
    protocol_name: str
    balance: Decimal
    amount_usd: Decimal | None = None
    latest_activity_at: datetime | None = None


@dataclass(frozen=True)
class DirectAssetHolding:
    """A token held directly by a prime that is not a registered receipt-token wrapper.

    The ``underlying_*`` fields are set when the holding is allowlisted for
    underlying-value pricing and its row carries a resolvable underlying (the
    pricing basis for ``amount_usd``), and are always set or unset together.
    For non-allowlisted holdings ``None`` means the token prices by its own
    oracle and the underlying is the token itself; for allowlisted holdings
    ``None`` marks a row with no resolvable underlying (e.g. written before
    the type's valuation deployed), a surfaced coverage gap priced as NULL,
    never by the share-count balance.
    """

    chain_id: int
    token_id: int
    token_address: str
    symbol: str
    balance: Decimal
    amount_usd: Decimal | None = None
    latest_activity_at: datetime | None = None
    underlying_token_id: int | None = None
    underlying_token_address: str | None = None
    underlying_symbol: str | None = None


@dataclass(frozen=True)
class Prime:
    id: str
    name: str
    address: str


@dataclass(frozen=True)
class ChainMetadata:
    chain_id: int
    name: str


@dataclass(frozen=True)
class ProtocolMetadata:
    id: int
    chain_id: int
    encode: str
    name: str
