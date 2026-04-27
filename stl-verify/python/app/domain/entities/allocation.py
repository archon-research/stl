import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from pydantic import GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import core_schema

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

    @property
    def bytes(self) -> bytes:
        """Return the address as raw 20-byte data."""
        return bytes.fromhex(self._hex)

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

    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type: Any, _handler: Any) -> core_schema.CoreSchema:
        def validate(value: Any) -> EthAddress:
            if isinstance(value, cls):
                return value
            return cls(value)

        return core_schema.no_info_plain_validator_function(
            validate,
            serialization=core_schema.plain_serializer_function_ser_schema(lambda value: str(value)),
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls,
        _core_schema: core_schema.CoreSchema,
        _handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        return {
            "type": "string",
            "pattern": _ETH_ADDRESS_RE.pattern,
            "examples": ["0x0000000000000000000000000000000000000000"],
        }


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


@dataclass(frozen=True)
class Prime:
    id: str
    name: str
    address: str
