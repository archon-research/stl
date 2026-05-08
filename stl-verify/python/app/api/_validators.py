"""Shared FastAPI parameter validators.

These belong here (not in any single router) because multiple routers
need them. The validators return ``str`` rather than richer value
objects so the FastAPI-visible type matches the runtime type — keeping
the OpenAPI schema accurate and Pyright/mypy happy. Routers that need
the value object construct it explicitly at the call site.
"""

import re
from typing import Annotated

from pydantic import AfterValidator

from app.domain.entities.allocation import EthAddress

# Regex patterns for common validation
TX_HASH_PATTERN = r"^(?:0[xX])?[0-9a-fA-F]{64}$"
"""Ethereum transaction hash pattern. Accepts optional 0x/0X prefix followed by 64 hex chars."""


def _validate_eth_address(value: str) -> str:
    """Validate ``value`` as an EVM address; raises ``ValueError`` -> 422.

    Returns the original string unchanged if valid. Routers that need
    an :class:`EthAddress` instance call ``EthAddress(value)`` themselves.
    """
    EthAddress(value)  # raises ValueError on malformed input
    return value


def _validate_tx_hash(value: str) -> str:
    """Validate ``value`` as an Ethereum transaction hash; raises ``ValueError`` -> 422.

    Accepts optional 0x/0X prefix followed by 64 hexadecimal characters.
    The prefix is canonicalized to lowercase ``0x`` before returning so
    downstream consumers receive a consistent format.
    """
    if not re.fullmatch(TX_HASH_PATTERN, value):
        raise ValueError(f"Invalid transaction hash format: {value}")
    # Normalize uppercase 0X prefix to lowercase 0x for consistent downstream handling
    if value.startswith("0X"):
        value = "0x" + value[2:]
    return value


EthAddressParam = Annotated[str, AfterValidator(_validate_eth_address)]
"""Use as the type for any path/query/body field that holds an EVM address."""

TxHashParam = Annotated[str, AfterValidator(_validate_tx_hash)]
"""Use as the type for any path/query/body field that holds a transaction hash."""
