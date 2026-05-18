"""Shared FastAPI parameter validators and the API's ID-error contract.

These belong here (not in any single router) because multiple routers
need them. The validators return ``str`` rather than richer value
objects so the FastAPI-visible type matches the runtime type — keeping
the OpenAPI schema accurate and Pyright/mypy happy. Routers that need
the value object construct it explicitly at the call site.

ID-error contract
-----------------
All endpoints follow these semantics for identifier handling:

* **Malformed ID** (wrong format/length/charset, in any parameter — path,
  query, or body): ``422 Unprocessable Entity``. Validation runs before
  any business logic.
* **Valid but unknown path resource** (a well-formed identifier that
  does not correspond to a known entity, when the identifier names the
  resource being addressed — e.g. ``/v1/primes/{prime_id}/...``):
  ``404 Not Found``.
* **Valid filter with no matching rows** (a well-formed identifier used
  as a query filter rather than a path resource — e.g.
  ``/v1/allocations/activity?prime_id=...``): ``200 []``. An empty
  result is a valid answer to a filter query.

Use :data:`EthAddressParam` and :data:`TxHashParam` for required fields
and :data:`OptionalEthAddressParam` for optional query filters so
malformed-ID rejection is uniform across the API.
"""

import re
from typing import Annotated

from fastapi import Path
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


def _validate_optional_eth_address(value: str | None) -> str | None:
    """Validate an optional EVM address; ``None`` passes through unchanged.

    Use for query filters that accept an optional address. Reuses
    :func:`_validate_eth_address` so malformed input still raises
    ``ValueError`` and surfaces as ``422``.
    """
    if value is None:
        return None
    return _validate_eth_address(value)


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

OptionalEthAddressParam = Annotated[str | None, AfterValidator(_validate_optional_eth_address)]
"""Use as the type for optional query filters that hold an EVM address."""

TxHashParam = Annotated[str, AfterValidator(_validate_tx_hash)]
"""Use as the type for any path/query/body field that holds a transaction hash."""


ChainIdPath = Annotated[
    int,
    Path(ge=1, description="EVM chain id.", examples=[1]),
]
"""Path parameter for EVM chain id (must be >= 1)."""


TokenAddressPath = Annotated[
    str,
    Path(
        description="0x-prefixed token contract address (40 hex chars).",
        examples=["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"],
    ),
    AfterValidator(_validate_eth_address),
]
"""Path parameter for an EVM token address. Validation matches ``EthAddressParam``."""
