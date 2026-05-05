"""Shared FastAPI parameter validators.

These belong here (not in any single router) because multiple routers
need them. The validators return ``str`` rather than richer value
objects so the FastAPI-visible type matches the runtime type — keeping
the OpenAPI schema accurate and Pyright/mypy happy. Routers that need
the value object construct it explicitly at the call site.
"""

from typing import Annotated

from pydantic import AfterValidator

from app.domain.entities.allocation import EthAddress


def _validate_eth_address(value: str) -> str:
    """Validate ``value`` as an EVM address; raises ``ValueError`` -> 422.

    Returns the original string unchanged if valid. Routers that need
    an :class:`EthAddress` instance call ``EthAddress(value)`` themselves.
    """
    EthAddress(value)  # raises ValueError on malformed input
    return value


EthAddressParam = Annotated[str, AfterValidator(_validate_eth_address)]
"""Use as the type for any path/query/body field that holds an EVM address."""
