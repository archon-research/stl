"""Shared serialization types for money/quantity ``Decimal`` fields.

Lives in the domain (not under ``app/api``) so both domain entities that
double as response models (``RrcResult`` et al.) and the API routers can share
one JSON representation without the domain importing an outer layer.
"""

from decimal import Decimal
from typing import Annotated

from pydantic import PlainSerializer, TypeAdapter, WithJsonSchema

# ``WithJsonSchema`` below replaces the serialization schema wholesale, so these
# fields would degrade to a bare ``string`` without restating pydantic's own
# schema for a ``Decimal``. Derived rather than copied: a pydantic upgrade that
# changes (or drops) the numeric-string ``pattern`` follows through here instead
# of silently drifting the committed ``openapi-schema.json``, which nothing in
# CI regenerates. One ``TypeAdapter`` build at import; pydantic hands out a copy
# per use, so sharing the dict across every field is safe.
_DECIMAL_SERIALIZATION_SCHEMA = TypeAdapter(Decimal).json_schema(mode="serialization")


def _decimal_to_plain_string(value: Decimal) -> str:
    """Render a ``Decimal`` as a fixed-point string, never in exponential form.

    asyncpg decodes a large trailing-zero ``NUMERIC`` (e.g. a 1e18-scaled wad)
    into a ``Decimal`` with a *positive* exponent, and pydantic's default
    serializer is ``str(value)`` — so ``2570714…E+27`` reaches the client as
    ``"2.5707140E+27"``. Consumers that parse with ``BigInt`` (which rejects
    exponents) then read it as ``0``. ``format(value, "f")`` expands to the
    exact fixed-point digits the API contract promises.
    """
    return format(value, "f")


PlainDecimal = Annotated[
    Decimal,
    PlainSerializer(_decimal_to_plain_string, return_type=str, when_used="json"),
    WithJsonSchema(_DECIMAL_SERIALIZATION_SCHEMA, mode="serialization"),
]
"""A ``Decimal`` that always serializes to a plain (non-exponential) JSON string.

Use for every ``Decimal`` response field. Values stay exact; only the textual
form changes (fixed-point instead of scientific notation).
"""
