"""Shared validation for the ``usd_exposure`` scenario override.

Extracted verbatim from the SURAF service so every model that accepts
``usd_exposure`` validates it identically (same bounds, same error strings).
"""

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from app.domain.exceptions import InvalidOverrideError

_ALLOWED_OVERRIDES = frozenset({"usd_exposure"})
# Sanity ceiling on overridden exposure: above this bound a request is more
# likely to be malformed (or a CPU-burn attack via a multi-million-digit
# Decimal string) than a legitimate scenario. 1e15 USD ≈ a quadrillion.
_USD_EXPOSURE_MAX = Decimal("1e15")
# Reject pathological input strings *before* `Decimal(str(raw))` — parsing a
# multi-megabyte numeric literal is the actual CPU-burn vector; the bound
# check above only fires after parse.
_DECIMAL_STR_MAX_LEN = 64


def parse_usd_exposure_override(overrides: Mapping[str, Any]) -> Decimal | None:
    """Validate the override mapping and return the ``usd_exposure`` value, or None if absent.

    Raises :class:`InvalidOverrideError` on unknown keys or a malformed,
    non-positive, non-finite, or out-of-range value. Callers fall back to the
    position-derived exposure when this returns None.
    """
    unknown = set(overrides) - _ALLOWED_OVERRIDES
    if unknown:
        raise InvalidOverrideError(f"unknown override keys: {sorted(unknown)}")

    if "usd_exposure" not in overrides:
        return None

    raw = overrides["usd_exposure"]
    if raw is None:
        raise InvalidOverrideError("invalid usd_exposure: expected a positive finite number, got None")
    if isinstance(raw, str) and len(raw) > _DECIMAL_STR_MAX_LEN:
        raise InvalidOverrideError(f"invalid usd_exposure: input string too long ({len(raw)} > {_DECIMAL_STR_MAX_LEN})")
    try:
        usd_exposure = raw if isinstance(raw, Decimal) else Decimal(str(raw))
    except Exception as exc:
        raise InvalidOverrideError(f"invalid usd_exposure: expected a positive finite number, got {raw!r}") from exc
    if not usd_exposure.is_finite():
        raise InvalidOverrideError(f"invalid usd_exposure: expected a positive finite number, got {usd_exposure}")
    if usd_exposure <= Decimal("0"):
        raise InvalidOverrideError(f"invalid usd_exposure: expected a positive finite number, got {usd_exposure}")
    if usd_exposure > _USD_EXPOSURE_MAX:
        raise InvalidOverrideError(f"usd_exposure must be <= {_USD_EXPOSURE_MAX:E}, got {usd_exposure}")
    return usd_exposure
