"""Asset -> rating_id mapping.

The mapping file is a flat JSON object keyed by
``chain_id:0xReceipt_token_address`` composite keys with ``rating_id``
string values.  At load time the file is parsed and each key is
validated; the result is a list of ``(chain_id, address_bytes,
rating_id)`` tuples ready for DB resolution in startup wiring.

This module is deliberately independent of the SURAF loader and the
database: it only validates file structure and key syntax.
Cross-validation (rating_ids against loaded ratings, addresses against
the DB) belongs in startup wiring.
"""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import RootModel, ValidationError


class MappingError(Exception):
    """Raised when the asset -> rating_id mapping file cannot be loaded."""


class _RawMapping(RootModel[dict[str, str]]):
    pass


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """``object_pairs_hook`` for ``json.loads`` that rejects duplicate keys.

    Python's default JSON parser silently drops earlier values when a key
    appears more than once.  This hook raises before that can happen.
    """
    seen: set[str] = set()
    for key, _ in pairs:
        if key in seen:
            raise MappingError(f"duplicate key in mapping JSON: {key!r}")
        seen.add(key)
    return dict(pairs)


def _parse_composite_key(raw_key: str) -> tuple[int, bytes]:
    """Parse ``'chain_id:0xHexAddress'`` into ``(chain_id, address_bytes)``.

    Raises ``MappingError`` on any format violation.
    """
    parts = raw_key.split(":", 1)
    if len(parts) != 2:
        raise MappingError(f"invalid composite key (expected chain_id:0xAddress): {raw_key!r}")

    chain_str, addr_str = parts

    try:
        chain_id = int(chain_str)
    except ValueError:
        raise MappingError(f"invalid composite key (chain_id is not an integer): {raw_key!r}")

    if not addr_str.startswith("0x"):
        raise MappingError(f"invalid composite key (address missing 0x prefix): {raw_key!r}")

    try:
        address = bytes.fromhex(addr_str[2:])
    except ValueError:
        raise MappingError(f"invalid composite key (address is not valid hex): {raw_key!r}")

    if len(address) != 20:
        raise MappingError(f"invalid composite key (address must be 20 bytes, got {len(address)}): {raw_key!r}")

    return chain_id, address


def load_asset_mapping(path: Path) -> list[tuple[int, bytes, str]]:
    """Load and validate the asset mapping file.

    Returns a list of ``(chain_id, receipt_token_address, rating_id)``
    tuples.  Does **not** resolve addresses to ``receipt_token_id`` —
    that requires DB access and belongs in startup wiring.
    """
    if not path.is_file():
        raise MappingError(f"asset mapping file not found: {path}")

    try:
        raw = json.loads(path.read_text(), object_pairs_hook=_reject_duplicate_json_keys)
    except MappingError as exc:
        raise MappingError(f"{exc} ({path})") from exc
    except json.JSONDecodeError as exc:
        raise MappingError(f"asset mapping file is not valid JSON ({path}): {exc}") from exc

    try:
        parsed = _RawMapping.model_validate(raw).root
    except ValidationError as exc:
        raise MappingError(f"asset mapping file has wrong shape ({path}): {exc}") from exc

    result: list[tuple[int, bytes, str]] = []
    seen: dict[tuple[int, bytes], str] = {}
    for key, rating_id in parsed.items():
        chain_id, address = _parse_composite_key(key)
        normalized = (chain_id, address)
        if normalized in seen:
            addr_hex = "0x" + address.hex()
            raise MappingError(
                f"duplicate receipt token in mapping: chain_id={chain_id} address={addr_hex}"
                f" (rating_ids {seen[normalized]!r} and {rating_id!r})"
            )
        seen[normalized] = rating_id
        result.append((chain_id, address, rating_id))
    return result
