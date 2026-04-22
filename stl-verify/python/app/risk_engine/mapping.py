"""Asset -> rating_id mapping.

The mapping file is a flat JSON object: ``{asset: rating_id}``. The
``asset`` key is treated as an opaque string at this layer; canonical
key format (token symbol vs address vs something else) is an open
question tracked in the mapping file itself.

Lookups are case-insensitive: keys are casefolded at load time and
callers must casefold the asset argument before ``.get(...)``.

This module is deliberately independent of the SURAF loader: it does
not check that a ``rating_id`` exists in the loaded ratings. That kind
of cross-validation, if we want it, belongs in startup wiring.
"""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import RootModel, ValidationError


class MappingError(Exception):
    """Raised when the asset -> rating_id mapping file cannot be loaded."""


class _AssetMapping(RootModel[dict[str, str]]):
    pass


def load_asset_mapping(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise MappingError(f"asset mapping file not found: {path}")

    try:
        raw = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise MappingError(f"asset mapping file is not valid JSON ({path}): {exc}") from exc

    try:
        parsed = _AssetMapping.model_validate(raw).root
    except ValidationError as exc:
        raise MappingError(f"asset mapping file has wrong shape ({path}): {exc}") from exc
    return {k.casefold(): v for k, v in parsed.items()}
