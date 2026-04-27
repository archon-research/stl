import re

_AAVE_LIKE = frozenset({"sparklend", "aave_v2", "aave_v3", "aave_v3_lido", "aave_v3_rwa"})
_MORPHO = frozenset({"morpho_blue"})
_NORMALIZE_RE = re.compile(r"[\s\-_]+")


def normalize_protocol_name(protocol_name: str) -> str:
    """Normalise protocol names for matching across DB naming variants."""
    return _NORMALIZE_RE.sub("_", protocol_name.strip().casefold())


def is_aave_like_protocol(protocol_name: str) -> bool:
    return normalize_protocol_name(protocol_name) in _AAVE_LIKE


def is_morpho_protocol(protocol_name: str) -> bool:
    return normalize_protocol_name(protocol_name) in _MORPHO


def supports_gap_sweep(protocol_name: str) -> bool:
    normalized = normalize_protocol_name(protocol_name)
    return normalized in _AAVE_LIKE or normalized in _MORPHO
