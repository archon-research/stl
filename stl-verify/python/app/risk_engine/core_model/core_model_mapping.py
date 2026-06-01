"""asset_id -> market_key mapping loader for the CORE model.

Follows the identical pattern as app/risk_engine/mapping.py (SURAF).
The JSON file maps composite keys (chain_id:0xAddress) to market_key
strings (e.g. "morpho_cbbtc_usdc"). At startup, resolve_receipt_token_mapping
converts these to {asset_id: market_key}.
"""

from pathlib import Path

from app.risk_engine.mapping import load_asset_mapping


def load_core_model_mapping(path: Path) -> list[tuple[int, bytes, str]]:
    """Load and validate the asset -> market_key mapping file.

    Returns a list of (chain_id, receipt_token_address, market_key) tuples.
    Delegates validation to the shared load_asset_mapping utility.
    """
    return load_asset_mapping(path)
