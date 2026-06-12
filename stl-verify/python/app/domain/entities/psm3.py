from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

# PSM3 network slugs, mirroring the PSM3 subset of Go's entity.ChainIDToName
# (internal/domain/entity/chain.go). The DB `chain.name` values ("Arbitrum One",
# "Base", ...) are display names and do not match the API's network slugs.
CHAIN_ID_TO_NETWORK = {
    8453: "base",
    10: "optimism",
    42161: "arbitrum",
    130: "unichain",
}
NETWORK_TO_CHAIN_ID = {network: chain_id for chain_id, network in CHAIN_ID_TO_NETWORK.items()}


@dataclass(frozen=True)
class Psm3Snapshot:
    """Raw PSM3 reserve snapshot row as ingested by the Go psm3-indexer."""

    chain_id: int
    address: str
    usds_balance: Decimal  # raw 1e18
    susds_balance: Decimal  # raw 1e18
    usdc_balance: Decimal  # raw 1e6
    total_assets: Decimal  # raw 1e18, PSM3.totalAssets()
    conversion_rate: Decimal  # raw 1e27, SSR oracle
    block_number: int
    block_version: int
    block_timestamp: datetime


@dataclass(frozen=True)
class Psm3Reserves:
    """PSM3 reserves normalized to token units and enriched with USD prices."""

    network: str
    address: str
    usds_balance: Decimal
    susds_balance: Decimal
    usdc_balance: Decimal
    usds_balance_usd: Decimal
    susds_balance_usd: Decimal
    usdc_balance_usd: Decimal
    usds_price: Decimal
    usdc_price: Decimal
    susds_price: Decimal
    total_assets: Decimal
    block_number: int
    block_version: int
    block_timestamp: datetime


@dataclass(frozen=True)
class PriceQuote:
    """A USD price observation from an offchain source."""

    price_usd: Decimal
    timestamp: datetime
