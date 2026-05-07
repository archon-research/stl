# Data Entities

This inventory reflects data products currently persisted by STL Verify as of May 2026.

| Data Entity | Source | Chain | Collection Method | Storage |
|---|---|---|---|---|
| On-chain Oracle Prices | SparkLend Oracle (Aave-style) | Ethereum | Multicall3 `getAssetPrice()` via SQS events + Erigon backfill | PostgreSQL `onchain_token_price` + metadata tables `oracle`, `protocol_oracle`, `oracle_asset` |
| On-chain Oracle Prices | Chainlink feeds | Ethereum | Multicall3 `latestRoundData()` via SQS events + Erigon backfill | PostgreSQL `onchain_token_price` + `oracle*` metadata |
| On-chain Oracle Prices | Chronicle feeds | Ethereum | Direct caller `latestAnswer()` via SQS events + Erigon backfill | PostgreSQL `onchain_token_price` + `oracle*` metadata |
| On-chain Oracle Prices | Redstone feeds | Ethereum | Multicall3 `latestRoundData()` via SQS events + Erigon backfill | PostgreSQL `onchain_token_price` + `oracle*` metadata |
| Off-chain Market Prices | Offchain source adapters (for example CoinGecko) | N/A | HTTP polling with rate limiting and source asset mapping | PostgreSQL `offchain_token_price`, `offchain_price_source`, `offchain_price_asset` |
| Lending Positions | SparkLend | Ethereum | Event log decoding from receipts | PostgreSQL `borrower`, `borrower_collateral`, `protocol_event` |
| Lending Positions | Aave V2 | Ethereum | Event log decoding from receipts | PostgreSQL `borrower`, `borrower_collateral`, `protocol_event` |
| Lending Positions | Aave V3 | Ethereum | Event log decoding from receipts | PostgreSQL `borrower`, `borrower_collateral`, `protocol_event` |
| Lending Positions | Aave V3 Lido | Ethereum | Event log decoding from receipts | PostgreSQL `borrower`, `borrower_collateral`, `protocol_event` |
| Lending Positions | Aave V3 RWA | Ethereum | Event log decoding from receipts | PostgreSQL `borrower`, `borrower_collateral`, `protocol_event` |
| Reserve State | All 5 lending protocols | Ethereum | Multicall3 `getReserveData()` + `getReserveConfigurationData()` | PostgreSQL `sparklend_reserve_data` |
| Allocation Positions | Prime allocation tracker | Ethereum and supported EVM chains | Event-driven + periodic sweep balance reads | PostgreSQL `allocation_position` |
| Token Total Supply | Prime allocation tracker | Ethereum and supported EVM chains | Same multicall batch as allocation tracker balance reads | PostgreSQL `token_total_supply` |
| Prime Debt Snapshots | Sky/Maker vault debt readers | Ethereum | Periodic reads and block-aware snapshot writes | PostgreSQL `prime_debt` |
| Morpho Market State and Positions | Morpho indexer | Ethereum | Event + state indexing | PostgreSQL `morpho_market`, `morpho_market_state`, `morpho_market_position`, `morpho_vault`, `morpho_vault_state`, `morpho_vault_position` |
| Anchorage Package Snapshots | Anchorage API | N/A (prime-level collateral feed) | API polling | PostgreSQL `anchorage_package_snapshot` |
| Anchorage Operations | Anchorage API | N/A (prime-level collateral feed) | API polling and cursor backfill | PostgreSQL `anchorage_operation` |
| Raw Blocks | Alchemy WS + Erigon RPC | Multi-chain (for example Ethereum, Avalanche, Base, Optimism, Arbitrum, Unichain) | WebSocket (live), HTTP RPC (backfill) | PostgreSQL `block_states` + Redis + S3 |
| Raw Receipts | Alchemy WS + Erigon RPC | Multi-chain | WebSocket (live), HTTP RPC (backfill) | PostgreSQL + Redis + S3 |
| Raw Traces | Alchemy WS + Erigon RPC | Chain-dependent | WebSocket (live), `trace_block` RPC (backfill) | PostgreSQL + Redis + S3 |
| Raw Blobs | Alchemy WS + Erigon RPC | Ethereum (post-Dencun) | WebSocket (live), `eth_getBlobSidecars` (backfill) | PostgreSQL + Redis + S3 |

## Planned or Partial (Not Fully Productized in Python API)

| Data Entity | Current State | Notes |
|---|---|---|
| Protocol event detail endpoints | Persisted, partially surfaced | `protocol_event` is persisted and used for activity summaries; tx-level detail endpoints are still additive API work. |
| Price provenance endpoint surfaces | Persisted, partially surfaced | Source and oracle metadata exist in DB but are not fully exposed across all API contracts yet. |
| Token catalog endpoints | Persisted, partially surfaced | `token`/`receipt_token` metadata is stored and joined in places; dedicated catalog endpoints remain additive API work. |
| Cross-prime all-allocations endpoint | Not yet shipped | Can be implemented from canonical allocation reads over existing storage. |
