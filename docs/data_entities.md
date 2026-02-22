# Data Entities

| Data Entity | Source | Chain | Collection Method | Storage |
|---|---|---|---|---|
| On-chain Oracle Prices | SparkLend Oracle (Aave-style) | Ethereum | Multicall3 `getAssetPrice()` via SQS events + Erigon backfill | PostgreSQL `onchain_token_price` |
| On-chain Oracle Prices | Chainlink (15 feeds) | Ethereum | Multicall3 `latestRoundData()` via SQS events + Erigon backfill | PostgreSQL `onchain_token_price` |
| On-chain Oracle Prices | Chronicle (7 feeds) | Ethereum | DirectCaller `latestAnswer()` (toll/whitelist) via SQS events + Erigon backfill | PostgreSQL `onchain_token_price` |
| On-chain Oracle Prices | Redstone (12 feeds) | Ethereum | Multicall3 `latestRoundData()` via SQS events + Erigon backfill | PostgreSQL `onchain_token_price` |
| Off-chain Market Prices | CoinGecko Pro API (18 assets) | N/A | HTTP polling with rate limiting | PostgreSQL `offchain_token_price` |
| Lending Positions | SparkLend | Ethereum | Event log decoding from receipts | PostgreSQL `borrower`, `borrower_collateral`, `protocol_event` |
| Lending Positions | Aave V2 | Ethereum | Event log decoding from receipts | PostgreSQL `borrower`, `borrower_collateral`, `protocol_event` |
| Lending Positions | Aave V3 | Ethereum | Event log decoding from receipts | PostgreSQL `borrower`, `borrower_collateral`, `protocol_event` |
| Lending Positions | Aave V3 Lido | Ethereum | Event log decoding from receipts | PostgreSQL `borrower`, `borrower_collateral`, `protocol_event` |
| Lending Positions | Aave V3 RWA | Ethereum | Event log decoding from receipts | PostgreSQL `borrower`, `borrower_collateral`, `protocol_event` |
| Reserve State | All 5 lending protocols | Ethereum | Multicall3 `getReserveData()` + `getReserveConfigurationData()` | PostgreSQL `sparklend_reserve_data` |
| Raw Blocks | Alchemy WS + Erigon RPC | Ethereum, Avalanche | WebSocket (live), HTTP RPC (backfill) | PostgreSQL `block_states` + Redis + S3 |
| Raw Receipts | Alchemy WS + Erigon RPC | Ethereum, Avalanche | WebSocket (live), HTTP RPC (backfill) | PostgreSQL + Redis + S3 |
| Raw Traces | Alchemy WS + Erigon RPC | Ethereum only | WebSocket (live), `trace_block` RPC (backfill) | PostgreSQL + Redis + S3 |
| Raw Blobs | Alchemy WS + Erigon RPC | Ethereum only (post-Dencun) | WebSocket (live), `eth_getBlobSidecars` (backfill) | PostgreSQL + Redis + S3 |
| **TODO:** Positions | Morpho | | | |
| **TODO:** Positions | Maple | | | |
| **TODO:** Positions | Anchorage | | | |
| **TODO:** Positions | Spark | | | |
| **TODO:** Positions | Grove | | | |
| **TODO:** Sky Debt | Spark | | | |
| **TODO:** Sky Debt | Grove | | | |
| **TODO:** Sky Debt | River | | | |
