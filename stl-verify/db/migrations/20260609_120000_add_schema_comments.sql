-- Add COMMENT ON TABLE / COMMENT ON COLUMN metadata for all tables.
-- Based on https://app.notion.com/p/L0-Data-Dictionary-372b87693a5a80ccb227e71d6614211b
-- Consumed by the metadata catalogue. Conventions used in comments:
--   [Type]: Dimension | Configuration | Operational | Hypertable
--   Roles:  PK | FK→table.col | Derived | Audit | Partition
--   Scale:  wad=÷1e18 | ray=÷1e27 | shares=÷1e24 | price=÷1e8

-- =============================================================================
-- Dimension tables
-- =============================================================================

COMMENT ON TABLE chain IS
  '[Dimension] Master list of tracked blockchain networks. Root dimension — every other table with on-chain data references chain_id.';
COMMENT ON COLUMN chain.chain_id IS
  'PK. EVM standard chain ID (meaningful value: 1=Ethereum, 10=Optimism, 130=Unichain, 8453=Base, 42161=Arbitrum, 43114=Avalanche).';
COMMENT ON COLUMN chain.name IS
  'Human-readable label used in dashboards and alerts.';

COMMENT ON TABLE protocol IS
  '[Dimension] One row per deployed smart contract per chain. SparkLend on Ethereum and Aave V3 on Avalanche are separate rows.';
COMMENT ON COLUMN protocol.chain_id IS
  'FK→chain.chain_id. Chain this contract is deployed on.';
COMMENT ON COLUMN protocol.address IS
  'On-chain contract address (20 bytes). Used to filter events from the blockchain node.';
COMMENT ON COLUMN protocol.protocol_type IS
  'Category: "lending" for Aave/SparkLend, "morpho" for Morpho Blue.';
COMMENT ON COLUMN protocol.created_at_block IS
  'Block number when contract was deployed. Allows filtering to the period the protocol was live.';

COMMENT ON TABLE token IS
  '[Dimension] Every ERC-20 token the system tracks. One row per token contract per chain — the same symbol on two chains is two rows.';
COMMENT ON COLUMN token.chain_id IS
  'FK→chain.chain_id.';
COMMENT ON COLUMN token.address IS
  'Token contract address (20 bytes). Used to verify on-chain data and look up prices.';
COMMENT ON COLUMN token.decimals IS
  'ERC-20 decimal places. Critical for scaling raw on-chain balances. Most tokens: 18; USDC: 6.';

COMMENT ON TABLE "user" IS
  '[Dimension] Every on-chain wallet that has interacted with a tracked protocol. One row per address per chain.';
COMMENT ON COLUMN "user".chain_id IS
  'FK→chain.chain_id. Scopes address to a specific chain.';
COMMENT ON COLUMN "user".address IS
  'Wallet address (20 bytes). Used to attribute positions, debt, and collateral to a counterparty.';
COMMENT ON COLUMN "user".first_seen_block IS
  'Block number when this wallet was first observed.';

COMMENT ON TABLE prime IS
  '[Dimension] The three Prime Finance borrowing entities managed by Anchorage (grove, obex, spark). Anchor for all prime-domain data.';
COMMENT ON COLUMN prime.vault_address IS
  'On-chain vault contract address (20 bytes). Used to identify this prime''s on-chain transactions.';

-- =============================================================================
-- Configuration tables
-- =============================================================================

COMMENT ON TABLE morpho_market IS
  '[Configuration] Static configuration of each Morpho Blue lending market. Immutable once deployed on-chain.';
COMMENT ON COLUMN morpho_market.market_id IS
  'On-chain natural key (32 bytes). keccak256 hash of market parameters. Distinct from the surrogate id column.';
COMMENT ON COLUMN morpho_market.loan_token_id IS
  'FK→token.id. Asset that is lent and borrowed in this market.';
COMMENT ON COLUMN morpho_market.collateral_token_id IS
  'FK→token.id. Asset borrowers post as collateral.';
COMMENT ON COLUMN morpho_market.lltv IS
  'Liquidation loan-to-value threshold. Stored as wad (÷1e18 → ratio). Example: 0.915e18 = 91.5%.';
COMMENT ON COLUMN morpho_market.created_at_block IS
  'Block when the market was deployed. Defines start of this market''s data history.';

COMMENT ON TABLE morpho_vault IS
  '[Configuration] MetaMorpho ERC-4626 vaults that accept deposits and allocate capital across Morpho Blue markets.';
COMMENT ON COLUMN morpho_vault.address IS
  'Vault ERC-4626 contract address (20 bytes).';
COMMENT ON COLUMN morpho_vault.asset_token_id IS
  'FK→token.id. Underlying token this vault accepts for deposit.';
COMMENT ON COLUMN morpho_vault.vault_version IS
  'MetaMorpho contract version (1, 1.1, or 2). Affects asset and fee calculation behaviour.';

COMMENT ON TABLE receipt_token IS
  '[Configuration] Maps each aToken (interest-bearing receipt issued to lenders in SparkLend/Aave) to its underlying asset.';
COMMENT ON COLUMN receipt_token.underlying_token_id IS
  'FK→token.id. Actual deposited asset (e.g. DAI for spDAI).';
COMMENT ON COLUMN receipt_token.receipt_token_address IS
  'aToken contract address (20 bytes). Used to track Transfer events.';

COMMENT ON TABLE debt_token IS
  '[Configuration] Maps variable and stable debt tokens to their underlying asset. Currently empty — pending activation.';

COMMENT ON TABLE oracle IS
  '[Configuration] One row per price oracle contract (Aave oracle, Chainlink, Chronicle, Redstone).';
COMMENT ON COLUMN oracle.oracle_type IS
  'Determines ABI method called when reading prices: aave_oracle | chainlink | chronicle | redstone.';
COMMENT ON COLUMN oracle.price_decimals IS
  'Decimal precision of prices returned by this oracle. Default 8.';
COMMENT ON COLUMN oracle.enabled IS
  'Set to false to disable polling without deleting the record.';

COMMENT ON TABLE oracle_asset IS
  '[Configuration] Junction table: which tokens each oracle is configured to price. One row per (oracle, token) pair.';
COMMENT ON COLUMN oracle_asset.oracle_id IS
  'FK→oracle.id (app-only — no DB constraint).';
COMMENT ON COLUMN oracle_asset.token_id IS
  'FK→token.id (app-only — no DB constraint).';
COMMENT ON COLUMN oracle_asset.feed_address IS
  'Specific Chainlink feed contract for this token, when different from the oracle''s root contract (20 bytes). Null for Aave-style oracles.';

COMMENT ON TABLE protocol_oracle IS
  '[Configuration] Assigns which oracle a protocol uses per block range. New row on each oracle upgrade; from_block determines which row is active.';
COMMENT ON COLUMN protocol_oracle.from_block IS
  'Block from which this oracle assignment is valid. Active row = highest from_block ≤ query block.';

COMMENT ON TABLE offchain_price_source IS
  '[Configuration] Off-chain price data providers (currently CoinGecko only).';

COMMENT ON TABLE offchain_price_asset IS
  '[Configuration] Maps tracked tokens to their identifiers in off-chain price providers (e.g. CoinGecko slug).';
COMMENT ON COLUMN offchain_price_asset.source_asset_id IS
  'Provider''s own identifier (e.g. "wrapped-steth" for wstETH on CoinGecko).';

-- =============================================================================
-- Operational tables
-- =============================================================================

COMMENT ON TABLE build_registry IS
  '[Operational] Audit log of every production code deployment. All hypertable rows carry build_id pointing here.';
COMMENT ON COLUMN build_registry.git_hash IS
  'Full Git commit SHA. Traces any data row to the exact commit that produced it.';
COMMENT ON COLUMN build_registry.docker_sha IS
  'Docker image digest. Currently always null — tracing stops at git commit level.';

COMMENT ON TABLE migrations IS
  '[Operational] Records every SQL migration applied, with checksum. Migrator rejects any file whose checksum has changed after application.';
COMMENT ON COLUMN migrations.checksum IS
  'Hash of migration file contents. Mismatch at startup means the file was modified after being applied.';

COMMENT ON TABLE backfill_watermark IS
  '[Operational] Highest block fully backfilled per chain. One row per chain. Used by backfill service to resume after gaps.';
COMMENT ON COLUMN backfill_watermark.watermark IS
  'Highest block number confirmed as filled. Backfill scans between watermark and chain tip for holes.';

COMMENT ON TABLE reorg_events IS
  '[Operational] Audit log of every chain reorganisation detected by the watcher.';
COMMENT ON COLUMN reorg_events.block_number IS
  'Block height where the reorg occurred.';
COMMENT ON COLUMN reorg_events.depth IS
  'Number of blocks rolled back. Arbitrum reorgs up to depth 35 are expected.';

-- =============================================================================
-- Raw ingestion hypertables
-- =============================================================================

COMMENT ON TABLE block_states IS
  '[Hypertable] Every block header received from the blockchain node. Pipeline heartbeat — written before any event parsing. Partition key: created_at (daily) + chain_id.';
COMMENT ON COLUMN block_states.number IS
  'Block height. Named "number" here; all other tables use "block_number" for the same concept.';
COMMENT ON COLUMN block_states.version IS
  'Reorg counter. Named "version" here; all other tables use "block_version". Canonical row = highest version where is_orphaned = false.';
COMMENT ON COLUMN block_states.is_orphaned IS
  'True if this block was replaced by a reorg. Orphaned rows must be ignored.';
COMMENT ON COLUMN block_states.block_published IS
  'True once this block has been published downstream. A single SQS event carries all block data (block, receipts, traces, blobs).';
COMMENT ON COLUMN block_states.received_at IS
  'Unix epoch. Approximate time the block arrived. Type should be TIMESTAMPTZ (known inconsistency).';

COMMENT ON TABLE protocol_event IS
  '[Hypertable] Every decoded on-chain event log from tracked protocol contracts. Canonical raw archive — all position and state tables are derived from this. Partition key: created_at.';
COMMENT ON COLUMN protocol_event.block_number IS
  'Block this event was included in.';
COMMENT ON COLUMN protocol_event.block_version IS
  'Reorg counter. Canonical event = highest block_version at this block_number.';
COMMENT ON COLUMN protocol_event.log_index IS
  'Position of this log within the transaction. Required to uniquely identify an event when a single tx emits multiple logs.';
COMMENT ON COLUMN protocol_event.event_name IS
  'Decoded event name (e.g. Supply, Borrow, Repay, LiquidationCall, CreateMarket). Used by downstream parsers to route to the correct domain table.';
COMMENT ON COLUMN protocol_event.event_data IS
  'Full decoded event payload as JSONB. Downstream parsers extract specific fields (e.g. event_data->>''amount'').';
COMMENT ON COLUMN protocol_event.processing_version IS
  'Audit. Increments when the event is reprocessed by a newer parser build.';
COMMENT ON COLUMN protocol_event.build_id IS
  'Audit. FK→build_registry.id (advisory). Traces row to the code version that produced it.';

-- =============================================================================
-- Lending position tables
-- =============================================================================

COMMENT ON TABLE borrower IS
  '[Hypertable] Append-only ledger of lending events per user (Supply, Borrow, Repay, Withdraw, Liquidation). Despite the name, tracks ALL lending activity including supplies. Partition key: created_at.';
COMMENT ON COLUMN borrower.amount IS
  'Derived. Running balance in wad (÷1e18). Cumulative sum of all prior change values for this (user, protocol, token). Latest position = row with max block_number.';
COMMENT ON COLUMN borrower.change IS
  'Signed delta in wad. Positive = inflow (supply/repay). Negative = outflow (borrow/withdraw).';
COMMENT ON COLUMN borrower.event_type IS
  'One of: Supply | Withdraw | Borrow | Repay | LiquidationCall | ReserveUsedAsCollateralEnabled | ReserveUsedAsCollateralDisabled | Snapshot.';
COMMENT ON COLUMN borrower.block_version IS
  'Reorg counter. Canonical row = highest block_version at this block_number.';

COMMENT ON TABLE borrower_collateral IS
  '[Hypertable] Primary lending position ledger — all supply, collateral, and liquidation events per user. One row per event. Partition key: created_at.';
COMMENT ON COLUMN borrower_collateral.amount IS
  'Raw token amount in wad (÷1e18). Collateral position size at this event.';
COMMENT ON COLUMN borrower_collateral.change IS
  'Signed delta in wad.';
COMMENT ON COLUMN borrower_collateral.collateral_enabled IS
  'True if this token is enabled as collateral for borrowing. If false, user cannot borrow against this position even with a balance.';

COMMENT ON TABLE sparklend_reserve_data IS
  '[Hypertable] Per-block snapshot of aggregate state for each lending reserve (SparkLend/Aave). Emitted by ReserveDataUpdated events. liquidity_index is used to compute scaled balances in allocation_position and token_total_supply. Partition key: block_number.';
COMMENT ON COLUMN sparklend_reserve_data.id IS
  'Identifies the reserve entity but is NOT unique per row — multiple rows exist per id (one per block). Always filter with block_number.';
COMMENT ON COLUMN sparklend_reserve_data.total_a_token IS
  'Wad (÷1e18). Total aToken supply = total liquidity supplied to this reserve.';
COMMENT ON COLUMN sparklend_reserve_data.total_variable_debt IS
  'Wad (÷1e18). Total variable-rate debt outstanding.';
COMMENT ON COLUMN sparklend_reserve_data.liquidity_rate IS
  'Ray (÷1e27). Current annual supply yield (APY).';
COMMENT ON COLUMN sparklend_reserve_data.variable_borrow_rate IS
  'Ray (÷1e27). Current annual borrowing cost (APY).';
COMMENT ON COLUMN sparklend_reserve_data.liquidity_index IS
  'Ray (÷1e27). Cumulative interest factor since reserve creation. Monotonically increasing. Divide a raw balance by this for inflation-adjusted (scaled) balance.';
COMMENT ON COLUMN sparklend_reserve_data.last_update_timestamp IS
  'Unix epoch. WARNING: 5.9% of values are corrupt due to int64 overflow of the source uint40 value.';
COMMENT ON COLUMN sparklend_reserve_data.ltv IS
  'Maximum LTV for borrowing against this token as collateral. Stored in basis points (7500 = 75%).';
COMMENT ON COLUMN sparklend_reserve_data.liquidation_threshold IS
  'LTV at which positions become liquidatable. Stored in basis points.';

-- =============================================================================
-- Morpho Blue tables
-- =============================================================================

COMMENT ON TABLE morpho_market_state IS
  '[Hypertable] Per-block aggregate snapshot of each Morpho Blue market (supply, borrow, shares, fee). Partition key: timestamp (on-chain block time).';
COMMENT ON COLUMN morpho_market_state.total_supply_assets IS
  'Wad (÷1e18). Total assets supplied to this market.';
COMMENT ON COLUMN morpho_market_state.total_supply_shares IS
  'Morpho shares (÷1e24). Must equal sum of all user supply_shares in morpho_market_position.';
COMMENT ON COLUMN morpho_market_state.total_borrow_assets IS
  'Wad (÷1e18). Total assets borrowed. Equals total_supply_assets at 100% utilisation.';
COMMENT ON COLUMN morpho_market_state.total_borrow_shares IS
  'Morpho shares (÷1e24). Must equal sum of all user borrow_shares in morpho_market_position.';
COMMENT ON COLUMN morpho_market_state.fee IS
  'Wad. Protocol fee rate. Share of interest going to Morpho fee recipient rather than lenders.';
COMMENT ON COLUMN morpho_market_state.timestamp IS
  'Partition key. On-chain block timestamp (event time).';

COMMENT ON TABLE morpho_market_position IS
  '[Hypertable] Per-user, per-market snapshot at each block where the user interacted. supply_assets and borrow_assets are derived from shares at write time. Partition key: timestamp.';
COMMENT ON COLUMN morpho_market_position.supply_shares IS
  'Morpho shares (÷1e24). User''s proportional claim on the supply pool. Use shares for conservation checks.';
COMMENT ON COLUMN morpho_market_position.borrow_shares IS
  'Morpho shares (÷1e24). User''s proportional share of the borrow pool.';
COMMENT ON COLUMN morpho_market_position.collateral IS
  'Wad (÷1e18). Collateral deposited by this user.';
COMMENT ON COLUMN morpho_market_position.supply_assets IS
  'Derived. Wad (÷1e18). supply_shares × (total_supply_assets / total_supply_shares) at this block. Use shares for conservation checks; assets are an approximation.';
COMMENT ON COLUMN morpho_market_position.borrow_assets IS
  'Derived. Wad (÷1e18). borrow_shares × (total_borrow_assets / total_borrow_shares) at this block. Same approximation caveat as supply_assets.';
COMMENT ON COLUMN morpho_market_position.timestamp IS
  'Partition key. On-chain event time.';

COMMENT ON TABLE morpho_vault_state IS
  '[Hypertable] Per-block aggregate snapshot of each MetaMorpho vault (total assets, total shares). Partition key: timestamp.';
COMMENT ON COLUMN morpho_vault_state.total_assets IS
  'Wad (÷1e18). Total assets under management = NAV of the vault.';
COMMENT ON COLUMN morpho_vault_state.total_shares IS
  'Vault shares. Must equal sum of all user shares in morpho_vault_position.';

COMMENT ON TABLE morpho_vault_position IS
  '[Hypertable] Per-user, per-vault snapshot at each block where the user interacted. assets is derived from shares at write time. Partition key: timestamp.';
COMMENT ON COLUMN morpho_vault_position.shares IS
  'Vault share units. User''s proportional ownership of the vault. Use for conservation checks.';
COMMENT ON COLUMN morpho_vault_position.assets IS
  'Derived. Wad (÷1e18). shares × (total_assets / total_shares) at this block.';
COMMENT ON COLUMN morpho_vault_position.timestamp IS
  'Partition key. On-chain event time.';

-- =============================================================================
-- Prime financial tables
-- =============================================================================

COMMENT ON TABLE prime_debt IS
  '[Hypertable] Per-block record of each Prime''s outstanding MakerDAO debt per ilk. Polled every 15 minutes. Partition key: synced_at.';
COMMENT ON COLUMN prime_debt.ilk_name IS
  'MakerDAO vault type identifier (e.g. ALLOCATOR-SPARK-A). Different ilks may have different stability fees.';
COMMENT ON COLUMN prime_debt.debt_wad IS
  'Derived. Wad (÷1e18). Computed as art × rate / 1e27 from MakerDAO Vat contract reads. Total outstanding debt in DAI-equivalent terms.';
COMMENT ON COLUMN prime_debt.synced_at IS
  'Partition key. On-chain block timestamp when debt was read (event time, despite the name suggesting processing time).';

COMMENT ON TABLE allocation_position IS
  '[Hypertable] Every token movement into/out of each Prime''s proxy contract (deposit, withdrawal, sweep). balance column currently unpopulated (always 0). Partition key: created_at.';
COMMENT ON COLUMN allocation_position.proxy_address IS
  'Specific proxy contract holding this prime''s tokens (20 bytes). Part of PK — a prime may have multiple proxies.';
COMMENT ON COLUMN allocation_position.balance IS
  'Derived. Wad (÷1e18). Running balance after this transaction. Currently always 0 — pipeline gap.';
COMMENT ON COLUMN allocation_position.scaled_balance IS
  'Derived. Nullable. Wad. balance / liquidity_index. Removes interest accrual from nominal balance. Only applicable for aTokens.';
COMMENT ON COLUMN allocation_position.tx_amount IS
  'Wad. Signed amount of this transaction. Positive = inflow; negative = outflow.';
COMMENT ON COLUMN allocation_position.direction IS
  'One of: in (deposit) | out (withdrawal) | sweep (internal reallocation, tx_amount = 0).';

COMMENT ON TABLE anchorage_package_snapshot IS
  '[Hypertable] Every 15-minute poll of Anchorage custody package state (collateral, loan, LTV). Primary source for margin call monitoring. Partition key: snapshot_time.';
COMMENT ON COLUMN anchorage_package_snapshot.package_id IS
  'Anchorage''s own string identifier. Part of PK.';
COMMENT ON COLUMN anchorage_package_snapshot.current_ltv IS
  'Derived. exposure_value / package_value. Key risk metric — compared against margin_call_ltv and critical_ltv.';
COMMENT ON COLUMN anchorage_package_snapshot.state IS
  'Package lifecycle state (e.g. HEALTHY, MARGIN_CALL_M1, CRITICAL). Changes when Anchorage determines a threshold is breached.';
COMMENT ON COLUMN anchorage_package_snapshot.margin_call_ltv IS
  'LTV threshold that triggers a margin call. When current_ltv ≥ this value, prime must post more collateral or reduce debt.';
COMMENT ON COLUMN anchorage_package_snapshot.critical_ltv IS
  'LTV threshold at which forced liquidation may begin.';
COMMENT ON COLUMN anchorage_package_snapshot.snapshot_time IS
  'Partition key. When our poller retrieved this snapshot.';
COMMENT ON COLUMN anchorage_package_snapshot.ltv_timestamp IS
  'When Anchorage computed the LTV. May differ from snapshot_time by API latency.';

COMMENT ON TABLE anchorage_operation IS
  '[Hypertable] Audit trail of individual custody operations (deposit, withdrawal, transfer) through Anchorage. Partition key: created_at (Anchorage event time — exception to the standard created_at semantics).';
COMMENT ON COLUMN anchorage_operation.created_at IS
  'Partition key. Anchorage event time — when Anchorage recorded the operation. NOT pipeline write time. This is the only table where created_at has this meaning.';
COMMENT ON COLUMN anchorage_operation.action IS
  'High-level action: DEPOSIT | WITHDRAWAL | TRANSFER.';

-- =============================================================================
-- Price and supply tables
-- =============================================================================

COMMENT ON TABLE onchain_token_price IS
  '[Hypertable] Oracle price reads per token per block (Aave oracle, Chainlink, Chronicle, Redstone). Written only when price moves beyond threshold — absence means price is stable. Partition key: timestamp.';
COMMENT ON COLUMN onchain_token_price.oracle_id IS
  'FK→oracle.id (app-only). Type is SMALLINT instead of BIGINT — known inconsistency.';
COMMENT ON COLUMN onchain_token_price.block_version IS
  'Reorg counter. Type is SMALLINT instead of INT — known inconsistency.';
COMMENT ON COLUMN onchain_token_price.price_usd IS
  'USD price with 8 decimal precision (÷1e8). Zero or implausible value here corrupts all downstream calculations.';
COMMENT ON COLUMN onchain_token_price.timestamp IS
  'Partition key. On-chain block time.';

COMMENT ON TABLE offchain_token_price IS
  '[Hypertable] API-polled token prices from off-chain providers (CoinGecko). Polled ~hourly. Used for cross-validation of on-chain oracle prices. Partition key: timestamp.';
COMMENT ON COLUMN offchain_token_price.source_id IS
  'FK→offchain_price_source.id (app-only). Type is SMALLINT instead of BIGINT — known inconsistency.';
COMMENT ON COLUMN offchain_token_price.price_usd IS
  'USD price from off-chain provider. Divergence >5% from onchain_token_price signals a potentially stale oracle.';
COMMENT ON COLUMN offchain_token_price.volume_usd IS
  '24h trading volume. Currently always null — not populated by the CoinGecko integration.';
COMMENT ON COLUMN offchain_token_price.timestamp IS
  'Partition key. API observation time.';

COMMENT ON TABLE token_total_supply IS
  '[Hypertable] Per-block total circulating supply for tracked tokens. Written from Transfer events (exact) or periodic balance sweeps (approximate). Partition key: block_timestamp.';
COMMENT ON COLUMN token_total_supply.total_supply IS
  'Wad (raw on-chain value in the token''s native decimals). Total circulating supply.';
COMMENT ON COLUMN token_total_supply.scaled_total_supply IS
  'Derived. Nullable. total_supply / liquidity_index. Removes accrued interest from aToken supplies. Only populated for aTokens.';
COMMENT ON COLUMN token_total_supply.source IS
  'event = derived from a Transfer log (exact). sweep = derived from a contract read (approximate).';
COMMENT ON COLUMN token_total_supply.block_timestamp IS
  'Partition key. On-chain block time.';

INSERT INTO migrations (filename)
VALUES ('20260609_120000_add_schema_comments.sql')
ON CONFLICT (filename) DO NOTHING;