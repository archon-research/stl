-- Backfill COMMENT ON metadata for the Maple table family.
-- The maple_* tables were created after 20260609_120000_add_schema_comments
-- (the original catalogue backfill), so they were never documented. This adds
-- the missing COMMENTs, consistent with that migration's conventions:
--   [Type]: Dimension | Hypertable | View
--   Roles:  PK | FK→table.col | Derived | Partition | Audit
--   Scale:  raw amounts are on-chain/API integers in the named asset's native
--           decimals (USDC/USDT = 6); ratios are fixed-point ×1e6 (6 decimals);
--           APYs are ×1e30 (30 decimals); per-unit USD prices are ×1e8.
-- Reflects the post-20260627 schema (SCD2 hub + satellite split): editorial
-- columns live in the *_meta satellites, not the hubs. The fixed-term-loan
-- tables (maple_ftl_loan / maple_ftl_loan_state) are commented in their own
-- migration (20260619) and are intentionally not repeated here.
--
-- Comment-only migration: no DDL on table structure, safe and idempotent
-- (COMMENT ON overwrites).

-- ============================================================================
-- Registry hubs (identity only; editorial attributes moved to *_meta)
-- ============================================================================
COMMENT ON TABLE maple_pool IS
  '[Dimension] Registry of Maple PoolV2 lending pools, discovered dynamically per sync. Identity hub; mutable editorial attributes (name, is_syrup) live in the maple_pool_meta SCD2 satellite. Use the maple_pool_current view for the flat current shape.';
COMMENT ON COLUMN maple_pool.id IS 'PK. Surrogate id; every FK to a pool references this.';
COMMENT ON COLUMN maple_pool.chain_id IS 'FK→chain.chain_id.';
COMMENT ON COLUMN maple_pool.protocol_id IS 'FK→protocol.id. The Maple protocol row.';
COMMENT ON COLUMN maple_pool.address IS 'Pool contract address (20 bytes); poolV2.id in the Maple API. Unique per (chain_id, address).';
COMMENT ON COLUMN maple_pool.asset_token_id IS 'FK→token.id. The pool''s underlying ERC-20 asset (poolV2.asset, e.g. USDC/USDT).';
COMMENT ON COLUMN maple_pool.created_at IS 'Audit. Wall-clock time the registry row was first inserted.';

COMMENT ON TABLE maple_loan IS
  '[Dimension] Registry of Maple Open Term Loans (OTLs), one row per loan contract. Identity hub; editorial attributes (loan_type, loanMeta fields) live in the maple_loan_meta SCD2 satellite. Fixed-term loans use the separate maple_ftl_loan table. Use maple_loan_current for the flat current shape.';
COMMENT ON COLUMN maple_loan.id IS 'PK. Surrogate id referenced by maple_loan_state, maple_loan_collateral, and maple_loan_meta.';
COMMENT ON COLUMN maple_loan.chain_id IS 'FK→chain.chain_id.';
COMMENT ON COLUMN maple_loan.protocol_id IS 'FK→protocol.id. The Maple protocol row.';
COMMENT ON COLUMN maple_loan.loan_address IS 'OTL loan contract address (20 bytes); openTermLoan.id in the Maple API. Unique per (chain_id, loan_address).';
COMMENT ON COLUMN maple_loan.maple_pool_id IS 'FK→maple_pool.id. The funding pool.';
COMMENT ON COLUMN maple_loan.borrower_user_id IS 'FK→user.id. The borrower (borrower.id).';
COMMENT ON COLUMN maple_loan.first_seen_at IS 'Audit. Wall-clock time the registry row was first inserted.';

COMMENT ON TABLE maple_sky_strategy IS
  '[Dimension] Registry of Sky strategies (internal pool-asset deployments into Sky/Maker yield). Identity hub; the mutable version lives in the maple_sky_strategy_meta SCD2 satellite. Use maple_sky_strategy_current for the flat current shape.';
COMMENT ON COLUMN maple_sky_strategy.id IS 'PK. Surrogate id referenced by maple_sky_strategy_state and maple_sky_strategy_meta.';
COMMENT ON COLUMN maple_sky_strategy.chain_id IS 'FK→chain.chain_id.';
COMMENT ON COLUMN maple_sky_strategy.strategy_address IS 'Strategy contract address (20 bytes); skyStrategy.id in the Maple API. Unique per (chain_id, strategy_address).';
COMMENT ON COLUMN maple_sky_strategy.maple_pool_id IS 'FK→maple_pool.id. The pool whose cash this strategy deploys.';
COMMENT ON COLUMN maple_sky_strategy.created_at IS 'Audit. Wall-clock time the registry row was first inserted.';

-- ============================================================================
-- SCD2 editorial satellites (append-on-change, keyed (hub_id, synced_at))
-- ============================================================================
COMMENT ON TABLE maple_pool_meta IS
  '[Dimension] SCD2 satellite of maple_pool editorial attributes. Append-only: a change appends a row keyed (maple_pool_id, synced_at); the hub id never moves. UPDATE/DELETE revoked from the app role.';
COMMENT ON COLUMN maple_pool_meta.maple_pool_id IS 'FK→maple_pool.id. Part of PK.';
COMMENT ON COLUMN maple_pool_meta.synced_at IS 'PK. Cron-cycle timestamp (UTC) this editorial version was first observed (SCD2 effective-from).';
COMMENT ON COLUMN maple_pool_meta.name IS 'Pool display name (poolV2.name); nullable.';
COMMENT ON COLUMN maple_pool_meta.is_syrup IS 'Derived. TRUE when poolV2.syrupRouter is set (a Syrup pool).';
COMMENT ON COLUMN maple_pool_meta.hashdiff IS 'md5 over the canonical editorial-field encoding; drives append-on-change (see migration 20260627). Not security.';
COMMENT ON COLUMN maple_pool_meta.is_present IS 'SCD2 tombstone: FALSE when the entity was absent from the latest sync. The *_current views drop entities whose latest row is a tombstone.';
COMMENT ON COLUMN maple_pool_meta.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';

COMMENT ON TABLE maple_loan_meta IS
  '[Dimension] SCD2 satellite of maple_loan editorial attributes (loan_type + loanMeta). Append-only, keyed (maple_loan_id, synced_at). UPDATE/DELETE revoked from the app role.';
COMMENT ON COLUMN maple_loan_meta.maple_loan_id IS 'FK→maple_loan.id. Part of PK.';
COMMENT ON COLUMN maple_loan_meta.synced_at IS 'PK. Cron-cycle timestamp (UTC) this editorial version was first observed (SCD2 effective-from).';
COMMENT ON COLUMN maple_loan_meta.loan_type IS 'Loan-family discriminator; ''OTL'' for open-term loans (fixed-term loans live in maple_ftl_loan, not here).';
COMMENT ON COLUMN maple_loan_meta.loan_meta_type IS 'loanMeta.type; open set (null | ''amm'' | ''strategy'' | ''tBills'' | ''intercompany'' | ...). ''amm''/''strategy'' mark an internal Maple position (is_internal in the view).';
COMMENT ON COLUMN maple_loan_meta.loan_meta_asset_symbol IS 'loanMeta.assetSymbol; nullable.';
COMMENT ON COLUMN maple_loan_meta.loan_meta_dex IS 'loanMeta.dex; nullable.';
COMMENT ON COLUMN maple_loan_meta.loan_meta_wallet_address IS 'loanMeta.walletAddress; nullable. May be non-EVM (BASE/SOL custody wallets), so stored as text, not bytea.';
COMMENT ON COLUMN maple_loan_meta.loan_meta_wallet_type IS 'loanMeta.walletType; nullable.';
COMMENT ON COLUMN maple_loan_meta.loan_meta_location IS 'loanMeta.location; nullable.';
COMMENT ON COLUMN maple_loan_meta.hashdiff IS 'md5 over the canonical editorial-field encoding (NULL rendered as 0x1e, fields joined by 0x1f); drives append-on-change. Not security.';
COMMENT ON COLUMN maple_loan_meta.is_present IS 'SCD2 tombstone: FALSE when the entity was absent from the latest sync.';
COMMENT ON COLUMN maple_loan_meta.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';

COMMENT ON TABLE maple_sky_strategy_meta IS
  '[Dimension] SCD2 satellite of maple_sky_strategy editorial attributes. Append-only, keyed (maple_sky_strategy_id, synced_at). UPDATE/DELETE revoked from the app role.';
COMMENT ON COLUMN maple_sky_strategy_meta.maple_sky_strategy_id IS 'FK→maple_sky_strategy.id. Part of PK.';
COMMENT ON COLUMN maple_sky_strategy_meta.synced_at IS 'PK. Cron-cycle timestamp (UTC) this editorial version was first observed (SCD2 effective-from).';
COMMENT ON COLUMN maple_sky_strategy_meta.version IS 'skyStrategy.version; nullable. Arrives from the API as a JSON number (e.g. 100), not a string.';
COMMENT ON COLUMN maple_sky_strategy_meta.hashdiff IS 'md5 over the canonical editorial-field encoding; drives append-on-change. Not security.';
COMMENT ON COLUMN maple_sky_strategy_meta.is_present IS 'SCD2 tombstone: FALSE when the entity was absent from the latest sync.';
COMMENT ON COLUMN maple_sky_strategy_meta.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';

-- ============================================================================
-- State hypertables (per-cycle snapshots, partitioned on synced_at)
-- ============================================================================
COMMENT ON TABLE maple_pool_state IS
  '[Hypertable] Per-cycle snapshot of each PoolV2 pool''s lending metrics, partitioned on synced_at. No block_version: GraphQL data has no reorg concept.';
COMMENT ON COLUMN maple_pool_state.maple_pool_id IS 'FK→maple_pool.id. Part of PK.';
COMMENT ON COLUMN maple_pool_state.synced_at IS 'Partition. Cron-cycle timestamp (UTC), shared by every row of one sync cycle. Part of PK.';
COMMENT ON COLUMN maple_pool_state.tvl IS 'Total value locked, raw integer in pool-asset native decimals (USDC/USDT = 6). Nullable in the API schema.';
COMMENT ON COLUMN maple_pool_state.liquid_assets IS 'Liquid pool cash (poolV2.assets), raw integer in pool-asset native decimals.';
COMMENT ON COLUMN maple_pool_state.collateral_value_usd IS 'USD value of loan collateral, raw integer in pool-asset native decimals. Nullable in the API schema.';
COMMENT ON COLUMN maple_pool_state.principal_out IS 'Outstanding loan principal, raw integer in pool-asset native decimals.';
COMMENT ON COLUMN maple_pool_state.utilization IS 'Derived fraction 0–1: principal_out / (liquid_assets + principal_out); 0 when the pool is empty. Not scaled.';
COMMENT ON COLUMN maple_pool_state.monthly_apy IS '30-day historical average APY, fixed-point ×1e30 (30 decimals). Nullable.';
COMMENT ON COLUMN maple_pool_state.spot_apy IS 'Current spot APY, fixed-point ×1e30 (30 decimals). Nullable.';
COMMENT ON COLUMN maple_pool_state.processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by synced_at DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN maple_pool_state.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';

COMMENT ON TABLE maple_loan_state IS
  '[Hypertable] Per-cycle snapshot of an Open Term Loan, partitioned on synced_at. Only Active loans are queried, so a loan absent at a given synced_at is no longer active.';
COMMENT ON COLUMN maple_loan_state.maple_loan_id IS 'FK→maple_loan.id. Part of PK.';
COMMENT ON COLUMN maple_loan_state.synced_at IS 'Partition. Cron-cycle timestamp (UTC). Part of PK.';
COMMENT ON COLUMN maple_loan_state.state IS 'LoanState enum; currently only ''Active'' is queried.';
COMMENT ON COLUMN maple_loan_state.principal_owed IS 'Outstanding principal, raw integer in pool-asset native decimals (USDC/USDT = 6).';
COMMENT ON COLUMN maple_loan_state.acm_ratio IS 'Asset Coverage Margin ratio, fixed-point ×1e6 (6 decimals; 1445731 = 144.57%). NULL on uncollateralized loans.';
COMMENT ON COLUMN maple_loan_state.processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by synced_at DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN maple_loan_state.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';

COMMENT ON TABLE maple_loan_collateral IS
  '[Hypertable] Per-cycle snapshot of an Open Term Loan''s collateral, partitioned on synced_at. One row per loan per cycle; loans with null API collateral have no row. Collateral is off-chain custodied (BTC/SOL), so it is stored by symbol, not a token FK.';
COMMENT ON COLUMN maple_loan_collateral.maple_loan_id IS 'FK→maple_loan.id. Part of PK.';
COMMENT ON COLUMN maple_loan_collateral.synced_at IS 'Partition. Cron-cycle timestamp (UTC). Part of PK.';
COMMENT ON COLUMN maple_loan_collateral.asset_symbol IS 'Collateral asset symbol (e.g. BTC, SOL); off-chain custodied, no Ethereum address.';
COMMENT ON COLUMN maple_loan_collateral.asset_amount IS 'Collateral amount, raw integer in asset_decimals native decimals. NULL when the API reports null (e.g. DepositPending).';
COMMENT ON COLUMN maple_loan_collateral.asset_decimals IS 'Native decimals of the collateral asset; use to scale asset_amount.';
COMMENT ON COLUMN maple_loan_collateral.asset_value_usd IS 'Per-unit USD price of the collateral asset, fixed-point ×1e8 (8 decimals) — NOT a total. Multiply by asset_amount (scaled) for total USD. NULL when the API reports null.';
COMMENT ON COLUMN maple_loan_collateral.state IS 'Collateral state (e.g. ''Deposited'' | ''DepositPending''); nullable.';
COMMENT ON COLUMN maple_loan_collateral.custodian IS 'Custodian name (e.g. ''FORDEFI'', ''ANCHORAGE''); nullable.';
COMMENT ON COLUMN maple_loan_collateral.liquidation_level IS 'Liquidation level from the API (arrives as a JSON number, e.g. 900000); nullable.';
COMMENT ON COLUMN maple_loan_collateral.processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by synced_at DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN maple_loan_collateral.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';

COMMENT ON TABLE maple_sky_strategy_state IS
  '[Hypertable] Per-cycle snapshot of a Sky strategy''s deployment metrics, partitioned on synced_at.';
COMMENT ON COLUMN maple_sky_strategy_state.maple_sky_strategy_id IS 'FK→maple_sky_strategy.id. Part of PK.';
COMMENT ON COLUMN maple_sky_strategy_state.synced_at IS 'Partition. Cron-cycle timestamp (UTC). Part of PK.';
COMMENT ON COLUMN maple_sky_strategy_state.state IS 'Strategy state string.';
COMMENT ON COLUMN maple_sky_strategy_state.currently_deployed IS 'Amount currently deployed, raw integer in pool-asset native decimals. 0 across all strategies as of 2026-06-15 (dormant).';
COMMENT ON COLUMN maple_sky_strategy_state.deposited_assets IS 'Cumulative assets deposited, raw integer in pool-asset native decimals.';
COMMENT ON COLUMN maple_sky_strategy_state.withdrawn_assets IS 'Cumulative assets withdrawn, raw integer in pool-asset native decimals.';
COMMENT ON COLUMN maple_sky_strategy_state.strategy_fee_rate IS 'Strategy fee rate, fixed-point ×1e6 (6 decimals; observed 100000); nullable.';
COMMENT ON COLUMN maple_sky_strategy_state.total_fees_collected IS 'Cumulative fees collected, raw integer in pool-asset native decimals; nullable.';
COMMENT ON COLUMN maple_sky_strategy_state.processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by synced_at DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN maple_sky_strategy_state.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';

COMMENT ON TABLE maple_syrup_global_state IS
  '[Hypertable] Per-cycle snapshot of Syrup protocol-wide aggregates, partitioned on synced_at. Keyed by chain_id (one Syrup aggregate per chain per cycle).';
COMMENT ON COLUMN maple_syrup_global_state.chain_id IS 'FK→chain.chain_id. Part of PK.';
COMMENT ON COLUMN maple_syrup_global_state.synced_at IS 'Partition. Cron-cycle timestamp (UTC). Part of PK.';
COMMENT ON COLUMN maple_syrup_global_state.tvl IS 'Syrup-wide total value locked, raw integer in pool-asset native decimals.';
COMMENT ON COLUMN maple_syrup_global_state.apy IS 'Overall Syrup APY, fixed-point ×1e30 (30 decimals).';
COMMENT ON COLUMN maple_syrup_global_state.collateral_apy IS 'APY contribution from collateral, fixed-point ×1e30 (30 decimals).';
COMMENT ON COLUMN maple_syrup_global_state.pool_apy IS 'APY contribution from pool lending, fixed-point ×1e30 (30 decimals).';
COMMENT ON COLUMN maple_syrup_global_state.drips_yield_boost IS 'Additional yield from DRIPS; nullable. Scale unconfirmed (pending baseline, ORB-145) — verify before computing.';
COMMENT ON COLUMN maple_syrup_global_state.processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by synced_at DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN maple_syrup_global_state.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';

-- ============================================================================
-- Convenience views (hub identity + latest live satellite row)
-- ============================================================================
COMMENT ON VIEW maple_pool_current IS
  '[View] maple_pool joined to its latest maple_pool_meta row, dropping entities whose latest satellite row is a tombstone (is_present = FALSE). Flat current shape for external SQL consumers (Grafana, analysts).';
COMMENT ON VIEW maple_loan_current IS
  '[View] maple_loan joined to its latest maple_loan_meta row (tombstones dropped). is_internal is derived from the current loan_meta_type (''amm''/''strategy''). Flat current shape for external consumers.';
COMMENT ON VIEW maple_sky_strategy_current IS
  '[View] maple_sky_strategy joined to its latest maple_sky_strategy_meta row (tombstones dropped). Flat current shape for external consumers.';

INSERT INTO migrations (filename)
VALUES ('20260630_120000_add_maple_schema_comments.sql')
ON CONFLICT (filename) DO NOTHING;
