-- Catalogue metadata for cex_orderbook_snapshots and psm3_reserves.
-- Both tables were created after 20260609_120000_add_schema_comments.sql (the
-- original catalogue backfill), so they were never documented. Follows that
-- migration's conventions:
--   [Type]: Dimension | Configuration | Operational | Hypertable
--   Roles:  PK | FK→table.col | Derived | Partition | Audit
--   Scale:  amounts are raw on-chain integers in their native decimals, stated
--           per column. psm3_reserves mixes three scales (1e6, 1e18, 1e27).
-- These comments state invariants only: no point-in-time values (addresses that
-- are governance-settable, rate ranges, venue/chain lists), because nothing
-- refreshes a COMMENT when they change.
-- Comment-only migration: no DDL on table structure, safe and idempotent
-- (COMMENT ON overwrites).

-- ===========================================================================
-- cex_orderbook_snapshots — off-chain CEX order book snapshots
-- ===========================================================================

COMMENT ON TABLE cex_orderbook_snapshots IS
  '[Hypertable] Periodic top-N L2 order book snapshots from centralised exchanges. One row per (exchange, symbol) per tick — snapshot-per-row, not row-per-price-level. Append-only (INSERT only). No primary key: a row is identified in practice by (exchange, symbol, persisted_at), all three NOT NULL, matching idx_cex_orderbook_snapshots_lookup — though no constraint enforces it. Do not put event_time in an identity or dedup key: it is nullable, and NULL = NULL is never true, so a GROUP BY or self-join over it silently drops or duplicates exactly the rows whose feed gave no timestamp. Carries no chain_id — this is off-chain venue data. Partition key: persisted_at.';
COMMENT ON COLUMN cex_orderbook_snapshots.exchange IS
  'Venue that published the book, as the indexer names it. One indexer deployment per venue. No CHECK constrains the value set and it grows as venues are added, so discover it from the data rather than assuming a fixed list.';
COMMENT ON COLUMN cex_orderbook_snapshots.symbol IS
  'The venue''s own pair string, stored as the venue echoes it and deliberately NOT normalised across venues. Three things differ per venue: the separator, the asset naming (one venue spells BTC as XBT), and even the quote asset (one quotes USDT where another quotes USD). So one economic market has a different symbol string on every venue, and two venues'' quote assets are not necessarily the same asset — a spread or index built on the assumption that they are is economically wrong, not merely empty. Filter and join on (exchange, symbol), never on symbol alone, and discover the value set from the data.';
COMMENT ON COLUMN cex_orderbook_snapshots.event_time IS
  'Venue event time, copied verbatim from the source update. NULL when the feed carried no usable event time — never fabricated from a local clock, which is why this is not the partition key and must not be used as part of a row identity.';
COMMENT ON COLUMN cex_orderbook_snapshots.ingested_at IS
  'Audit. Processing time: when the feed last refreshed this symbol''s book. Drives the staleness skip — a book older than max(3 x tick interval, 30s) is not re-written, so a frozen book does not produce a flat-lined series that reads as live.';
COMMENT ON COLUMN cex_orderbook_snapshots.persisted_at IS
  'Partition key. Audit. Processing time: the tick clock, i.e. when the snapshot was captured for writing. Shared by every symbol in the same tick so they all land on one partition timestamp. Always present, unlike event_time, which is why it partitions the hypertable and why it belongs in the row identity.';
COMMENT ON COLUMN cex_orderbook_snapshots.bids IS
  'JSONB array of [price, size] pairs holding the EXACT decimal strings the venue published — strings, never numeric JSON or floats, so the stored value byte-matches the feed and loses no precision. Pre-trimmed to the configured depth and pre-sorted best-first, i.e. highest price first. An empty book side is [], never NULL. Do not re-sort and do not assume numeric JSON.';
COMMENT ON COLUMN cex_orderbook_snapshots.asks IS
  'JSONB array of [price, size] pairs holding the EXACT decimal strings the venue published — strings, never numeric JSON or floats. Pre-trimmed to the configured depth and pre-sorted best-first, i.e. lowest price first. An empty book side is [], never NULL. Do not re-sort and do not assume numeric JSON.';

-- ===========================================================================
-- psm3_reserves — Sky PSM3 reserve snapshots (L2 only)
-- ===========================================================================

COMMENT ON TABLE psm3_reserves IS
  '[Hypertable] Snapshot of the Sky PSM3 contract''s reserves, taken on a sweep cadence of every N blocks configured per chain — NOT on every block, so block_number is sparse by design and gaps in it are expected, not a data-quality fault. Every read in one snapshot is pinned to the same block hash, so the snapshot is internally consistent. Deployed once per L2 chain; not on mainnet, which uses LitePSM instead — treat the chain table and the deployment config as the source of coverage. All amounts are raw on-chain integers in their native decimals, never normalised at write time, and the scales differ per column, so read each column comment before computing or aggregating; USD valuation belongs in the serving layer, not here. Primary key: (chain_id, block_number, block_version, processing_version, block_timestamp). Partition key: block_timestamp.';
COMMENT ON COLUMN psm3_reserves.chain_id IS
  'PK (with block_number, block_version, processing_version, block_timestamp). FK→chain.chain_id. PSM3 is deployed once per L2 chain, so this identifies the deployment.';
COMMENT ON COLUMN psm3_reserves.address IS
  'The PSM3 contract address (20-byte), recorded on every snapshot.';
COMMENT ON COLUMN psm3_reserves.usds_balance IS
  'USDS.balanceOf(PSM3). Raw on-chain integer, 1e18.';
COMMENT ON COLUMN psm3_reserves.susds_balance IS
  'sUSDS.balanceOf(PSM3). Raw on-chain integer, 1e18.';
COMMENT ON COLUMN psm3_reserves.usdc_balance IS
  'USDC.balanceOf(pocket()). Raw on-chain integer, 1e6 — note the scale differs from the other balances. Read at the pocket, which is governance-settable (PocketSet) and is therefore resolved fresh on every call and never cached; reading the balance at PSM3 instead is not equivalent. The pocket address is not stored in this table.';
COMMENT ON COLUMN psm3_reserves.total_assets IS
  'PSM3.totalAssets(). Raw on-chain integer, 18-decimal USD. A par valuation, not a market-priced one: the contract normalises all three legs internally — USDC from 1e6, USDS and sUSDS from 1e18, and the sUSDS leg through the 1e27 conversion rate — and returns their sum at 1e18. Serve it as par value; do not recompute it from the balance columns and do not expect it to equal a market-priced sum of reserves.';
COMMENT ON COLUMN psm3_reserves.conversion_rate IS
  'rateProvider().getConversionRate(). Raw on-chain integer, 1e27 (ray) — the cross-chain Sky Savings Rate chi, where 1 sUSDS = conversion_rate USDS once divided by 1e27. It rises while the savings rate is positive, but no constraint enforces that, so do not code a monotonicity assumption from this comment. Derive the sUSDS price as (conversion_rate / 1e27) x usds_price; never fetch it directly.';
COMMENT ON COLUMN psm3_reserves.block_number IS
  'PK (with chain_id, block_version, processing_version, block_timestamp). Block the contract reads were pinned to. Sparse: the sweep runs every N blocks, so consecutive rows are not consecutive blocks.';
COMMENT ON COLUMN psm3_reserves.block_version IS
  'PK (with chain_id, block_number, processing_version, block_timestamp). Reorg counter. To select the current row, order by block_number DESC, block_version DESC, then processing_version DESC — block_version alone is not sufficient on this table, because processing_version is also in the key and a reprocess leaves several rows at the same block_version (which is why idx_psm3_reserves_current sorts by all three).';
COMMENT ON COLUMN psm3_reserves.block_timestamp IS
  'PK (with chain_id, block_number, block_version, processing_version). Partition key. On-chain block time; it is part of the key because a hypertable requires the partition column to be.';
COMMENT ON COLUMN psm3_reserves.source IS
  'Trigger for this row. A CHECK restricts it to sweep. A future event-driven path will widen that constraint in its own migration, mirroring token_total_supply''s event/sweep split. Note that PSM3''s own contract events (Deposit, Withdraw, Swap) are not a complete delta stream: a direct token transfer to the contract or its pocket moves balances without firing any of them, so that path has to index the three tokens'' ERC-20 Transfer logs filtered on the PSM3/pocket address.';
COMMENT ON COLUMN psm3_reserves.processing_version IS
  'PK (with chain_id, block_number, block_version, block_timestamp). Correction version: 0 = original, N = Nth reprocess. Required to select the current row — order by it DESC after block_version DESC. A DB trigger reuses the existing version on a same-build replay (dedup) and assigns max+1 when the row is reprocessed under any different build_id, whether newer or older.';
COMMENT ON COLUMN psm3_reserves.build_id IS
  'Audit. FK→build_registry.id (advisory). Traces the row to the code version that produced it; never use it to pick the latest row.';
COMMENT ON COLUMN psm3_reserves.created_at IS
  'Audit. Wall-clock time the row was inserted.';

INSERT INTO migrations (filename)
VALUES ('20260814_120000_orderbook_psm3_comments.sql')
ON CONFLICT (filename) DO NOTHING;
