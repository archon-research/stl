-- Catalogue metadata for cex_orderbook_snapshots and psm3_reserves.
-- Both tables were created after 20260609_120000_add_schema_comments.sql (the
-- original catalogue backfill), so they were never documented. Follows that
-- migration's conventions:
--   [Type]: Dimension | Configuration | Operational | Hypertable
--   Roles:  PK | FK→table.col | Derived | Partition | Audit
--   Scale:  amounts are raw on-chain integers in their native decimals, stated
--           per column. psm3_reserves mixes three scales (1e6, 1e18, 1e27).
-- Comment-only migration: no DDL on table structure, safe and idempotent
-- (COMMENT ON overwrites).

-- ===========================================================================
-- cex_orderbook_snapshots — off-chain CEX order book snapshots
-- ===========================================================================

COMMENT ON TABLE cex_orderbook_snapshots IS
  '[Hypertable] Periodic top-N L2 order book snapshots from centralised exchanges. One row per (exchange, symbol) per tick — snapshot-per-row, not row-per-price-level. Append-only (INSERT only). No natural primary key: a row is identified by (exchange, symbol, event_time, persisted_at), since event_time repeats across re-polls and ingested_at does not fully disambiguate. Carries no chain_id — this is off-chain venue data. Partition key: persisted_at.';
COMMENT ON COLUMN cex_orderbook_snapshots.exchange IS
  'Venue that published the book: one of coinbase, kraken, okx. One indexer deployment per venue.';
COMMENT ON COLUMN cex_orderbook_snapshots.symbol IS
  'The venue''s own pair string, upper-cased, and deliberately NOT normalised across venues: Coinbase and OKX separate with a hyphen (BTC-USD), Kraken with a slash (BTC/USD). The same market therefore has a different symbol per exchange — join on (exchange, symbol), never on symbol alone.';
COMMENT ON COLUMN cex_orderbook_snapshots.event_time IS
  'Venue event time, copied verbatim from the source update. NULL when the feed carried no usable event time — never fabricated from a local clock, which is why this is not the partition key.';
COMMENT ON COLUMN cex_orderbook_snapshots.ingested_at IS
  'Audit. Processing time: when the feed last refreshed this symbol''s book. Drives the staleness skip — a book older than max(3 x tick interval, 30s) is not re-written, so a frozen book does not produce a flat-lined series that reads as live.';
COMMENT ON COLUMN cex_orderbook_snapshots.persisted_at IS
  'Partition key. Audit. Processing time: the tick clock, i.e. when the snapshot was captured for writing. Shared by every symbol in the same tick so they all land on one partition timestamp. Always present, unlike event_time, which is why it partitions the hypertable.';
COMMENT ON COLUMN cex_orderbook_snapshots.bids IS
  'JSONB array of [price, size] pairs holding the EXACT decimal strings the venue published — strings, never numeric JSON or floats, so the stored value byte-matches the feed and loses no precision. Pre-trimmed to the configured depth (default 100 levels) and pre-sorted best-first, i.e. highest price first. An empty book side is [], never NULL. Do not re-sort and do not assume numeric JSON.';
COMMENT ON COLUMN cex_orderbook_snapshots.asks IS
  'JSONB array of [price, size] pairs holding the EXACT decimal strings the venue published — strings, never numeric JSON or floats. Pre-trimmed to the configured depth (default 100 levels) and pre-sorted best-first, i.e. lowest price first. An empty book side is [], never NULL. Do not re-sort and do not assume numeric JSON.';

-- ===========================================================================
-- psm3_reserves — Sky PSM3 reserve snapshots (L2 only)
-- ===========================================================================

COMMENT ON TABLE psm3_reserves IS
  '[Hypertable] Per-block snapshot of the Sky PSM3 contract''s reserves. One row per (chain, block) sweep; every read is pinned to the event''s block hash in a single multicall so the snapshot is internally consistent. L2 only — Base, Optimism, Arbitrum, Unichain; mainnet runs LitePSM and is not covered here. All amounts are raw on-chain integers in their native decimals and are never normalised at write time, and the scales differ per column — read each column comment before computing or aggregating. USD valuation lives in the Python API, not here. Partition key: block_timestamp.';
COMMENT ON COLUMN psm3_reserves.chain_id IS
  'FK→chain.chain_id. PSM3 is deployed once per L2, so this identifies the deployment.';
COMMENT ON COLUMN psm3_reserves.address IS
  'The PSM3 contract address (20-byte), recorded on every snapshot.';
COMMENT ON COLUMN psm3_reserves.usds_balance IS
  'USDS.balanceOf(PSM3). Raw on-chain integer, 1e18.';
COMMENT ON COLUMN psm3_reserves.susds_balance IS
  'sUSDS.balanceOf(PSM3). Raw on-chain integer, 1e18.';
COMMENT ON COLUMN psm3_reserves.usdc_balance IS
  'USDC.balanceOf(pocket()). Raw on-chain integer, 1e6 — note the scale differs from the other balances. Read at the governance-settable pocket, not at PSM3: today pocket == PSM3 on all four chains so reading PSM3 happens to give the same answer, but that breaks silently if a pocket is ever set. The pocket address itself is not stored in this table.';
COMMENT ON COLUMN psm3_reserves.total_assets IS
  'PSM3.totalAssets(). Raw on-chain integer, 1e18. A par valuation (usds + usdc + susds x conversion_rate) rather than a market-priced one, and the contract scales the 1e6 USDC leg internally. Serve it as par value; do not recompute it from the balance columns and do not expect it to equal a market-priced sum of reserves.';
COMMENT ON COLUMN psm3_reserves.conversion_rate IS
  'rateProvider().getConversionRate(). Raw on-chain integer, 1e27 (ray) — the cross-chain Sky Savings Rate chi, where 1 sUSDS = conversion_rate USDS. Monotonic and rising, currently ~1.05-1.10 once scaled. Derive the sUSDS price as (conversion_rate / 1e27) x usds_price; never fetch it directly.';
COMMENT ON COLUMN psm3_reserves.block_number IS
  'Block the contract reads were pinned to.';
COMMENT ON COLUMN psm3_reserves.block_version IS
  'Reorg counter. Canonical row = highest block_version at this block_number.';
COMMENT ON COLUMN psm3_reserves.block_timestamp IS
  'Partition key. On-chain block time. Also part of the primary key, which a hypertable requires.';
COMMENT ON COLUMN psm3_reserves.source IS
  'Trigger for this row. Always sweep today, enforced by a CHECK. A future event-driven path will widen the constraint in its own migration, mirroring token_total_supply''s event/sweep split; note that events alone are not a complete delta stream, because a direct ERC-20 transfer to the contract changes balances without emitting one.';
COMMENT ON COLUMN psm3_reserves.processing_version IS
  'Audit. A DB trigger reuses the existing version on a same-build replay (dedup) and assigns max+1 when the row is reprocessed under any different build_id, whether newer or older.';
COMMENT ON COLUMN psm3_reserves.build_id IS
  'Audit. FK→build_registry.id (advisory). Traces the row to the code version that produced it; never use it to pick the latest row.';
COMMENT ON COLUMN psm3_reserves.created_at IS
  'Audit. Wall-clock time the row was inserted.';

INSERT INTO migrations (filename)
VALUES ('20260814_120000_orderbook_psm3_comments.sql')
ON CONFLICT (filename) DO NOTHING;
