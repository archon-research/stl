-- Widen chunk_time_interval on the eight hypertables that had spread a few hundred MB
-- each over 150-355 chunks (VEC-663).
--
-- ==========================================================================
-- THIS AFFECTS NEWLY CREATED CHUNKS ONLY. EXISTING CHUNKS ARE NOT TOUCHED.
-- ==========================================================================
-- set_chunk_time_interval rewrites the dimension's interval_length, which TimescaleDB
-- reads when a row arrives that falls outside every chunk that already exists. Chunks
-- cut before this migration keep their old boundaries for the rest of their lives;
-- nothing here merges, splits, moves or rewrites one. The chunk counts in the table
-- below therefore describe the steady state one retention period from now, not the
-- morning after this is applied. Retiring the existing narrow chunks needs its own
-- change (see the ticket) and deliberately does not happen here.
--
-- WHY. The chunk COUNT, not the data volume, drives per-query planner and executor
-- memory. Measured on staging (PG 18.4 / TimescaleDB 2.29.1, 8 GB RAM, shared_buffers
-- 2 GB, work_mem 5 MB) for ONE routine API query against allocation_position, a 130 MB
-- table of 1.37M rows in 176 chunks:
--   * allocated_by_exec = 724 MB, allocated_by_plan = 2.76 GB. The executor holds 176
--     sorts open simultaneously under a Merge Append.
--   * planning ALONE touches 364,720 buffers and takes 561 ms, and pg_stat_statements
--     reports plans=5617 against calls=5616 with mean_plan_time=210 ms -- the statement
--     is re-planned on every single call.
--   * even WHERE token_id=13 AND chain_id=1 ORDER BY block_number DESC LIMIT 5 reports
--     allocated_by_plan = 1.28 GB.
-- That is the leading explanation for the SQLSTATE 53200 (out of memory) errors the
-- staging database returns to workers.
--
-- HOW EACH INTERVAL WAS PICKED. Ceiling: one ACTIVE chunk -- the newest, still
-- uncompressed one -- plus its indexes must fit in 25% of shared_buffers = 512 MB. That
-- is the rubric 20260206_100000_create_onchain_prices.sql already wrote down for this
-- schema. Rates are pg_total_relation_size of each table's uncompressed daily chunks
-- over the 57 days ending 2026-08-27 on staging, so indexes are included -- they are
-- 40-80% of chunk size on these tables. interval = 512 MB / (avg MB per day), rounded
-- down to a calendar value and capped at 30 days.
--
--   table                    avg MB/d  max MB/d  was     ->  now      active   chunks/yr
--   -----------------------  --------  --------  ------      -------  -------  ---------
--   protocol_event                116       148  1 day   ->   4 days   464 MB     365->92
--   morpho_vault_position          28        38  1 day   ->  14 days   392 MB     365->27
--   allocation_position            11        18  1 day   ->  30 days   330 MB     365->13
--   onchain_token_price            11        16  1 day   ->  30 days   330 MB     365->13
--   borrower_collateral           4.1       9.4  1 day   ->  30 days   123 MB     365->13
--   morpho_market_position        2.1       2.8  1 day   ->  30 days    63 MB     365->13
--   morpho_market_state           2.1       2.8  1 day   ->  30 days    63 MB     365->13
--
-- The worst measured DAY still fits the looser upstream reading of the same guidance
-- (25% of 8 GB main memory = 2 GB) for every table; the largest is protocol_event at
-- 4 x 148 MB = 592 MB. protocol_event is the one table the 512 MB ceiling binds hard
-- enough to leave it at 92 chunks a year rather than the low tens; at 116 MB/day there
-- is no interval that gives both.
--
-- The 30-day cap is not arithmetic. Compression fires 2 days after a chunk's END, so a
-- chunk sits in the rowstore for interval + 2 days; S3 tiering (move_after 1 year)
-- moves whole chunks, so a coarser chunk delays tiering by up to one interval; and
-- chunk exclusion loses precision on narrow time ranges. 12-13 chunks a year already
-- meets the goal, so widening further costs all three and buys nothing.

SELECT set_chunk_time_interval('public.protocol_event',         INTERVAL '4 days');
SELECT set_chunk_time_interval('public.morpho_vault_position',  INTERVAL '14 days');
SELECT set_chunk_time_interval('public.allocation_position',    INTERVAL '30 days');
SELECT set_chunk_time_interval('public.onchain_token_price',    INTERVAL '30 days');
SELECT set_chunk_time_interval('public.borrower_collateral',    INTERVAL '30 days');
SELECT set_chunk_time_interval('public.morpho_market_position', INTERVAL '30 days');
SELECT set_chunk_time_interval('public.morpho_market_state',    INTERVAL '30 days');

-- sparklend_reserve_data partitions on block_number (BIGINT), not time, so its interval
-- is a block count and the arithmetic above does not transfer. Two chains share the one
-- number line: measured on staging, Ethereum occupies 16,776,431..25,845,445 and
-- Avalanche 79,614,900..93,794,865 -- 23.2M blocks of occupied space, 233 slots of
-- 100,000 blocks, 159 of which hold rows. Density is ~2.0 MB per 100,000 blocks on
-- Ethereum and ~1.6 MB on Avalanche, so a 1,000,000-block chunk is ~20 MB; the
-- compression policy's 200,000-block lag adds at most 200,000 blocks more, still under
-- 5% of the 512 MB ceiling. The same occupied space becomes ~25 chunks (10 Ethereum +
-- 15 Avalanche) instead of 159.
--
-- One block interval cannot be right for two chains whose block rates differ ~6x:
-- 1,000,000 blocks is ~139 days of Ethereum and ~23 days of Avalanche. The real fix is
-- partitioning this table on time, or space-partitioning it by chain, which is a table
-- rewrite rather than an interval change and is deliberately out of scope here.
SELECT set_chunk_time_interval('public.sparklend_reserve_data', 1000000);

-- Restate each table's interval where \d+ shows it, so the value is discoverable
-- without finding this file. Text is otherwise unchanged from
-- 20260609_120000_add_schema_comments.sql.
COMMENT ON TABLE protocol_event IS
  '[Hypertable] Every decoded on-chain event log from tracked protocol contracts. Canonical raw archive for event-sourced data. Event-driven position/state tables are written as siblings from the same logs, not derived from rows here; anchorage, prime_debt, and price tables are API- or contract-read-sourced and independent of this table. Partition key: created_at. Chunk interval: 4 days (governs new chunks only; chunks cut before VEC-663 keep their 1-day boundaries).';
COMMENT ON TABLE morpho_vault_position IS
  '[Hypertable] Per-user, per-vault snapshot at each block where the user interacted. assets is derived from shares at write time. Partition key: timestamp. Chunk interval: 14 days (governs new chunks only; chunks cut before VEC-663 keep their 1-day boundaries).';
COMMENT ON TABLE allocation_position IS
  '[Hypertable] Every token movement into/out of each Prime''s proxy contract (deposit, withdrawal, sweep). Amount columns are decimals-normalized to human-readable values at write time. Partition key: created_at. Chunk interval: 30 days (governs new chunks only; chunks cut before VEC-663 keep their 1-day boundaries).';
COMMENT ON TABLE onchain_token_price IS
  '[Hypertable] Oracle price reads per token per block (Aave oracle, Chainlink, Chronicle, Redstone). A row is written only when the value differs from the last cached price (exact change detection, no threshold). Absence can mean the price was unchanged, a feed read failed, or the worker was down. Partition key: timestamp. Chunk interval: 30 days (governs new chunks only; chunks cut before VEC-663 keep their 1-day boundaries).';
COMMENT ON TABLE borrower_collateral IS
  '[Hypertable] Per-user collateral-position ledger. A full collateral snapshot is written on Supply, Withdraw, Borrow, Repay, LiquidationCall, and ReserveUsedAsCollateral Enabled/Disabled events, plus internal:Snapshot rows. One row per (user, collateral asset) per triggering event. Partition key: created_at. Chunk interval: 30 days (governs new chunks only; chunks cut before VEC-663 keep their 1-day boundaries).';
COMMENT ON TABLE morpho_market_position IS
  '[Hypertable] Per-user, per-market snapshot at each block where the user interacted. supply_assets and borrow_assets are derived from shares at write time. Partition key: timestamp. Chunk interval: 30 days (governs new chunks only; chunks cut before VEC-663 keep their 1-day boundaries).';
COMMENT ON TABLE morpho_market_state IS
  '[Hypertable] Per-block aggregate snapshot of each Morpho Blue market (supply, borrow, shares, fee). Partition key: timestamp (on-chain block time). Chunk interval: 30 days (governs new chunks only; chunks cut before VEC-663 keep their 1-day boundaries).';
COMMENT ON TABLE sparklend_reserve_data IS
  '[Hypertable] Per-block snapshot of aggregate state for each lending reserve (SparkLend/Aave), from ReserveDataUpdated events. Amount columns are raw on-chain integers in the reserve token''s native decimals; rates and indexes use ray (1e27). Partition key: block_number. Chunk interval: 1,000,000 blocks (governs new chunks only; chunks cut before VEC-663 keep their 100,000-block boundaries).';

INSERT INTO migrations (filename)
VALUES ('20260827_120000_widen_chunk_intervals.sql')
ON CONFLICT (filename) DO NOTHING;
