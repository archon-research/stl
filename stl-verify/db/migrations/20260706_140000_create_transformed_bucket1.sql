-- Transformation layer, bucket 1 (VEC-484): 13 governed tables with a native/renameable
-- observation column and a raw PK. Register-driven output (schema_master transforms/fills).
-- Each transformed.<table> is a hypertable (partitioned on the observation time) with a PK
-- derived from the raw PK, plus an incremental watermark upsert function transformed._run_<t>()
-- that a worker calls.
--
-- Watermark: the monotonic build_id ingestion cursor. build_id is a globally increasing
-- pipeline-run id present on every raw row of all 13 tables. Backfilled rows (older
-- block_number) and reorg corrections (new block_version at the same block_number) are written
-- under the current (higher) build, so watermarking on build_id always re-reads them, while a
-- block_number/synced_at watermark would permanently skip them (silent data hole). Because builds
-- are long-lived, each _run_<t>() reads WHERE build_id >= watermark (>= not >, so the current
-- build is reprocessed every run) and advances the watermark to the max build_id of the rows it
-- just upserted, captured from the upsert's RETURNING set via a CTE rather than a full-table
-- max() scan. Idempotent by construction: the upsert keys on the PK, so re-reading the current
-- build adds 0 new rows and refreshes any in-flight corrections. On-data 1:1 parity with raw is
-- verified on full data at rollout. Buckets 2 (block-time-dimension) and 3 (no-PK, keyed via
-- transform_config) follow separately.
--
-- This migration is idempotent and non-destructive: it creates the `transformed` schema and its
-- tables only if absent, guards the primary-key ALTERs, and passes if_not_exists to
-- create_hypertable, so re-applying it is a no-op and never drops data. (build_id is assumed
-- non-decreasing per write: a raw row written with a build_id below the current high-water would
-- be skipped, so the source tables carry build_id NOT NULL / DEFAULT 0.)
--
-- COMMENT ON metadata follows the catalogue convention (see 20260609_120000_add_schema_comments.sql):
-- a [Type] tag on each table, per-column Roles (PK | FK->table.col | Derived | Partition | Audit),
-- and, for numeric columns, the exact unit/scale. Text is carried verbatim from each raw source
-- table's own comments; a raw column that was itself undocumented is flagged "verify before
-- computing" rather than given a fabricated scale.

CREATE SCHEMA IF NOT EXISTS transformed;

CREATE TABLE IF NOT EXISTS transformed._watermark(source text PRIMARY KEY, build_wm int NOT NULL DEFAULT -1);
COMMENT ON TABLE transformed._watermark IS
  '[Operational] Incremental cursor for the transformation layer. One row per transformed table; transformed._run_<source>() reads it, materializes raw rows whose build_id is at or past it, and advances it.';
COMMENT ON COLUMN transformed._watermark.source IS
  'PK. Transformed table name (e.g. morpho_market_state) whose _run_<source>() function owns this cursor.';
COMMENT ON COLUMN transformed._watermark.build_wm IS
  'High-water build_id materialized so far; -1 before the first run. _run_<source>() reads rows WHERE build_id >= build_wm and advances it to the max build_id upserted in that pass.';

CREATE TABLE IF NOT EXISTS transformed."morpho_market_state" AS
SELECT p."chain_id",
       p."protocol_id",
       s."morpho_market_id",
       s."block_number",
       s."block_version",
       s."timestamp" AS "block_timestamp",
       s."total_supply_assets",
       s."total_supply_shares",
       s."total_borrow_assets",
       s."total_borrow_shares",
       CASE WHEN s."last_update" BETWEEN 1500000000 AND 4100000000 THEN to_timestamp(s."last_update") END AS "last_update_at",
       s."fee",
       s."prev_borrow_rate",
       s."interest_accrued",
       s."fee_shares",
       s."processing_version",
       s."build_id"
FROM public."morpho_market_state" s LEFT JOIN public."morpho_market" p ON p."id"=s."morpho_market_id" WHERE false;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='transformed."morpho_market_state"'::regclass AND contype='p') THEN ALTER TABLE transformed."morpho_market_state" ALTER COLUMN "block_timestamp" SET NOT NULL, ADD PRIMARY KEY ("morpho_market_id", "block_number", "block_version", "processing_version", "block_timestamp"); END IF; END $$;
SELECT create_hypertable('transformed."morpho_market_state"','block_timestamp',chunk_time_interval=>INTERVAL '30 days',if_not_exists=>TRUE);
ALTER TABLE transformed."morpho_market_state" SET (timescaledb.compress, timescaledb.compress_segmentby = 'morpho_market_id', timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC');
SELECT add_compression_policy('transformed."morpho_market_state"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."morpho_market_state"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.morpho_market_state'; END $$;
INSERT INTO transformed._watermark(source) VALUES ('morpho_market_state') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_morpho_market_state() RETURNS bigint AS $fn$
DECLARE bw int; n bigint; mx int;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='morpho_market_state';
  WITH ins AS (
  INSERT INTO transformed."morpho_market_state"
  SELECT p."chain_id",
       p."protocol_id",
       s."morpho_market_id",
       s."block_number",
       s."block_version",
       s."timestamp" AS "block_timestamp",
       s."total_supply_assets",
       s."total_supply_shares",
       s."total_borrow_assets",
       s."total_borrow_shares",
       CASE WHEN s."last_update" BETWEEN 1500000000 AND 4100000000 THEN to_timestamp(s."last_update") END AS "last_update_at",
       s."fee",
       s."prev_borrow_rate",
       s."interest_accrued",
       s."fee_shares",
       s."processing_version",
       s."build_id"
  FROM public."morpho_market_state" s LEFT JOIN public."morpho_market" p ON p."id"=s."morpho_market_id" WHERE (s."build_id" >= bw OR s."build_id" IS NULL)
  ON CONFLICT ("morpho_market_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "total_supply_assets"=EXCLUDED."total_supply_assets", "total_supply_shares"=EXCLUDED."total_supply_shares", "total_borrow_assets"=EXCLUDED."total_borrow_assets", "total_borrow_shares"=EXCLUDED."total_borrow_shares", "last_update_at"=EXCLUDED."last_update_at", "fee"=EXCLUDED."fee", "prev_borrow_rate"=EXCLUDED."prev_borrow_rate", "interest_accrued"=EXCLUDED."interest_accrued", "fee_shares"=EXCLUDED."fee_shares", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id"
  RETURNING "build_id")
  SELECT count(*), max("build_id") INTO n, mx FROM ins;
  UPDATE transformed._watermark SET build_wm=GREATEST(build_wm, COALESCE(mx, build_wm)) WHERE source='morpho_market_state';
  RETURN n;
END $fn$ LANGUAGE plpgsql;
COMMENT ON TABLE transformed."morpho_market_state" IS
  '[Hypertable] Canonical transform of public.morpho_market_state (VEC-484): per-block aggregate snapshot of each Morpho Blue market (supply, borrow, shares, fee). chain_id/protocol_id joined from morpho_market; raw "timestamp" renamed to block_timestamp; last_update cast to last_update_at. 1:1 register-driven mirror. Partition: block_timestamp.';
COMMENT ON COLUMN transformed."morpho_market_state".chain_id IS 'Derived. FK->chain.chain_id, joined from morpho_market. Chain scope introduced by the transform.';
COMMENT ON COLUMN transformed."morpho_market_state".protocol_id IS 'Derived. FK->protocol.id, joined from morpho_market. Protocol scope introduced by the transform.';
COMMENT ON COLUMN transformed."morpho_market_state".morpho_market_id IS 'FK->morpho_market.id. Part of PK.';
COMMENT ON COLUMN transformed."morpho_market_state".block_number IS 'Block height of the snapshot. Part of PK.';
COMMENT ON COLUMN transformed."morpho_market_state".block_version IS 'Reorg version: increments when a block is re-indexed after a reorg. Part of PK.';
COMMENT ON COLUMN transformed."morpho_market_state".block_timestamp IS 'Partition. On-chain block time, renamed from raw "timestamp". Part of PK.';
COMMENT ON COLUMN transformed."morpho_market_state".total_supply_assets IS 'Raw on-chain integer in the loan token''s native decimals. Total assets supplied to this market.';
COMMENT ON COLUMN transformed."morpho_market_state".total_supply_shares IS 'Morpho shares, raw on-chain integer (share scale = loan-token decimals + 6). Approximately the sum of user supply_shares in morpho_market_position, but not exactly: fee shares minted to the protocol fee recipient have no position row, and only event-observed users are tracked.';
COMMENT ON COLUMN transformed."morpho_market_state".total_borrow_assets IS 'Raw on-chain integer in the loan token''s native decimals. Total assets borrowed. Equals total_supply_assets at 100% utilisation.';
COMMENT ON COLUMN transformed."morpho_market_state".total_borrow_shares IS 'Morpho shares, raw on-chain integer (share scale = loan-token decimals + 6). Approximately the sum of user borrow_shares in morpho_market_position; see total_supply_shares for why it is not exact.';
COMMENT ON COLUMN transformed."morpho_market_state".last_update_at IS 'Derived. Market last-accrual time; to_timestamp() of the raw last_update epoch-seconds, NULL when outside [1500000000, 4100000000] plausibility bounds.';
COMMENT ON COLUMN transformed."morpho_market_state".fee IS 'Fee rate as wad (1e18 = 100%). Share of interest going to the Morpho fee recipient rather than lenders.';
COMMENT ON COLUMN transformed."morpho_market_state".prev_borrow_rate IS 'Morpho market borrow rate at the previous accrual, raw on-chain value mirrored 1:1 from public.morpho_market_state; scale as stored on-chain (raw column undocumented - verify unit/scale before computing).';
COMMENT ON COLUMN transformed."morpho_market_state".interest_accrued IS 'Interest accrued since the previous accrual, raw on-chain integer mirrored 1:1 from public.morpho_market_state (raw column undocumented - verify unit/scale before computing).';
COMMENT ON COLUMN transformed."morpho_market_state".fee_shares IS 'Fee shares minted to the fee recipient this accrual, Morpho shares raw on-chain integer mirrored 1:1 from public.morpho_market_state (raw column undocumented - verify scale before computing).';
COMMENT ON COLUMN transformed."morpho_market_state".processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by block_timestamp DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN transformed."morpho_market_state".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row. transformed._watermark advances on this.';

CREATE TABLE IF NOT EXISTS transformed."morpho_market_position" AS
SELECT p."chain_id",
       p."protocol_id",
       s."user_id",
       s."morpho_market_id",
       s."block_number",
       s."block_version",
       s."timestamp" AS "block_timestamp",
       s."supply_shares",
       s."borrow_shares",
       s."collateral",
       s."supply_assets",
       s."borrow_assets",
       s."processing_version",
       s."build_id"
FROM public."morpho_market_position" s LEFT JOIN public."morpho_market" p ON p."id"=s."morpho_market_id" WHERE false;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='transformed."morpho_market_position"'::regclass AND contype='p') THEN ALTER TABLE transformed."morpho_market_position" ALTER COLUMN "block_timestamp" SET NOT NULL, ADD PRIMARY KEY ("user_id", "morpho_market_id", "block_number", "block_version", "processing_version", "block_timestamp"); END IF; END $$;
SELECT create_hypertable('transformed."morpho_market_position"','block_timestamp',chunk_time_interval=>INTERVAL '30 days',if_not_exists=>TRUE);
ALTER TABLE transformed."morpho_market_position" SET (timescaledb.compress, timescaledb.compress_segmentby = 'morpho_market_id, user_id', timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC');
SELECT add_compression_policy('transformed."morpho_market_position"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."morpho_market_position"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.morpho_market_position'; END $$;
INSERT INTO transformed._watermark(source) VALUES ('morpho_market_position') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_morpho_market_position() RETURNS bigint AS $fn$
DECLARE bw int; n bigint; mx int;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='morpho_market_position';
  WITH ins AS (
  INSERT INTO transformed."morpho_market_position"
  SELECT p."chain_id",
       p."protocol_id",
       s."user_id",
       s."morpho_market_id",
       s."block_number",
       s."block_version",
       s."timestamp" AS "block_timestamp",
       s."supply_shares",
       s."borrow_shares",
       s."collateral",
       s."supply_assets",
       s."borrow_assets",
       s."processing_version",
       s."build_id"
  FROM public."morpho_market_position" s LEFT JOIN public."morpho_market" p ON p."id"=s."morpho_market_id" WHERE (s."build_id" >= bw OR s."build_id" IS NULL)
  ON CONFLICT ("user_id", "morpho_market_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "supply_shares"=EXCLUDED."supply_shares", "borrow_shares"=EXCLUDED."borrow_shares", "collateral"=EXCLUDED."collateral", "supply_assets"=EXCLUDED."supply_assets", "borrow_assets"=EXCLUDED."borrow_assets", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id"
  RETURNING "build_id")
  SELECT count(*), max("build_id") INTO n, mx FROM ins;
  UPDATE transformed._watermark SET build_wm=GREATEST(build_wm, COALESCE(mx, build_wm)) WHERE source='morpho_market_position';
  RETURN n;
END $fn$ LANGUAGE plpgsql;
COMMENT ON TABLE transformed."morpho_market_position" IS
  '[Hypertable] Canonical transform of public.morpho_market_position (VEC-484): per-user, per-market snapshot at each block where the user interacted. supply_assets/borrow_assets are derived from shares at write time. chain_id/protocol_id joined from morpho_market; raw "timestamp" renamed to block_timestamp. Partition: block_timestamp.';
COMMENT ON COLUMN transformed."morpho_market_position".chain_id IS 'Derived. FK->chain.chain_id, joined from morpho_market. Chain scope introduced by the transform.';
COMMENT ON COLUMN transformed."morpho_market_position".protocol_id IS 'Derived. FK->protocol.id, joined from morpho_market. Protocol scope introduced by the transform.';
COMMENT ON COLUMN transformed."morpho_market_position".user_id IS 'FK->"user".id. Part of PK.';
COMMENT ON COLUMN transformed."morpho_market_position".morpho_market_id IS 'FK->morpho_market.id. Part of PK.';
COMMENT ON COLUMN transformed."morpho_market_position".block_number IS 'Block height of the snapshot. Part of PK.';
COMMENT ON COLUMN transformed."morpho_market_position".block_version IS 'Reorg version: increments when a block is re-indexed after a reorg. Part of PK.';
COMMENT ON COLUMN transformed."morpho_market_position".block_timestamp IS 'Partition. On-chain block time, renamed from raw "timestamp". Part of PK.';
COMMENT ON COLUMN transformed."morpho_market_position".supply_shares IS 'Morpho shares, raw on-chain integer (share scale = loan-token decimals + 6). User''s proportional claim on the supply pool. Use shares for conservation checks.';
COMMENT ON COLUMN transformed."morpho_market_position".borrow_shares IS 'Morpho shares, raw on-chain integer (share scale = loan-token decimals + 6). User''s proportional share of the borrow pool.';
COMMENT ON COLUMN transformed."morpho_market_position".collateral IS 'Raw on-chain integer in the collateral token''s native decimals. Collateral deposited by this user.';
COMMENT ON COLUMN transformed."morpho_market_position".supply_assets IS 'Derived, raw integer in the loan token''s native decimals. supply_shares x (total_supply_assets / total_supply_shares) at this block. Use shares for conservation checks; assets are an approximation.';
COMMENT ON COLUMN transformed."morpho_market_position".borrow_assets IS 'Derived, raw integer in the loan token''s native decimals. borrow_shares x (total_borrow_assets / total_borrow_shares) at this block. Same approximation caveat as supply_assets.';
COMMENT ON COLUMN transformed."morpho_market_position".processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by block_timestamp DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN transformed."morpho_market_position".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row. transformed._watermark advances on this.';

CREATE TABLE IF NOT EXISTS transformed."morpho_vault_state" AS
SELECT p."chain_id",
       p."protocol_id",
       s."morpho_vault_id",
       s."block_number",
       s."block_version",
       s."timestamp" AS "block_timestamp",
       s."total_assets",
       s."total_shares",
       s."fee_shares",
       s."new_total_assets",
       s."previous_total_assets",
       s."management_fee_shares",
       s."processing_version",
       s."build_id"
FROM public."morpho_vault_state" s LEFT JOIN public."morpho_vault" p ON p."id"=s."morpho_vault_id" WHERE false;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='transformed."morpho_vault_state"'::regclass AND contype='p') THEN ALTER TABLE transformed."morpho_vault_state" ALTER COLUMN "block_timestamp" SET NOT NULL, ADD PRIMARY KEY ("morpho_vault_id", "block_number", "block_version", "processing_version", "block_timestamp"); END IF; END $$;
SELECT create_hypertable('transformed."morpho_vault_state"','block_timestamp',chunk_time_interval=>INTERVAL '30 days',if_not_exists=>TRUE);
ALTER TABLE transformed."morpho_vault_state" SET (timescaledb.compress, timescaledb.compress_segmentby = 'morpho_vault_id', timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC');
SELECT add_compression_policy('transformed."morpho_vault_state"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."morpho_vault_state"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.morpho_vault_state'; END $$;
INSERT INTO transformed._watermark(source) VALUES ('morpho_vault_state') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_morpho_vault_state() RETURNS bigint AS $fn$
DECLARE bw int; n bigint; mx int;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='morpho_vault_state';
  WITH ins AS (
  INSERT INTO transformed."morpho_vault_state"
  SELECT p."chain_id",
       p."protocol_id",
       s."morpho_vault_id",
       s."block_number",
       s."block_version",
       s."timestamp" AS "block_timestamp",
       s."total_assets",
       s."total_shares",
       s."fee_shares",
       s."new_total_assets",
       s."previous_total_assets",
       s."management_fee_shares",
       s."processing_version",
       s."build_id"
  FROM public."morpho_vault_state" s LEFT JOIN public."morpho_vault" p ON p."id"=s."morpho_vault_id" WHERE (s."build_id" >= bw OR s."build_id" IS NULL)
  ON CONFLICT ("morpho_vault_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "total_assets"=EXCLUDED."total_assets", "total_shares"=EXCLUDED."total_shares", "fee_shares"=EXCLUDED."fee_shares", "new_total_assets"=EXCLUDED."new_total_assets", "previous_total_assets"=EXCLUDED."previous_total_assets", "management_fee_shares"=EXCLUDED."management_fee_shares", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id"
  RETURNING "build_id")
  SELECT count(*), max("build_id") INTO n, mx FROM ins;
  UPDATE transformed._watermark SET build_wm=GREATEST(build_wm, COALESCE(mx, build_wm)) WHERE source='morpho_vault_state';
  RETURN n;
END $fn$ LANGUAGE plpgsql;
COMMENT ON TABLE transformed."morpho_vault_state" IS
  '[Hypertable] Canonical transform of public.morpho_vault_state (VEC-484): per-block aggregate snapshot of each MetaMorpho vault (total assets, total shares). chain_id/protocol_id joined from morpho_vault; raw "timestamp" renamed to block_timestamp. Partition: block_timestamp.';
COMMENT ON COLUMN transformed."morpho_vault_state".chain_id IS 'Derived. FK->chain.chain_id, joined from morpho_vault. Chain scope introduced by the transform.';
COMMENT ON COLUMN transformed."morpho_vault_state".protocol_id IS 'Derived. FK->protocol.id, joined from morpho_vault. Protocol scope introduced by the transform.';
COMMENT ON COLUMN transformed."morpho_vault_state".morpho_vault_id IS 'FK->morpho_vault.id. Part of PK.';
COMMENT ON COLUMN transformed."morpho_vault_state".block_number IS 'Block height of the snapshot. Part of PK.';
COMMENT ON COLUMN transformed."morpho_vault_state".block_version IS 'Reorg version: increments when a block is re-indexed after a reorg. Part of PK.';
COMMENT ON COLUMN transformed."morpho_vault_state".block_timestamp IS 'Partition. On-chain block time, renamed from raw "timestamp". Part of PK.';
COMMENT ON COLUMN transformed."morpho_vault_state".total_assets IS 'Raw on-chain integer in the underlying asset''s native decimals. Total assets under management = NAV of the vault.';
COMMENT ON COLUMN transformed."morpho_vault_state".total_shares IS 'Vault ERC-4626 shares, raw on-chain integer. Approximately the sum of user shares in morpho_vault_position, but not exactly: only event-observed holders get position rows.';
COMMENT ON COLUMN transformed."morpho_vault_state".fee_shares IS 'Vault fee shares accrued, ERC-4626 share units raw on-chain integer, mirrored 1:1 from public.morpho_vault_state (raw column undocumented - verify scale before computing).';
COMMENT ON COLUMN transformed."morpho_vault_state".new_total_assets IS 'Vault new_total_assets reading, raw on-chain integer in the underlying asset''s native decimals, mirrored 1:1 from public.morpho_vault_state (raw column undocumented - verify before computing).';
COMMENT ON COLUMN transformed."morpho_vault_state".previous_total_assets IS 'Vault previous_total_assets reading, raw on-chain integer in the underlying asset''s native decimals, mirrored 1:1 from public.morpho_vault_state (raw column undocumented - verify before computing).';
COMMENT ON COLUMN transformed."morpho_vault_state".management_fee_shares IS 'Management fee shares, ERC-4626 share units raw on-chain integer, mirrored 1:1 from public.morpho_vault_state (raw column undocumented - verify scale before computing).';
COMMENT ON COLUMN transformed."morpho_vault_state".processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by block_timestamp DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN transformed."morpho_vault_state".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row. transformed._watermark advances on this.';

CREATE TABLE IF NOT EXISTS transformed."morpho_vault_position" AS
SELECT p."chain_id",
       p."protocol_id",
       s."user_id",
       s."morpho_vault_id",
       s."block_number",
       s."block_version",
       s."timestamp" AS "block_timestamp",
       s."shares",
       s."assets",
       s."processing_version",
       s."build_id"
FROM public."morpho_vault_position" s LEFT JOIN public."morpho_vault" p ON p."id"=s."morpho_vault_id" WHERE false;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='transformed."morpho_vault_position"'::regclass AND contype='p') THEN ALTER TABLE transformed."morpho_vault_position" ALTER COLUMN "block_timestamp" SET NOT NULL, ADD PRIMARY KEY ("user_id", "morpho_vault_id", "block_number", "block_version", "processing_version", "block_timestamp"); END IF; END $$;
SELECT create_hypertable('transformed."morpho_vault_position"','block_timestamp',chunk_time_interval=>INTERVAL '30 days',if_not_exists=>TRUE);
ALTER TABLE transformed."morpho_vault_position" SET (timescaledb.compress, timescaledb.compress_segmentby = 'morpho_vault_id, user_id', timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC');
SELECT add_compression_policy('transformed."morpho_vault_position"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."morpho_vault_position"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.morpho_vault_position'; END $$;
INSERT INTO transformed._watermark(source) VALUES ('morpho_vault_position') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_morpho_vault_position() RETURNS bigint AS $fn$
DECLARE bw int; n bigint; mx int;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='morpho_vault_position';
  WITH ins AS (
  INSERT INTO transformed."morpho_vault_position"
  SELECT p."chain_id",
       p."protocol_id",
       s."user_id",
       s."morpho_vault_id",
       s."block_number",
       s."block_version",
       s."timestamp" AS "block_timestamp",
       s."shares",
       s."assets",
       s."processing_version",
       s."build_id"
  FROM public."morpho_vault_position" s LEFT JOIN public."morpho_vault" p ON p."id"=s."morpho_vault_id" WHERE (s."build_id" >= bw OR s."build_id" IS NULL)
  ON CONFLICT ("user_id", "morpho_vault_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "shares"=EXCLUDED."shares", "assets"=EXCLUDED."assets", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id"
  RETURNING "build_id")
  SELECT count(*), max("build_id") INTO n, mx FROM ins;
  UPDATE transformed._watermark SET build_wm=GREATEST(build_wm, COALESCE(mx, build_wm)) WHERE source='morpho_vault_position';
  RETURN n;
END $fn$ LANGUAGE plpgsql;
COMMENT ON TABLE transformed."morpho_vault_position" IS
  '[Hypertable] Canonical transform of public.morpho_vault_position (VEC-484): per-user, per-vault snapshot at each block where the user interacted. assets is derived from shares at write time. chain_id/protocol_id joined from morpho_vault; raw "timestamp" renamed to block_timestamp. Partition: block_timestamp.';
COMMENT ON COLUMN transformed."morpho_vault_position".chain_id IS 'Derived. FK->chain.chain_id, joined from morpho_vault. Chain scope introduced by the transform.';
COMMENT ON COLUMN transformed."morpho_vault_position".protocol_id IS 'Derived. FK->protocol.id, joined from morpho_vault. Protocol scope introduced by the transform.';
COMMENT ON COLUMN transformed."morpho_vault_position".user_id IS 'FK->"user".id. Part of PK.';
COMMENT ON COLUMN transformed."morpho_vault_position".morpho_vault_id IS 'FK->morpho_vault.id. Part of PK.';
COMMENT ON COLUMN transformed."morpho_vault_position".block_number IS 'Block height of the snapshot. Part of PK.';
COMMENT ON COLUMN transformed."morpho_vault_position".block_version IS 'Reorg version: increments when a block is re-indexed after a reorg. Part of PK.';
COMMENT ON COLUMN transformed."morpho_vault_position".block_timestamp IS 'Partition. On-chain block time, renamed from raw "timestamp". Part of PK.';
COMMENT ON COLUMN transformed."morpho_vault_position".shares IS 'Vault ERC-4626 share units, raw on-chain integer. User''s proportional ownership of the vault. Use for conservation checks.';
COMMENT ON COLUMN transformed."morpho_vault_position".assets IS 'Derived, raw integer in the underlying asset''s native decimals. shares x (total_assets / total_shares) at this block.';
COMMENT ON COLUMN transformed."morpho_vault_position".processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by block_timestamp DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN transformed."morpho_vault_position".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row. transformed._watermark advances on this.';

CREATE TABLE IF NOT EXISTS transformed."fluid_vault_state" AS
SELECT p."chain_id",
       p."protocol_id",
       s."fluid_vault_id",
       s."block_number",
       s."block_version",
       s."block_timestamp",
       s."total_collateral",
       s."total_debt",
       s."supply_exchange_price",
       s."borrow_exchange_price",
       s."supply_rate",
       s."borrow_rate",
       s."processing_version",
       s."build_id"
FROM public."fluid_vault_state" s LEFT JOIN public."fluid_vault" p ON p."id"=s."fluid_vault_id" WHERE false;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='transformed."fluid_vault_state"'::regclass AND contype='p') THEN ALTER TABLE transformed."fluid_vault_state" ALTER COLUMN "block_timestamp" SET NOT NULL, ADD PRIMARY KEY ("fluid_vault_id", "block_number", "block_version", "block_timestamp", "processing_version"); END IF; END $$;
SELECT create_hypertable('transformed."fluid_vault_state"','block_timestamp',chunk_time_interval=>INTERVAL '30 days',if_not_exists=>TRUE);
ALTER TABLE transformed."fluid_vault_state" SET (timescaledb.compress, timescaledb.compress_segmentby = 'fluid_vault_id', timescaledb.compress_orderby = 'block_timestamp DESC, processing_version DESC');
SELECT add_compression_policy('transformed."fluid_vault_state"', INTERVAL '14 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."fluid_vault_state"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.fluid_vault_state'; END $$;
INSERT INTO transformed._watermark(source) VALUES ('fluid_vault_state') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_fluid_vault_state() RETURNS bigint AS $fn$
DECLARE bw int; n bigint; mx int;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='fluid_vault_state';
  WITH ins AS (
  INSERT INTO transformed."fluid_vault_state"
  SELECT p."chain_id",
       p."protocol_id",
       s."fluid_vault_id",
       s."block_number",
       s."block_version",
       s."block_timestamp",
       s."total_collateral",
       s."total_debt",
       s."supply_exchange_price",
       s."borrow_exchange_price",
       s."supply_rate",
       s."borrow_rate",
       s."processing_version",
       s."build_id"
  FROM public."fluid_vault_state" s LEFT JOIN public."fluid_vault" p ON p."id"=s."fluid_vault_id" WHERE (s."build_id" >= bw OR s."build_id" IS NULL)
  ON CONFLICT ("fluid_vault_id", "block_number", "block_version", "block_timestamp", "processing_version") DO UPDATE SET "total_collateral"=EXCLUDED."total_collateral", "total_debt"=EXCLUDED."total_debt", "supply_exchange_price"=EXCLUDED."supply_exchange_price", "borrow_exchange_price"=EXCLUDED."borrow_exchange_price", "supply_rate"=EXCLUDED."supply_rate", "borrow_rate"=EXCLUDED."borrow_rate", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id"
  RETURNING "build_id")
  SELECT count(*), max("build_id") INTO n, mx FROM ins;
  UPDATE transformed._watermark SET build_wm=GREATEST(build_wm, COALESCE(mx, build_wm)) WHERE source='fluid_vault_state';
  RETURN n;
END $fn$ LANGUAGE plpgsql;
COMMENT ON TABLE transformed."fluid_vault_state" IS
  '[Hypertable] Canonical transform of public.fluid_vault_state (VEC-484): per-vault aggregate snapshot (total collateral + total debt across all borrowers) at a single block. chain_id/protocol_id joined from fluid_vault. Per-borrower positions are not modelled (VEC-436). Partition: block_timestamp.';
COMMENT ON COLUMN transformed."fluid_vault_state".chain_id IS 'Derived. FK->chain.chain_id, joined from fluid_vault. Chain scope introduced by the transform.';
COMMENT ON COLUMN transformed."fluid_vault_state".protocol_id IS 'Derived. FK->protocol.id, joined from fluid_vault. Protocol scope introduced by the transform.';
COMMENT ON COLUMN transformed."fluid_vault_state".fluid_vault_id IS 'FK->fluid_vault.id. Part of PK.';
COMMENT ON COLUMN transformed."fluid_vault_state".block_number IS 'Block height of the snapshot. Part of PK.';
COMMENT ON COLUMN transformed."fluid_vault_state".block_version IS 'Reorg version: increments when a block is re-indexed after a reorg. Part of PK; a new version inserts cleanly rather than overwriting.';
COMMENT ON COLUMN transformed."fluid_vault_state".block_timestamp IS 'Partition. Block timestamp (UTC). Part of PK.';
COMMENT ON COLUMN transformed."fluid_vault_state".total_collateral IS 'Vault total collateral across all borrowers, raw integer in the collateral token''s native decimals (unscaled).';
COMMENT ON COLUMN transformed."fluid_vault_state".total_debt IS 'Vault total debt across all borrowers, raw integer in the debt token''s native decimals (unscaled).';
COMMENT ON COLUMN transformed."fluid_vault_state".supply_exchange_price IS 'Fluid VaultResolver supply exchange price, raw on-chain value (unscaled); NULL when the snapshot did not capture it.';
COMMENT ON COLUMN transformed."fluid_vault_state".borrow_exchange_price IS 'Fluid VaultResolver borrow exchange price, raw on-chain value (unscaled); NULL when the snapshot did not capture it.';
COMMENT ON COLUMN transformed."fluid_vault_state".supply_rate IS 'Fluid VaultResolver supply rate, raw on-chain value (unscaled); NULL when the snapshot did not capture it.';
COMMENT ON COLUMN transformed."fluid_vault_state".borrow_rate IS 'Fluid VaultResolver borrow rate, raw on-chain value (unscaled); NULL when the snapshot did not capture it.';
COMMENT ON COLUMN transformed."fluid_vault_state".processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by block_timestamp DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN transformed."fluid_vault_state".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row. transformed._watermark advances on this.';

CREATE TABLE IF NOT EXISTS transformed."token_total_supply" AS
SELECT "chain_id",
       "token_id",
       "total_supply",
       "scaled_total_supply",
       "block_number",
       "block_version",
       "block_timestamp",
       "source",
       "processing_version",
       "build_id",
       "created_at"
FROM public."token_total_supply" WHERE false;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='transformed."token_total_supply"'::regclass AND contype='p') THEN ALTER TABLE transformed."token_total_supply" ALTER COLUMN "block_timestamp" SET NOT NULL, ADD PRIMARY KEY ("chain_id", "token_id", "block_number", "block_version", "processing_version", "block_timestamp"); END IF; END $$;
SELECT create_hypertable('transformed."token_total_supply"','block_timestamp',chunk_time_interval=>INTERVAL '30 days',if_not_exists=>TRUE);
ALTER TABLE transformed."token_total_supply" SET (timescaledb.compress, timescaledb.compress_segmentby = 'chain_id, token_id', timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC');
SELECT add_compression_policy('transformed."token_total_supply"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."token_total_supply"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.token_total_supply'; END $$;
INSERT INTO transformed._watermark(source) VALUES ('token_total_supply') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_token_total_supply() RETURNS bigint AS $fn$
DECLARE bw int; n bigint; mx int;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='token_total_supply';
  WITH ins AS (
  INSERT INTO transformed."token_total_supply"
  SELECT "chain_id",
       "token_id",
       "total_supply",
       "scaled_total_supply",
       "block_number",
       "block_version",
       "block_timestamp",
       "source",
       "processing_version",
       "build_id",
       "created_at"
  FROM public."token_total_supply" WHERE ("build_id" >= bw OR "build_id" IS NULL)
  ON CONFLICT ("chain_id", "token_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "total_supply"=EXCLUDED."total_supply", "scaled_total_supply"=EXCLUDED."scaled_total_supply", "source"=EXCLUDED."source", "build_id"=EXCLUDED."build_id", "created_at"=EXCLUDED."created_at"
  RETURNING "build_id")
  SELECT count(*), max("build_id") INTO n, mx FROM ins;
  UPDATE transformed._watermark SET build_wm=GREATEST(build_wm, COALESCE(mx, build_wm)) WHERE source='token_total_supply';
  RETURN n;
END $fn$ LANGUAGE plpgsql;
COMMENT ON TABLE transformed."token_total_supply" IS
  '[Hypertable] Canonical transform of public.token_total_supply (VEC-484): per-block total circulating supply for tracked tokens. Native chain_id/token_id (no join). Partition: block_timestamp.';
COMMENT ON COLUMN transformed."token_total_supply".chain_id IS 'FK->chain.chain_id. Part of PK. Native column (not derived).';
COMMENT ON COLUMN transformed."token_total_supply".token_id IS 'FK->token.id. Part of PK.';
COMMENT ON COLUMN transformed."token_total_supply".total_supply IS 'Total circulating supply, decimals-normalized to a human-readable value at write time (not a raw integer).';
COMMENT ON COLUMN transformed."token_total_supply".scaled_total_supply IS 'Nullable, decimals-normalized. On-chain scaledTotalSupply reading (interest-free), not computed from liquidity_index. Populated only for aTokens.';
COMMENT ON COLUMN transformed."token_total_supply".block_number IS 'Block height of the snapshot. Part of PK.';
COMMENT ON COLUMN transformed."token_total_supply".block_version IS 'Reorg version: increments when a block is re-indexed after a reorg. Part of PK.';
COMMENT ON COLUMN transformed."token_total_supply".block_timestamp IS 'Partition. On-chain block time. Part of PK.';
COMMENT ON COLUMN transformed."token_total_supply".source IS 'Trigger for this row: event = written in response to a Transfer log; sweep = written by a periodic poll. Both read totalSupply() on-chain at the row''s block.';
COMMENT ON COLUMN transformed."token_total_supply".processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by block_timestamp DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN transformed."token_total_supply".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row. transformed._watermark advances on this.';
COMMENT ON COLUMN transformed."token_total_supply".created_at IS 'Audit. Wall-clock time the raw row was written; carried through the transform.';

CREATE TABLE IF NOT EXISTS transformed."onchain_token_price" AS
SELECT p."chain_id",
       s."token_id",
       CAST(s."oracle_id" AS bigint) AS "oracle_id",
       s."block_number",
       CAST(s."block_version" AS integer) AS "block_version",
       s."timestamp" AS "block_timestamp",
       s."price_usd",
       s."processing_version",
       s."build_id"
FROM public."onchain_token_price" s LEFT JOIN public."oracle" p ON p."id"=s."oracle_id" WHERE false;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='transformed."onchain_token_price"'::regclass AND contype='p') THEN ALTER TABLE transformed."onchain_token_price" ALTER COLUMN "block_timestamp" SET NOT NULL, ADD PRIMARY KEY ("token_id", "oracle_id", "block_number", "block_version", "processing_version", "block_timestamp"); END IF; END $$;
SELECT create_hypertable('transformed."onchain_token_price"','block_timestamp',chunk_time_interval=>INTERVAL '30 days',if_not_exists=>TRUE);
ALTER TABLE transformed."onchain_token_price" SET (timescaledb.compress, timescaledb.compress_segmentby = 'oracle_id, token_id', timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC');
SELECT add_compression_policy('transformed."onchain_token_price"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."onchain_token_price"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.onchain_token_price'; END $$;
INSERT INTO transformed._watermark(source) VALUES ('onchain_token_price') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_onchain_token_price() RETURNS bigint AS $fn$
DECLARE bw int; n bigint; mx int;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='onchain_token_price';
  WITH ins AS (
  INSERT INTO transformed."onchain_token_price"
  SELECT p."chain_id",
       s."token_id",
       CAST(s."oracle_id" AS bigint) AS "oracle_id",
       s."block_number",
       CAST(s."block_version" AS integer) AS "block_version",
       s."timestamp" AS "block_timestamp",
       s."price_usd",
       s."processing_version",
       s."build_id"
  FROM public."onchain_token_price" s LEFT JOIN public."oracle" p ON p."id"=s."oracle_id" WHERE (s."build_id" >= bw OR s."build_id" IS NULL)
  ON CONFLICT ("token_id", "oracle_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "price_usd"=EXCLUDED."price_usd", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id"
  RETURNING "build_id")
  SELECT count(*), max("build_id") INTO n, mx FROM ins;
  UPDATE transformed._watermark SET build_wm=GREATEST(build_wm, COALESCE(mx, build_wm)) WHERE source='onchain_token_price';
  RETURN n;
END $fn$ LANGUAGE plpgsql;
COMMENT ON TABLE transformed."onchain_token_price" IS
  '[Hypertable] Canonical transform of public.onchain_token_price (VEC-484): oracle price reads per token per block (Aave oracle, Chainlink, Chronicle, Redstone). chain_id joined from oracle; oracle_id widened to bigint and block_version to integer (raw smallint, known inconsistency); raw "timestamp" renamed to block_timestamp. A raw row is written only when the value differs from the last cached price. Partition: block_timestamp.';
COMMENT ON COLUMN transformed."onchain_token_price".chain_id IS 'Derived. FK->chain.chain_id, joined from oracle. Chain scope introduced by the transform.';
COMMENT ON COLUMN transformed."onchain_token_price".token_id IS 'FK->token.id. Part of PK.';
COMMENT ON COLUMN transformed."onchain_token_price".oracle_id IS 'FK->oracle.id. Part of PK. Widened to bigint from the raw smallint (known raw inconsistency).';
COMMENT ON COLUMN transformed."onchain_token_price".block_number IS 'Block height of the snapshot. Part of PK.';
COMMENT ON COLUMN transformed."onchain_token_price".block_version IS 'Reorg version: increments when a block is re-indexed after a reorg. Part of PK. Widened to integer from the raw smallint (known raw inconsistency).';
COMMENT ON COLUMN transformed."onchain_token_price".block_timestamp IS 'Partition. On-chain block time, renamed from raw "timestamp". Part of PK.';
COMMENT ON COLUMN transformed."onchain_token_price".price_usd IS 'USD price, already decimal-adjusted at write time (raw feed value divided by that feed''s decimals). Stored as NUMERIC(30,18), e.g. 1.23379556; use it as-is, do not divide again. Zero or implausible values corrupt downstream calculations.';
COMMENT ON COLUMN transformed."onchain_token_price".processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by block_timestamp DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN transformed."onchain_token_price".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row. transformed._watermark advances on this.';

CREATE TABLE IF NOT EXISTS transformed."maple_loan_state" AS
SELECT p."chain_id",
       p."protocol_id",
       s."maple_loan_id",
       s."synced_at" AS "snapshot_time",
       s."state",
       s."principal_owed",
       s."acm_ratio",
       s."processing_version",
       s."build_id"
FROM public."maple_loan_state" s LEFT JOIN public."maple_loan" p ON p."id"=s."maple_loan_id" WHERE false;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='transformed."maple_loan_state"'::regclass AND contype='p') THEN ALTER TABLE transformed."maple_loan_state" ALTER COLUMN "snapshot_time" SET NOT NULL, ADD PRIMARY KEY ("maple_loan_id", "snapshot_time", "processing_version"); END IF; END $$;
SELECT create_hypertable('transformed."maple_loan_state"','snapshot_time',chunk_time_interval=>INTERVAL '30 days',if_not_exists=>TRUE);
ALTER TABLE transformed."maple_loan_state" SET (timescaledb.compress, timescaledb.compress_segmentby = 'maple_loan_id', timescaledb.compress_orderby = 'snapshot_time DESC, processing_version DESC');
SELECT add_compression_policy('transformed."maple_loan_state"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."maple_loan_state"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.maple_loan_state'; END $$;
INSERT INTO transformed._watermark(source) VALUES ('maple_loan_state') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_maple_loan_state() RETURNS bigint AS $fn$
DECLARE bw int; n bigint; mx int;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='maple_loan_state';
  WITH ins AS (
  INSERT INTO transformed."maple_loan_state"
  SELECT p."chain_id",
       p."protocol_id",
       s."maple_loan_id",
       s."synced_at" AS "snapshot_time",
       s."state",
       s."principal_owed",
       s."acm_ratio",
       s."processing_version",
       s."build_id"
  FROM public."maple_loan_state" s LEFT JOIN public."maple_loan" p ON p."id"=s."maple_loan_id" WHERE (s."build_id" >= bw OR s."build_id" IS NULL)
  ON CONFLICT ("maple_loan_id", "snapshot_time", "processing_version") DO UPDATE SET "state"=EXCLUDED."state", "principal_owed"=EXCLUDED."principal_owed", "acm_ratio"=EXCLUDED."acm_ratio", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id"
  RETURNING "build_id")
  SELECT count(*), max("build_id") INTO n, mx FROM ins;
  UPDATE transformed._watermark SET build_wm=GREATEST(build_wm, COALESCE(mx, build_wm)) WHERE source='maple_loan_state';
  RETURN n;
END $fn$ LANGUAGE plpgsql;
COMMENT ON TABLE transformed."maple_loan_state" IS
  '[Hypertable] Canonical transform of public.maple_loan_state (VEC-484): per-cycle snapshot of an Open Term Loan. chain_id/protocol_id joined from maple_loan; raw "synced_at" renamed to snapshot_time. Only Active loans are queried, so a loan absent at a given snapshot_time is no longer active. Partition: snapshot_time.';
COMMENT ON COLUMN transformed."maple_loan_state".chain_id IS 'Derived. FK->chain.chain_id, joined from maple_loan. Chain scope introduced by the transform.';
COMMENT ON COLUMN transformed."maple_loan_state".protocol_id IS 'Derived. FK->protocol.id, joined from maple_loan. Protocol scope introduced by the transform.';
COMMENT ON COLUMN transformed."maple_loan_state".maple_loan_id IS 'FK->maple_loan.id. Part of PK.';
COMMENT ON COLUMN transformed."maple_loan_state".snapshot_time IS 'Partition. Cron-cycle timestamp (UTC), renamed from raw "synced_at". Part of PK.';
COMMENT ON COLUMN transformed."maple_loan_state".state IS 'LoanState enum; currently only ''Active'' is queried.';
COMMENT ON COLUMN transformed."maple_loan_state".principal_owed IS 'Outstanding principal, raw integer in pool-asset native decimals (USDC/USDT = 6).';
COMMENT ON COLUMN transformed."maple_loan_state".acm_ratio IS 'Asset Coverage Margin ratio, fixed-point x1e6 (6 decimals; 1445731 = 144.57%). NULL on uncollateralized loans.';
COMMENT ON COLUMN transformed."maple_loan_state".processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by snapshot_time DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN transformed."maple_loan_state".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row. transformed._watermark advances on this.';

CREATE TABLE IF NOT EXISTS transformed."maple_loan_collateral" AS
SELECT p."chain_id",
       p."protocol_id",
       s."maple_loan_id",
       s."synced_at" AS "snapshot_time",
       s."asset_symbol",
       s."asset_amount",
       s."asset_decimals",
       s."asset_value_usd",
       s."state",
       s."custodian",
       s."liquidation_level",
       s."processing_version",
       s."build_id"
FROM public."maple_loan_collateral" s LEFT JOIN public."maple_loan" p ON p."id"=s."maple_loan_id" WHERE false;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='transformed."maple_loan_collateral"'::regclass AND contype='p') THEN ALTER TABLE transformed."maple_loan_collateral" ALTER COLUMN "snapshot_time" SET NOT NULL, ADD PRIMARY KEY ("maple_loan_id", "snapshot_time", "processing_version"); END IF; END $$;
SELECT create_hypertable('transformed."maple_loan_collateral"','snapshot_time',chunk_time_interval=>INTERVAL '30 days',if_not_exists=>TRUE);
ALTER TABLE transformed."maple_loan_collateral" SET (timescaledb.compress, timescaledb.compress_segmentby = 'maple_loan_id', timescaledb.compress_orderby = 'snapshot_time DESC, processing_version DESC');
SELECT add_compression_policy('transformed."maple_loan_collateral"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."maple_loan_collateral"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.maple_loan_collateral'; END $$;
INSERT INTO transformed._watermark(source) VALUES ('maple_loan_collateral') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_maple_loan_collateral() RETURNS bigint AS $fn$
DECLARE bw int; n bigint; mx int;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='maple_loan_collateral';
  WITH ins AS (
  INSERT INTO transformed."maple_loan_collateral"
  SELECT p."chain_id",
       p."protocol_id",
       s."maple_loan_id",
       s."synced_at" AS "snapshot_time",
       s."asset_symbol",
       s."asset_amount",
       s."asset_decimals",
       s."asset_value_usd",
       s."state",
       s."custodian",
       s."liquidation_level",
       s."processing_version",
       s."build_id"
  FROM public."maple_loan_collateral" s LEFT JOIN public."maple_loan" p ON p."id"=s."maple_loan_id" WHERE (s."build_id" >= bw OR s."build_id" IS NULL)
  ON CONFLICT ("maple_loan_id", "snapshot_time", "processing_version") DO UPDATE SET "asset_symbol"=EXCLUDED."asset_symbol", "asset_amount"=EXCLUDED."asset_amount", "asset_decimals"=EXCLUDED."asset_decimals", "asset_value_usd"=EXCLUDED."asset_value_usd", "state"=EXCLUDED."state", "custodian"=EXCLUDED."custodian", "liquidation_level"=EXCLUDED."liquidation_level", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id"
  RETURNING "build_id")
  SELECT count(*), max("build_id") INTO n, mx FROM ins;
  UPDATE transformed._watermark SET build_wm=GREATEST(build_wm, COALESCE(mx, build_wm)) WHERE source='maple_loan_collateral';
  RETURN n;
END $fn$ LANGUAGE plpgsql;
COMMENT ON TABLE transformed."maple_loan_collateral" IS
  '[Hypertable] Canonical transform of public.maple_loan_collateral (VEC-484): per-cycle snapshot of an Open Term Loan''s collateral. chain_id/protocol_id joined from maple_loan; raw "synced_at" renamed to snapshot_time. Collateral is off-chain custodied (BTC/SOL), stored by symbol, not a token FK. Partition: snapshot_time.';
COMMENT ON COLUMN transformed."maple_loan_collateral".chain_id IS 'Derived. FK->chain.chain_id, joined from maple_loan. Chain scope introduced by the transform.';
COMMENT ON COLUMN transformed."maple_loan_collateral".protocol_id IS 'Derived. FK->protocol.id, joined from maple_loan. Protocol scope introduced by the transform.';
COMMENT ON COLUMN transformed."maple_loan_collateral".maple_loan_id IS 'FK->maple_loan.id. Part of PK.';
COMMENT ON COLUMN transformed."maple_loan_collateral".snapshot_time IS 'Partition. Cron-cycle timestamp (UTC), renamed from raw "synced_at". Part of PK.';
COMMENT ON COLUMN transformed."maple_loan_collateral".asset_symbol IS 'Collateral asset symbol (e.g. BTC, SOL); off-chain custodied, no Ethereum address.';
COMMENT ON COLUMN transformed."maple_loan_collateral".asset_amount IS 'Collateral amount, raw integer in asset_decimals native decimals. NULL when the API reports null (e.g. DepositPending).';
COMMENT ON COLUMN transformed."maple_loan_collateral".asset_decimals IS 'Native decimals of the collateral asset; use to scale asset_amount.';
COMMENT ON COLUMN transformed."maple_loan_collateral".asset_value_usd IS 'Per-unit USD price of the collateral asset, fixed-point x1e8 (8 decimals) - NOT a total. Multiply by asset_amount (scaled) for total USD. NULL when the API reports null.';
COMMENT ON COLUMN transformed."maple_loan_collateral".state IS 'Collateral state (e.g. ''Deposited'' | ''DepositPending''); nullable.';
COMMENT ON COLUMN transformed."maple_loan_collateral".custodian IS 'Custodian name (e.g. ''FORDEFI'', ''ANCHORAGE''); nullable.';
COMMENT ON COLUMN transformed."maple_loan_collateral".liquidation_level IS 'Liquidation level from the API (arrives as a JSON number, e.g. 900000); nullable.';
COMMENT ON COLUMN transformed."maple_loan_collateral".processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by snapshot_time DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN transformed."maple_loan_collateral".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row. transformed._watermark advances on this.';

CREATE TABLE IF NOT EXISTS transformed."maple_pool_state" AS
SELECT p."chain_id",
       p."protocol_id",
       s."maple_pool_id",
       s."synced_at" AS "snapshot_time",
       s."tvl",
       s."liquid_assets",
       s."collateral_value_usd",
       s."principal_out",
       s."utilization",
       s."monthly_apy",
       s."spot_apy",
       s."processing_version",
       s."build_id"
FROM public."maple_pool_state" s LEFT JOIN public."maple_pool" p ON p."id"=s."maple_pool_id" WHERE false;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='transformed."maple_pool_state"'::regclass AND contype='p') THEN ALTER TABLE transformed."maple_pool_state" ALTER COLUMN "snapshot_time" SET NOT NULL, ADD PRIMARY KEY ("maple_pool_id", "snapshot_time", "processing_version"); END IF; END $$;
SELECT create_hypertable('transformed."maple_pool_state"','snapshot_time',chunk_time_interval=>INTERVAL '30 days',if_not_exists=>TRUE);
ALTER TABLE transformed."maple_pool_state" SET (timescaledb.compress, timescaledb.compress_segmentby = 'maple_pool_id', timescaledb.compress_orderby = 'snapshot_time DESC, processing_version DESC');
SELECT add_compression_policy('transformed."maple_pool_state"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."maple_pool_state"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.maple_pool_state'; END $$;
INSERT INTO transformed._watermark(source) VALUES ('maple_pool_state') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_maple_pool_state() RETURNS bigint AS $fn$
DECLARE bw int; n bigint; mx int;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='maple_pool_state';
  WITH ins AS (
  INSERT INTO transformed."maple_pool_state"
  SELECT p."chain_id",
       p."protocol_id",
       s."maple_pool_id",
       s."synced_at" AS "snapshot_time",
       s."tvl",
       s."liquid_assets",
       s."collateral_value_usd",
       s."principal_out",
       s."utilization",
       s."monthly_apy",
       s."spot_apy",
       s."processing_version",
       s."build_id"
  FROM public."maple_pool_state" s LEFT JOIN public."maple_pool" p ON p."id"=s."maple_pool_id" WHERE (s."build_id" >= bw OR s."build_id" IS NULL)
  ON CONFLICT ("maple_pool_id", "snapshot_time", "processing_version") DO UPDATE SET "tvl"=EXCLUDED."tvl", "liquid_assets"=EXCLUDED."liquid_assets", "collateral_value_usd"=EXCLUDED."collateral_value_usd", "principal_out"=EXCLUDED."principal_out", "utilization"=EXCLUDED."utilization", "monthly_apy"=EXCLUDED."monthly_apy", "spot_apy"=EXCLUDED."spot_apy", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id"
  RETURNING "build_id")
  SELECT count(*), max("build_id") INTO n, mx FROM ins;
  UPDATE transformed._watermark SET build_wm=GREATEST(build_wm, COALESCE(mx, build_wm)) WHERE source='maple_pool_state';
  RETURN n;
END $fn$ LANGUAGE plpgsql;
COMMENT ON TABLE transformed."maple_pool_state" IS
  '[Hypertable] Canonical transform of public.maple_pool_state (VEC-484): per-cycle snapshot of each PoolV2 pool''s lending metrics. chain_id/protocol_id joined from maple_pool; raw "synced_at" renamed to snapshot_time. No block_version: GraphQL data has no reorg concept. Partition: snapshot_time.';
COMMENT ON COLUMN transformed."maple_pool_state".chain_id IS 'Derived. FK->chain.chain_id, joined from maple_pool. Chain scope introduced by the transform.';
COMMENT ON COLUMN transformed."maple_pool_state".protocol_id IS 'Derived. FK->protocol.id, joined from maple_pool. Protocol scope introduced by the transform.';
COMMENT ON COLUMN transformed."maple_pool_state".maple_pool_id IS 'FK->maple_pool.id. Part of PK.';
COMMENT ON COLUMN transformed."maple_pool_state".snapshot_time IS 'Partition. Cron-cycle timestamp (UTC), shared by every row of one sync cycle, renamed from raw "synced_at". Part of PK.';
COMMENT ON COLUMN transformed."maple_pool_state".tvl IS 'Total value locked, raw integer in pool-asset native decimals (USDC/USDT = 6). Nullable in the API schema.';
COMMENT ON COLUMN transformed."maple_pool_state".liquid_assets IS 'Liquid pool cash (poolV2.assets), raw integer in pool-asset native decimals.';
COMMENT ON COLUMN transformed."maple_pool_state".collateral_value_usd IS 'USD value of loan collateral, raw integer in pool-asset native decimals. Nullable in the API schema.';
COMMENT ON COLUMN transformed."maple_pool_state".principal_out IS 'Outstanding loan principal, raw integer in pool-asset native decimals.';
COMMENT ON COLUMN transformed."maple_pool_state".utilization IS 'Derived fraction 0-1: principal_out / (liquid_assets + principal_out); 0 when the pool is empty. Not scaled.';
COMMENT ON COLUMN transformed."maple_pool_state".monthly_apy IS '30-day historical average APY, fixed-point x1e30 (30 decimals). Nullable.';
COMMENT ON COLUMN transformed."maple_pool_state".spot_apy IS 'Current spot APY, fixed-point x1e30 (30 decimals). Nullable.';
COMMENT ON COLUMN transformed."maple_pool_state".processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by snapshot_time DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN transformed."maple_pool_state".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row. transformed._watermark advances on this.';

CREATE TABLE IF NOT EXISTS transformed."maple_sky_strategy_state" AS
SELECT p."chain_id",
       (SELECT l."protocol_id" FROM public."maple_pool" l WHERE l."id"=p."maple_pool_id") AS "protocol_id",
       s."maple_sky_strategy_id",
       s."synced_at" AS "snapshot_time",
       s."state",
       s."currently_deployed",
       s."deposited_assets",
       s."withdrawn_assets",
       s."strategy_fee_rate",
       s."total_fees_collected",
       s."processing_version",
       s."build_id"
FROM public."maple_sky_strategy_state" s LEFT JOIN public."maple_sky_strategy" p ON p."id"=s."maple_sky_strategy_id" WHERE false;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='transformed."maple_sky_strategy_state"'::regclass AND contype='p') THEN ALTER TABLE transformed."maple_sky_strategy_state" ALTER COLUMN "snapshot_time" SET NOT NULL, ADD PRIMARY KEY ("maple_sky_strategy_id", "snapshot_time", "processing_version"); END IF; END $$;
SELECT create_hypertable('transformed."maple_sky_strategy_state"','snapshot_time',chunk_time_interval=>INTERVAL '30 days',if_not_exists=>TRUE);
ALTER TABLE transformed."maple_sky_strategy_state" SET (timescaledb.compress, timescaledb.compress_segmentby = 'maple_sky_strategy_id', timescaledb.compress_orderby = 'snapshot_time DESC, processing_version DESC');
SELECT add_compression_policy('transformed."maple_sky_strategy_state"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."maple_sky_strategy_state"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.maple_sky_strategy_state'; END $$;
INSERT INTO transformed._watermark(source) VALUES ('maple_sky_strategy_state') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_maple_sky_strategy_state() RETURNS bigint AS $fn$
DECLARE bw int; n bigint; mx int;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='maple_sky_strategy_state';
  WITH ins AS (
  INSERT INTO transformed."maple_sky_strategy_state"
  SELECT p."chain_id",
       (SELECT l."protocol_id" FROM public."maple_pool" l WHERE l."id"=p."maple_pool_id") AS "protocol_id",
       s."maple_sky_strategy_id",
       s."synced_at" AS "snapshot_time",
       s."state",
       s."currently_deployed",
       s."deposited_assets",
       s."withdrawn_assets",
       s."strategy_fee_rate",
       s."total_fees_collected",
       s."processing_version",
       s."build_id"
  FROM public."maple_sky_strategy_state" s LEFT JOIN public."maple_sky_strategy" p ON p."id"=s."maple_sky_strategy_id" WHERE (s."build_id" >= bw OR s."build_id" IS NULL)
  ON CONFLICT ("maple_sky_strategy_id", "snapshot_time", "processing_version") DO UPDATE SET "state"=EXCLUDED."state", "currently_deployed"=EXCLUDED."currently_deployed", "deposited_assets"=EXCLUDED."deposited_assets", "withdrawn_assets"=EXCLUDED."withdrawn_assets", "strategy_fee_rate"=EXCLUDED."strategy_fee_rate", "total_fees_collected"=EXCLUDED."total_fees_collected", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id"
  RETURNING "build_id")
  SELECT count(*), max("build_id") INTO n, mx FROM ins;
  UPDATE transformed._watermark SET build_wm=GREATEST(build_wm, COALESCE(mx, build_wm)) WHERE source='maple_sky_strategy_state';
  RETURN n;
END $fn$ LANGUAGE plpgsql;
COMMENT ON TABLE transformed."maple_sky_strategy_state" IS
  '[Hypertable] Canonical transform of public.maple_sky_strategy_state (VEC-484): per-cycle snapshot of a Sky strategy''s deployment metrics. chain_id joined from maple_sky_strategy; protocol_id resolved via the strategy''s maple_pool (second-hop fill); raw "synced_at" renamed to snapshot_time. Partition: snapshot_time.';
COMMENT ON COLUMN transformed."maple_sky_strategy_state".chain_id IS 'Derived. FK->chain.chain_id, joined from maple_sky_strategy. Chain scope introduced by the transform.';
COMMENT ON COLUMN transformed."maple_sky_strategy_state".protocol_id IS 'Derived. FK->protocol.id, resolved via maple_sky_strategy -> maple_pool.protocol_id (second-hop fill).';
COMMENT ON COLUMN transformed."maple_sky_strategy_state".maple_sky_strategy_id IS 'FK->maple_sky_strategy.id. Part of PK.';
COMMENT ON COLUMN transformed."maple_sky_strategy_state".snapshot_time IS 'Partition. Cron-cycle timestamp (UTC), renamed from raw "synced_at". Part of PK.';
COMMENT ON COLUMN transformed."maple_sky_strategy_state".state IS 'Strategy state string.';
COMMENT ON COLUMN transformed."maple_sky_strategy_state".currently_deployed IS 'Amount currently deployed, raw integer in pool-asset native decimals. 0 across all strategies as of 2026-06-15 (dormant).';
COMMENT ON COLUMN transformed."maple_sky_strategy_state".deposited_assets IS 'Cumulative assets deposited, raw integer in pool-asset native decimals.';
COMMENT ON COLUMN transformed."maple_sky_strategy_state".withdrawn_assets IS 'Cumulative assets withdrawn, raw integer in pool-asset native decimals.';
COMMENT ON COLUMN transformed."maple_sky_strategy_state".strategy_fee_rate IS 'Strategy fee rate, fixed-point x1e6 (6 decimals; observed 100000); nullable.';
COMMENT ON COLUMN transformed."maple_sky_strategy_state".total_fees_collected IS 'Cumulative fees collected, raw integer in pool-asset native decimals; nullable.';
COMMENT ON COLUMN transformed."maple_sky_strategy_state".processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by snapshot_time DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN transformed."maple_sky_strategy_state".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row. transformed._watermark advances on this.';

CREATE TABLE IF NOT EXISTS transformed."maple_syrup_global_state" AS
SELECT "chain_id",
       "synced_at" AS "snapshot_time",
       "tvl",
       "apy",
       "collateral_apy",
       "pool_apy",
       "drips_yield_boost",
       "processing_version",
       "build_id"
FROM public."maple_syrup_global_state" WHERE false;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='transformed."maple_syrup_global_state"'::regclass AND contype='p') THEN ALTER TABLE transformed."maple_syrup_global_state" ALTER COLUMN "snapshot_time" SET NOT NULL, ADD PRIMARY KEY ("chain_id", "snapshot_time", "processing_version"); END IF; END $$;
SELECT create_hypertable('transformed."maple_syrup_global_state"','snapshot_time',chunk_time_interval=>INTERVAL '30 days',if_not_exists=>TRUE);
ALTER TABLE transformed."maple_syrup_global_state" SET (timescaledb.compress, timescaledb.compress_segmentby = 'chain_id', timescaledb.compress_orderby = 'snapshot_time DESC, processing_version DESC');
SELECT add_compression_policy('transformed."maple_syrup_global_state"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."maple_syrup_global_state"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.maple_syrup_global_state'; END $$;
INSERT INTO transformed._watermark(source) VALUES ('maple_syrup_global_state') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_maple_syrup_global_state() RETURNS bigint AS $fn$
DECLARE bw int; n bigint; mx int;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='maple_syrup_global_state';
  WITH ins AS (
  INSERT INTO transformed."maple_syrup_global_state"
  SELECT "chain_id",
       "synced_at" AS "snapshot_time",
       "tvl",
       "apy",
       "collateral_apy",
       "pool_apy",
       "drips_yield_boost",
       "processing_version",
       "build_id"
  FROM public."maple_syrup_global_state" WHERE ("build_id" >= bw OR "build_id" IS NULL)
  ON CONFLICT ("chain_id", "snapshot_time", "processing_version") DO UPDATE SET "tvl"=EXCLUDED."tvl", "apy"=EXCLUDED."apy", "collateral_apy"=EXCLUDED."collateral_apy", "pool_apy"=EXCLUDED."pool_apy", "drips_yield_boost"=EXCLUDED."drips_yield_boost", "build_id"=EXCLUDED."build_id"
  RETURNING "build_id")
  SELECT count(*), max("build_id") INTO n, mx FROM ins;
  UPDATE transformed._watermark SET build_wm=GREATEST(build_wm, COALESCE(mx, build_wm)) WHERE source='maple_syrup_global_state';
  RETURN n;
END $fn$ LANGUAGE plpgsql;
COMMENT ON TABLE transformed."maple_syrup_global_state" IS
  '[Hypertable] Canonical transform of public.maple_syrup_global_state (VEC-484): per-cycle snapshot of Syrup protocol-wide aggregates, one per chain per cycle. Native chain_id; raw "synced_at" renamed to snapshot_time. Partition: snapshot_time.';
COMMENT ON COLUMN transformed."maple_syrup_global_state".chain_id IS 'FK->chain.chain_id. Part of PK. Native column (not derived).';
COMMENT ON COLUMN transformed."maple_syrup_global_state".snapshot_time IS 'Partition. Cron-cycle timestamp (UTC), renamed from raw "synced_at". Part of PK.';
COMMENT ON COLUMN transformed."maple_syrup_global_state".tvl IS 'Syrup-wide total value locked, raw integer in pool-asset native decimals.';
COMMENT ON COLUMN transformed."maple_syrup_global_state".apy IS 'Overall Syrup APY, fixed-point x1e30 (30 decimals).';
COMMENT ON COLUMN transformed."maple_syrup_global_state".collateral_apy IS 'APY contribution from collateral, fixed-point x1e30 (30 decimals).';
COMMENT ON COLUMN transformed."maple_syrup_global_state".pool_apy IS 'APY contribution from pool lending, fixed-point x1e30 (30 decimals).';
COMMENT ON COLUMN transformed."maple_syrup_global_state".drips_yield_boost IS 'Additional yield from DRIPS; nullable. Scale unconfirmed (pending baseline, ORB-145) - verify before computing.';
COMMENT ON COLUMN transformed."maple_syrup_global_state".processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by snapshot_time DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN transformed."maple_syrup_global_state".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row. transformed._watermark advances on this.';

CREATE TABLE IF NOT EXISTS transformed."offchain_token_price" AS
SELECT "token_id",
       CAST("source_id" AS bigint) AS "source_id",
       "timestamp" AS "snapshot_time",
       "price_usd",
       "market_cap_usd",
       "volume_usd",
       "processing_version",
       "build_id"
FROM public."offchain_token_price" WHERE false;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='transformed."offchain_token_price"'::regclass AND contype='p') THEN ALTER TABLE transformed."offchain_token_price" ALTER COLUMN "snapshot_time" SET NOT NULL, ADD PRIMARY KEY ("token_id", "source_id", "processing_version", "snapshot_time"); END IF; END $$;
SELECT create_hypertable('transformed."offchain_token_price"','snapshot_time',chunk_time_interval=>INTERVAL '30 days',if_not_exists=>TRUE);
ALTER TABLE transformed."offchain_token_price" SET (timescaledb.compress, timescaledb.compress_segmentby = 'token_id', timescaledb.compress_orderby = 'snapshot_time DESC, processing_version DESC');
SELECT add_compression_policy('transformed."offchain_token_price"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."offchain_token_price"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.offchain_token_price'; END $$;
INSERT INTO transformed._watermark(source) VALUES ('offchain_token_price') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_offchain_token_price() RETURNS bigint AS $fn$
DECLARE bw int; n bigint; mx int;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='offchain_token_price';
  WITH ins AS (
  INSERT INTO transformed."offchain_token_price"
  SELECT "token_id",
       CAST("source_id" AS bigint) AS "source_id",
       "timestamp" AS "snapshot_time",
       "price_usd",
       "market_cap_usd",
       "volume_usd",
       "processing_version",
       "build_id"
  FROM public."offchain_token_price" WHERE ("build_id" >= bw OR "build_id" IS NULL)
  ON CONFLICT ("token_id", "source_id", "processing_version", "snapshot_time") DO UPDATE SET "price_usd"=EXCLUDED."price_usd", "market_cap_usd"=EXCLUDED."market_cap_usd", "volume_usd"=EXCLUDED."volume_usd", "build_id"=EXCLUDED."build_id"
  RETURNING "build_id")
  SELECT count(*), max("build_id") INTO n, mx FROM ins;
  UPDATE transformed._watermark SET build_wm=GREATEST(build_wm, COALESCE(mx, build_wm)) WHERE source='offchain_token_price';
  RETURN n;
END $fn$ LANGUAGE plpgsql;
COMMENT ON TABLE transformed."offchain_token_price" IS
  '[Hypertable] Canonical transform of public.offchain_token_price (VEC-484): API-polled token prices from off-chain providers (CoinGecko), used for cross-validation of on-chain oracle prices. No chain_id (off-chain, required-key exempt); source_id widened to bigint from the raw smallint; raw "timestamp" renamed to snapshot_time. Partition: snapshot_time.';
COMMENT ON COLUMN transformed."offchain_token_price".token_id IS 'FK->token.id. Part of PK.';
COMMENT ON COLUMN transformed."offchain_token_price".source_id IS 'FK->offchain_price_source.id. Part of PK. Widened to bigint from the raw smallint (known raw inconsistency).';
COMMENT ON COLUMN transformed."offchain_token_price".snapshot_time IS 'Partition. API observation time, renamed from raw "timestamp". Part of PK.';
COMMENT ON COLUMN transformed."offchain_token_price".price_usd IS 'USD price from off-chain provider. Divergence >5% from onchain_token_price signals a potentially stale oracle.';
COMMENT ON COLUMN transformed."offchain_token_price".market_cap_usd IS 'Market capitalization in USD from the provider; mirrored 1:1 from public.offchain_token_price (raw column undocumented - verify scale before computing).';
COMMENT ON COLUMN transformed."offchain_token_price".volume_usd IS '24h trading volume. Currently always null; not populated by the CoinGecko integration.';
COMMENT ON COLUMN transformed."offchain_token_price".processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by snapshot_time DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN transformed."offchain_token_price".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row. transformed._watermark advances on this.';

-- Grants: the worker connects as stl_readwrite and runs the _run_<t>() functions
-- (SECURITY INVOKER), so it needs schema usage, table read/write, and EXECUTE.
-- stl_readonly gets read access for downstream consumers of the transformed layer.
GRANT USAGE ON SCHEMA transformed TO stl_readonly, stl_readwrite;
GRANT SELECT ON ALL TABLES IN SCHEMA transformed TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA transformed TO stl_readwrite;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA transformed TO stl_readwrite;

INSERT INTO migrations (filename)
VALUES ('20260706_140000_create_transformed_bucket1.sql')
ON CONFLICT (filename) DO NOTHING;
