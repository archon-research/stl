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
-- build is reprocessed every run) and advances the watermark to max(build_id) of the transformed
-- table. Idempotent by construction: the upsert keys on the PK, so re-reading the current build
-- adds 0 new rows and refreshes any in-flight corrections. On-data 1:1 parity with raw is verified
-- on full data at rollout. Buckets 2 (block-time-dimension) and 3 (no-PK, keyed via
-- transform_config) follow separately.
--
-- This migration is idempotent and non-destructive: it creates the `transformed` schema and its
-- tables only if absent, guards the primary-key ALTERs, and passes if_not_exists to
-- create_hypertable. Re-application (e.g. in test harnesses that reset the public schema between
-- cases while the separate `transformed` schema survives) is a no-op and never drops data.

CREATE SCHEMA IF NOT EXISTS transformed;

CREATE TABLE IF NOT EXISTS transformed._watermark(source text PRIMARY KEY, build_wm int NOT NULL DEFAULT -1);

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
INSERT INTO transformed._watermark(source) VALUES ('morpho_market_state') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_morpho_market_state() RETURNS bigint AS $fn$
DECLARE bw int; n bigint;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='morpho_market_state';
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
  FROM public."morpho_market_state" s LEFT JOIN public."morpho_market" p ON p."id"=s."morpho_market_id" WHERE s."build_id" >= bw
  ON CONFLICT ("morpho_market_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "total_supply_assets"=EXCLUDED."total_supply_assets", "total_supply_shares"=EXCLUDED."total_supply_shares", "total_borrow_assets"=EXCLUDED."total_borrow_assets", "total_borrow_shares"=EXCLUDED."total_borrow_shares", "last_update_at"=EXCLUDED."last_update_at", "fee"=EXCLUDED."fee", "prev_borrow_rate"=EXCLUDED."prev_borrow_rate", "interest_accrued"=EXCLUDED."interest_accrued", "fee_shares"=EXCLUDED."fee_shares", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id";
  GET DIAGNOSTICS n=ROW_COUNT;
  UPDATE transformed._watermark SET build_wm=COALESCE((SELECT max("build_id") FROM transformed."morpho_market_state"),-1) WHERE source='morpho_market_state';
  RETURN n;
END $fn$ LANGUAGE plpgsql;

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
INSERT INTO transformed._watermark(source) VALUES ('morpho_market_position') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_morpho_market_position() RETURNS bigint AS $fn$
DECLARE bw int; n bigint;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='morpho_market_position';
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
  FROM public."morpho_market_position" s LEFT JOIN public."morpho_market" p ON p."id"=s."morpho_market_id" WHERE s."build_id" >= bw
  ON CONFLICT ("user_id", "morpho_market_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "supply_shares"=EXCLUDED."supply_shares", "borrow_shares"=EXCLUDED."borrow_shares", "collateral"=EXCLUDED."collateral", "supply_assets"=EXCLUDED."supply_assets", "borrow_assets"=EXCLUDED."borrow_assets", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id";
  GET DIAGNOSTICS n=ROW_COUNT;
  UPDATE transformed._watermark SET build_wm=COALESCE((SELECT max("build_id") FROM transformed."morpho_market_position"),-1) WHERE source='morpho_market_position';
  RETURN n;
END $fn$ LANGUAGE plpgsql;

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
INSERT INTO transformed._watermark(source) VALUES ('morpho_vault_state') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_morpho_vault_state() RETURNS bigint AS $fn$
DECLARE bw int; n bigint;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='morpho_vault_state';
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
  FROM public."morpho_vault_state" s LEFT JOIN public."morpho_vault" p ON p."id"=s."morpho_vault_id" WHERE s."build_id" >= bw
  ON CONFLICT ("morpho_vault_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "total_assets"=EXCLUDED."total_assets", "total_shares"=EXCLUDED."total_shares", "fee_shares"=EXCLUDED."fee_shares", "new_total_assets"=EXCLUDED."new_total_assets", "previous_total_assets"=EXCLUDED."previous_total_assets", "management_fee_shares"=EXCLUDED."management_fee_shares", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id";
  GET DIAGNOSTICS n=ROW_COUNT;
  UPDATE transformed._watermark SET build_wm=COALESCE((SELECT max("build_id") FROM transformed."morpho_vault_state"),-1) WHERE source='morpho_vault_state';
  RETURN n;
END $fn$ LANGUAGE plpgsql;

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
INSERT INTO transformed._watermark(source) VALUES ('morpho_vault_position') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_morpho_vault_position() RETURNS bigint AS $fn$
DECLARE bw int; n bigint;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='morpho_vault_position';
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
  FROM public."morpho_vault_position" s LEFT JOIN public."morpho_vault" p ON p."id"=s."morpho_vault_id" WHERE s."build_id" >= bw
  ON CONFLICT ("user_id", "morpho_vault_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "shares"=EXCLUDED."shares", "assets"=EXCLUDED."assets", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id";
  GET DIAGNOSTICS n=ROW_COUNT;
  UPDATE transformed._watermark SET build_wm=COALESCE((SELECT max("build_id") FROM transformed."morpho_vault_position"),-1) WHERE source='morpho_vault_position';
  RETURN n;
END $fn$ LANGUAGE plpgsql;

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
INSERT INTO transformed._watermark(source) VALUES ('fluid_vault_state') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_fluid_vault_state() RETURNS bigint AS $fn$
DECLARE bw int; n bigint;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='fluid_vault_state';
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
  FROM public."fluid_vault_state" s LEFT JOIN public."fluid_vault" p ON p."id"=s."fluid_vault_id" WHERE s."build_id" >= bw
  ON CONFLICT ("fluid_vault_id", "block_number", "block_version", "block_timestamp", "processing_version") DO UPDATE SET "total_collateral"=EXCLUDED."total_collateral", "total_debt"=EXCLUDED."total_debt", "supply_exchange_price"=EXCLUDED."supply_exchange_price", "borrow_exchange_price"=EXCLUDED."borrow_exchange_price", "supply_rate"=EXCLUDED."supply_rate", "borrow_rate"=EXCLUDED."borrow_rate", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id";
  GET DIAGNOSTICS n=ROW_COUNT;
  UPDATE transformed._watermark SET build_wm=COALESCE((SELECT max("build_id") FROM transformed."fluid_vault_state"),-1) WHERE source='fluid_vault_state';
  RETURN n;
END $fn$ LANGUAGE plpgsql;

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
INSERT INTO transformed._watermark(source) VALUES ('token_total_supply') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_token_total_supply() RETURNS bigint AS $fn$
DECLARE bw int; n bigint;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='token_total_supply';
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
  FROM public."token_total_supply" WHERE "build_id" >= bw
  ON CONFLICT ("chain_id", "token_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "total_supply"=EXCLUDED."total_supply", "scaled_total_supply"=EXCLUDED."scaled_total_supply", "source"=EXCLUDED."source", "build_id"=EXCLUDED."build_id", "created_at"=EXCLUDED."created_at";
  GET DIAGNOSTICS n=ROW_COUNT;
  UPDATE transformed._watermark SET build_wm=COALESCE((SELECT max("build_id") FROM transformed."token_total_supply"),-1) WHERE source='token_total_supply';
  RETURN n;
END $fn$ LANGUAGE plpgsql;

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
INSERT INTO transformed._watermark(source) VALUES ('onchain_token_price') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_onchain_token_price() RETURNS bigint AS $fn$
DECLARE bw int; n bigint;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='onchain_token_price';
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
  FROM public."onchain_token_price" s LEFT JOIN public."oracle" p ON p."id"=s."oracle_id" WHERE s."build_id" >= bw
  ON CONFLICT ("token_id", "oracle_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "price_usd"=EXCLUDED."price_usd", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id";
  GET DIAGNOSTICS n=ROW_COUNT;
  UPDATE transformed._watermark SET build_wm=COALESCE((SELECT max("build_id") FROM transformed."onchain_token_price"),-1) WHERE source='onchain_token_price';
  RETURN n;
END $fn$ LANGUAGE plpgsql;

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
INSERT INTO transformed._watermark(source) VALUES ('maple_loan_state') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_maple_loan_state() RETURNS bigint AS $fn$
DECLARE bw int; n bigint;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='maple_loan_state';
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
  FROM public."maple_loan_state" s LEFT JOIN public."maple_loan" p ON p."id"=s."maple_loan_id" WHERE s."build_id" >= bw
  ON CONFLICT ("maple_loan_id", "snapshot_time", "processing_version") DO UPDATE SET "state"=EXCLUDED."state", "principal_owed"=EXCLUDED."principal_owed", "acm_ratio"=EXCLUDED."acm_ratio", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id";
  GET DIAGNOSTICS n=ROW_COUNT;
  UPDATE transformed._watermark SET build_wm=COALESCE((SELECT max("build_id") FROM transformed."maple_loan_state"),-1) WHERE source='maple_loan_state';
  RETURN n;
END $fn$ LANGUAGE plpgsql;

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
INSERT INTO transformed._watermark(source) VALUES ('maple_loan_collateral') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_maple_loan_collateral() RETURNS bigint AS $fn$
DECLARE bw int; n bigint;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='maple_loan_collateral';
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
  FROM public."maple_loan_collateral" s LEFT JOIN public."maple_loan" p ON p."id"=s."maple_loan_id" WHERE s."build_id" >= bw
  ON CONFLICT ("maple_loan_id", "snapshot_time", "processing_version") DO UPDATE SET "asset_symbol"=EXCLUDED."asset_symbol", "asset_amount"=EXCLUDED."asset_amount", "asset_decimals"=EXCLUDED."asset_decimals", "asset_value_usd"=EXCLUDED."asset_value_usd", "state"=EXCLUDED."state", "custodian"=EXCLUDED."custodian", "liquidation_level"=EXCLUDED."liquidation_level", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id";
  GET DIAGNOSTICS n=ROW_COUNT;
  UPDATE transformed._watermark SET build_wm=COALESCE((SELECT max("build_id") FROM transformed."maple_loan_collateral"),-1) WHERE source='maple_loan_collateral';
  RETURN n;
END $fn$ LANGUAGE plpgsql;

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
INSERT INTO transformed._watermark(source) VALUES ('maple_pool_state') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_maple_pool_state() RETURNS bigint AS $fn$
DECLARE bw int; n bigint;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='maple_pool_state';
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
  FROM public."maple_pool_state" s LEFT JOIN public."maple_pool" p ON p."id"=s."maple_pool_id" WHERE s."build_id" >= bw
  ON CONFLICT ("maple_pool_id", "snapshot_time", "processing_version") DO UPDATE SET "tvl"=EXCLUDED."tvl", "liquid_assets"=EXCLUDED."liquid_assets", "collateral_value_usd"=EXCLUDED."collateral_value_usd", "principal_out"=EXCLUDED."principal_out", "utilization"=EXCLUDED."utilization", "monthly_apy"=EXCLUDED."monthly_apy", "spot_apy"=EXCLUDED."spot_apy", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id";
  GET DIAGNOSTICS n=ROW_COUNT;
  UPDATE transformed._watermark SET build_wm=COALESCE((SELECT max("build_id") FROM transformed."maple_pool_state"),-1) WHERE source='maple_pool_state';
  RETURN n;
END $fn$ LANGUAGE plpgsql;

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
INSERT INTO transformed._watermark(source) VALUES ('maple_sky_strategy_state') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_maple_sky_strategy_state() RETURNS bigint AS $fn$
DECLARE bw int; n bigint;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='maple_sky_strategy_state';
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
  FROM public."maple_sky_strategy_state" s LEFT JOIN public."maple_sky_strategy" p ON p."id"=s."maple_sky_strategy_id" WHERE s."build_id" >= bw
  ON CONFLICT ("maple_sky_strategy_id", "snapshot_time", "processing_version") DO UPDATE SET "state"=EXCLUDED."state", "currently_deployed"=EXCLUDED."currently_deployed", "deposited_assets"=EXCLUDED."deposited_assets", "withdrawn_assets"=EXCLUDED."withdrawn_assets", "strategy_fee_rate"=EXCLUDED."strategy_fee_rate", "total_fees_collected"=EXCLUDED."total_fees_collected", "build_id"=EXCLUDED."build_id", "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id";
  GET DIAGNOSTICS n=ROW_COUNT;
  UPDATE transformed._watermark SET build_wm=COALESCE((SELECT max("build_id") FROM transformed."maple_sky_strategy_state"),-1) WHERE source='maple_sky_strategy_state';
  RETURN n;
END $fn$ LANGUAGE plpgsql;

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
INSERT INTO transformed._watermark(source) VALUES ('maple_syrup_global_state') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_maple_syrup_global_state() RETURNS bigint AS $fn$
DECLARE bw int; n bigint;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='maple_syrup_global_state';
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
  FROM public."maple_syrup_global_state" WHERE "build_id" >= bw
  ON CONFLICT ("chain_id", "snapshot_time", "processing_version") DO UPDATE SET "tvl"=EXCLUDED."tvl", "apy"=EXCLUDED."apy", "collateral_apy"=EXCLUDED."collateral_apy", "pool_apy"=EXCLUDED."pool_apy", "drips_yield_boost"=EXCLUDED."drips_yield_boost", "build_id"=EXCLUDED."build_id";
  GET DIAGNOSTICS n=ROW_COUNT;
  UPDATE transformed._watermark SET build_wm=COALESCE((SELECT max("build_id") FROM transformed."maple_syrup_global_state"),-1) WHERE source='maple_syrup_global_state';
  RETURN n;
END $fn$ LANGUAGE plpgsql;

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
INSERT INTO transformed._watermark(source) VALUES ('offchain_token_price') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION transformed._run_offchain_token_price() RETURNS bigint AS $fn$
DECLARE bw int; n bigint;
BEGIN
  SELECT build_wm INTO bw FROM transformed._watermark WHERE source='offchain_token_price';
  INSERT INTO transformed."offchain_token_price"
  SELECT "token_id",
       CAST("source_id" AS bigint) AS "source_id",
       "timestamp" AS "snapshot_time",
       "price_usd",
       "market_cap_usd",
       "volume_usd",
       "processing_version",
       "build_id"
  FROM public."offchain_token_price" WHERE "build_id" >= bw
  ON CONFLICT ("token_id", "source_id", "processing_version", "snapshot_time") DO UPDATE SET "price_usd"=EXCLUDED."price_usd", "market_cap_usd"=EXCLUDED."market_cap_usd", "volume_usd"=EXCLUDED."volume_usd", "build_id"=EXCLUDED."build_id";
  GET DIAGNOSTICS n=ROW_COUNT;
  UPDATE transformed._watermark SET build_wm=COALESCE((SELECT max("build_id") FROM transformed."offchain_token_price"),-1) WHERE source='offchain_token_price';
  RETURN n;
END $fn$ LANGUAGE plpgsql;

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
