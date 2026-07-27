-- Transformation layer: morpho_adapter_state (VEC-218). One table added to the
-- bucket-1 layer created by 20260706_140000_create_transformed_bucket1.sql,
-- following the same recipe (hypertable + change queue + enqueue trigger +
-- _run_/_bootstrap_ functions + a transformed._sources row), emitted by the same
-- register-driven generator (cmd/util/gen-transformed) under the same regen-diff
-- gate. Migrations are immutable, so an added table lands as a NEW file rather
-- than an edit to bucket 1's; the schema, the _sources registry and the parity
-- machinery already exist and are not re-created here.
--
-- morpho_adapter_state is the first source whose dimension fills are TWO-HOP:
-- chain_id / protocol_id live on morpho_vault, one join further out than the
-- adapter, so each is a correlated subquery keyed on the joined morpho_adapter
-- row rather than a plain parent column.
--
-- Its sibling VaultV2 tables (morpho_vault_cap, morpho_vault_fee) stay deferred:
-- the transform layer's parity backstop (transformed._parity_refresh /
-- _parity_verify_all) resolves a source's time column from
-- timescaledb_information.dimensions and raises 'no time dimension' for a
-- non-hypertable, so a plain raw table cannot be registered in
-- transformed._sources without failing the worker on every tick.
--
-- Idempotent / non-destructive on the same terms as bucket 1: table, queue,
-- trigger and functions are all created IF NOT EXISTS / OR REPLACE and the PK
-- ALTER is guarded, so re-applying is a no-op.

-- Fail fast rather than convoy ingestion: a bounded lock_timeout makes the migration abort
-- and roll back if it cannot acquire a lock promptly, instead of blocking raw writers.
SET LOCAL lock_timeout = '10s';

-- ===== morpho_adapter_state =====

CREATE TABLE IF NOT EXISTS transformed."morpho_adapter_state" AS
SELECT (SELECT l."chain_id" FROM public."morpho_vault" l WHERE l."id"=p."morpho_vault_id") AS "chain_id",
       (SELECT l."protocol_id" FROM public."morpho_vault" l WHERE l."id"=p."morpho_vault_id") AS "protocol_id",
       s."morpho_adapter_id",
       s."block_number",
       s."block_version",
       s."timestamp" AS "block_timestamp",
       s."real_assets",
       s."processing_version",
       s."build_id"
FROM public."morpho_adapter_state" s LEFT JOIN public."morpho_adapter" p ON p."id"=s."morpho_adapter_id" WHERE false;
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='transformed."morpho_adapter_state"'::regclass AND contype='p') THEN ALTER TABLE transformed."morpho_adapter_state" ALTER COLUMN "block_timestamp" SET NOT NULL, ADD PRIMARY KEY ("morpho_adapter_id", "block_number", "block_version", "block_timestamp", "processing_version"); END IF; END $$;
SELECT create_hypertable('transformed."morpho_adapter_state"','block_timestamp',chunk_time_interval=>INTERVAL '1 day',if_not_exists=>TRUE);
ALTER TABLE transformed."morpho_adapter_state" SET (timescaledb.compress, timescaledb.compress_segmentby = 'morpho_adapter_id', timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC');
SELECT add_compression_policy('transformed."morpho_adapter_state"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."morpho_adapter_state"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.morpho_adapter_state'; END $$;
COMMENT ON TABLE transformed."morpho_adapter_state" IS
  '[Hypertable] Canonical transform of public.morpho_adapter_state (VEC-218): per-adapter realAssets() snapshot at a single block, one row per VaultV2 liquidity adapter per block. chain_id/protocol_id joined from morpho_vault via morpho_adapter; raw "timestamp" renamed to block_timestamp. Partition: block_timestamp.';
COMMENT ON COLUMN transformed."morpho_adapter_state".chain_id IS 'Derived. FK->chain.chain_id, joined from morpho_vault via morpho_adapter. Chain scope introduced by the transform.';
COMMENT ON COLUMN transformed."morpho_adapter_state".protocol_id IS 'Derived. FK->protocol.id, joined from morpho_vault via morpho_adapter. Protocol scope introduced by the transform.';
COMMENT ON COLUMN transformed."morpho_adapter_state".morpho_adapter_id IS 'FK->morpho_adapter.id. Part of PK.';
COMMENT ON COLUMN transformed."morpho_adapter_state".block_number IS 'Block height of the snapshot. Part of PK.';
COMMENT ON COLUMN transformed."morpho_adapter_state".block_version IS 'Reorg version: increments when a block is re-indexed after a reorg. Part of PK.';
COMMENT ON COLUMN transformed."morpho_adapter_state".block_timestamp IS 'Partition. On-chain block time, renamed from raw "timestamp". Part of PK.';
COMMENT ON COLUMN transformed."morpho_adapter_state".real_assets IS 'Adapter realAssets() reading: raw on-chain uint256 in the vault''s underlying asset base units (unscaled). Non-negative.';
COMMENT ON COLUMN transformed."morpho_adapter_state".processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by block_timestamp DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN transformed."morpho_adapter_state".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row (it identifies a git build, not a commit order). Not used by the transform refresh, which is queue-driven; carried through for lineage.';

CREATE TABLE IF NOT EXISTS transformed."_pending_morpho_adapter_state" AS SELECT "morpho_adapter_id", "block_number", "block_version", "timestamp", "processing_version" FROM public."morpho_adapter_state" WHERE false;
ALTER TABLE transformed."_pending_morpho_adapter_state" ADD COLUMN IF NOT EXISTS enqueued_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS "_pending_morpho_adapter_state_enqueued_at" ON transformed."_pending_morpho_adapter_state" (enqueued_at);
CREATE INDEX IF NOT EXISTS "_pending_morpho_adapter_state_timestamp" ON transformed."_pending_morpho_adapter_state" ("timestamp");
COMMENT ON TABLE transformed."_pending_morpho_adapter_state" IS '[Operational] Change queue for transformed.morpho_adapter_state. The AFTER INSERT trigger on public.morpho_adapter_state appends the raw PK here; transformed._run_morpho_adapter_state() drains it. A backlog with an old enqueued_at means the transform has stalled.';
CREATE OR REPLACE FUNCTION transformed."_enqueue_morpho_adapter_state"() RETURNS trigger
  LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, transformed, public AS $tg$
BEGIN
  INSERT INTO transformed."_pending_morpho_adapter_state"("morpho_adapter_id", "block_number", "block_version", "timestamp", "processing_version") VALUES (NEW."morpho_adapter_id", NEW."block_number", NEW."block_version", NEW."timestamp", NEW."processing_version");
  RETURN NEW;
END $tg$;
DROP TRIGGER IF EXISTS "_transform_enqueue" ON public."morpho_adapter_state";
CREATE TRIGGER "_transform_enqueue" AFTER INSERT ON public."morpho_adapter_state" FOR EACH ROW EXECUTE FUNCTION transformed."_enqueue_morpho_adapter_state"();
CREATE OR REPLACE FUNCTION transformed._run_morpho_adapter_state() RETURNS TABLE(consumed bigint, upserted bigint) AS $fn$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('transformed._run_morpho_adapter_state', 0));
  RETURN QUERY
  WITH batch AS (
    DELETE FROM transformed."_pending_morpho_adapter_state"
    WHERE ctid IN (SELECT ctid FROM transformed."_pending_morpho_adapter_state" ORDER BY "timestamp" LIMIT 10000)
    RETURNING "morpho_adapter_id", "block_number", "block_version", "timestamp", "processing_version"
  ), ins AS (
  INSERT INTO transformed."morpho_adapter_state" AS t ("chain_id", "protocol_id", "morpho_adapter_id", "block_number", "block_version", "block_timestamp", "real_assets", "processing_version", "build_id")
SELECT (SELECT l."chain_id" FROM public."morpho_vault" l WHERE l."id"=p."morpho_vault_id") AS "chain_id",
       (SELECT l."protocol_id" FROM public."morpho_vault" l WHERE l."id"=p."morpho_vault_id") AS "protocol_id",
       s."morpho_adapter_id",
       s."block_number",
       s."block_version",
       s."timestamp" AS "block_timestamp",
       s."real_assets",
       s."processing_version",
       s."build_id"
  FROM public."morpho_adapter_state" s LEFT JOIN public."morpho_adapter" p ON p."id"=s."morpho_adapter_id"
  WHERE (s."morpho_adapter_id", s."block_number", s."block_version", s."timestamp", s."processing_version") IN (SELECT DISTINCT "morpho_adapter_id", "block_number", "block_version", "timestamp", "processing_version" FROM batch)
    AND s."timestamp" >= (SELECT min("timestamp") FROM batch)
    AND s."timestamp" <= (SELECT max("timestamp") FROM batch)
  ON CONFLICT ("morpho_adapter_id", "block_number", "block_version", "block_timestamp", "processing_version") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "real_assets"=EXCLUDED."real_assets", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."real_assets", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."real_assets", EXCLUDED."build_id")
  RETURNING 1)
  SELECT (SELECT count(*) FROM batch), (SELECT count(*) FROM ins);
END $fn$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION transformed._bootstrap_morpho_adapter_state(_from timestamptz, _to timestamptz) RETURNS bigint AS $fn$
DECLARE n bigint;
BEGIN
  WITH ins AS (
  INSERT INTO transformed."morpho_adapter_state" AS t ("chain_id", "protocol_id", "morpho_adapter_id", "block_number", "block_version", "block_timestamp", "real_assets", "processing_version", "build_id")
SELECT (SELECT l."chain_id" FROM public."morpho_vault" l WHERE l."id"=p."morpho_vault_id") AS "chain_id",
       (SELECT l."protocol_id" FROM public."morpho_vault" l WHERE l."id"=p."morpho_vault_id") AS "protocol_id",
       s."morpho_adapter_id",
       s."block_number",
       s."block_version",
       s."timestamp" AS "block_timestamp",
       s."real_assets",
       s."processing_version",
       s."build_id"
  FROM public."morpho_adapter_state" s LEFT JOIN public."morpho_adapter" p ON p."id"=s."morpho_adapter_id"
  WHERE s."timestamp" >= _from AND s."timestamp" < _to
  ON CONFLICT ("morpho_adapter_id", "block_number", "block_version", "block_timestamp", "processing_version") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "real_assets"=EXCLUDED."real_assets", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."real_assets", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."real_assets", EXCLUDED."build_id")
  RETURNING 1)
  SELECT count(*) INTO n FROM ins;
  RETURN n;
END $fn$ LANGUAGE plpgsql;
INSERT INTO transformed._sources(source) VALUES ('morpho_adapter_state') ON CONFLICT DO NOTHING;

-- Re-created over the full cumulative source list so the new queue is visible to
-- the stalled-transform alert; generated, so an added table cannot be omitted.
CREATE OR REPLACE VIEW transformed._queue_status AS
SELECT 'morpho_market_state'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_morpho_market_state"
UNION ALL
SELECT 'morpho_market_position'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_morpho_market_position"
UNION ALL
SELECT 'morpho_vault_state'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_morpho_vault_state"
UNION ALL
SELECT 'morpho_vault_position'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_morpho_vault_position"
UNION ALL
SELECT 'fluid_vault_state'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_fluid_vault_state"
UNION ALL
SELECT 'token_total_supply'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_token_total_supply"
UNION ALL
SELECT 'onchain_token_price'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_onchain_token_price"
UNION ALL
SELECT 'maple_loan_state'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_maple_loan_state"
UNION ALL
SELECT 'maple_loan_collateral'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_maple_loan_collateral"
UNION ALL
SELECT 'maple_pool_state'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_maple_pool_state"
UNION ALL
SELECT 'maple_sky_strategy_state'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_maple_sky_strategy_state"
UNION ALL
SELECT 'maple_syrup_global_state'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_maple_syrup_global_state"
UNION ALL
SELECT 'offchain_token_price'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_offchain_token_price"
UNION ALL
SELECT 'morpho_adapter_state'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_morpho_adapter_state";

-- Grants: bucket 1's ALTER DEFAULT PRIVILEGES already covers objects this migration
-- creates, but only for the role that ran it. Re-granting explicitly makes the new
-- table's access independent of which role applied bucket 1.
GRANT SELECT ON ALL TABLES IN SCHEMA transformed TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA transformed TO stl_readwrite;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA transformed TO stl_readwrite;

INSERT INTO migrations (filename)
VALUES ('20260727_150000_create_transformed_morpho_adapter_state.sql')
ON CONFLICT (filename) DO NOTHING;
