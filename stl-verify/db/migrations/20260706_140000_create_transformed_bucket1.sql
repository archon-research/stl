-- Transformation layer, bucket 1 (VEC-484): 13 governed tables canonicalised 1:1 from raw
-- (rename / cast / dimension-fill), each a hypertable with a PK derived from the raw PK.
--
-- Refresh model: a trigger-maintained change queue (replaces the build_id watermark, which was
-- unsound: build_id identifies a git build, not a commit position, so it is reused on rollback
-- and differs across the multiple binaries that write one raw table; a build_id >= watermark
-- cursor silently drops rows from a concurrent lower-build writer, a rollback, or an older-image
-- backfill. Instead:
--   * Each raw table gets an AFTER INSERT row trigger that appends the new row's raw PK to a
--     per-table queue transformed._pending_<t>. The queue row commits in the same transaction as
--     the raw row, so overlap / rollback / backfill / multi-writer are all non-issues, and nothing
--     that inserts into raw can bypass it. Raw is append-only (corrections arrive as a new
--     processing_version row -> a new PK), so an INSERT trigger is sufficient.
--   * transformed._run_<t>() drains its queue in bounded batches (DELETE ... RETURNING under an
--     advisory lock), re-reads just those raw rows by PK (recent, uncompressed, untiered -- no
--     full scan, no S3 read), applies the transform, and upserts ON CONFLICT DO UPDATE ... WHERE
--     IS DISTINCT FROM. The upsert keeps the layer re-runnable -- a re-enqueue or a re-bootstrap
--     after a transform-logic fix or a late-arriving dimension refreshes the row -- while the
--     IS DISTINCT FROM guard skips the write when nothing changed, so there is no churn. It
--     returns the queue rows consumed; the worker loops until the queue is drained.
--   * The trigger functions are SECURITY DEFINER (owned by the migration role) so enqueue never
--     depends on the raw writer holding a grant on the queue -- a grant gap can never halt ingest.
--   * Bootstrap of pre-existing history is out-of-band (transformed._bootstrap_<t>(_from,_to) run
--     in time windows by a one-off job with tiered reads enabled), NOT through the 10-minute
--     Temporal activity. The trigger is live before bootstrap starts, so the overlap window is
--     closed by the upsert's idempotency (re-inserting an identical row is a guarded no-op).
--
-- Idempotent / non-destructive: schema, tables, queues, triggers and functions are all created
-- IF NOT EXISTS / OR REPLACE and PK ALTERs are guarded, so re-applying is a no-op.
--
-- COMMENT ON metadata follows the catalogue convention (see 20260609_120000_add_schema_comments.sql).
-- Comment text is carried verbatim from each raw source table; a raw column that was itself
-- undocumented is flagged "verify before computing" rather than given a fabricated unit/scale.

CREATE SCHEMA IF NOT EXISTS transformed;

-- Registry of transformed tables the worker iterates (replaces the _watermark row set).
CREATE TABLE IF NOT EXISTS transformed._sources(source text PRIMARY KEY);
COMMENT ON TABLE transformed._sources IS
  '[Operational] One row per transformed table. The transform worker lists these and calls transformed._run_<source>() for each.';
COMMENT ON COLUMN transformed._sources.source IS 'PK. Transformed table name whose _run_<source>()/_bootstrap_<source>() functions and _pending_<source> queue exist.';


-- ===== morpho_market_state =====
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
SELECT create_hypertable('transformed."morpho_market_state"','block_timestamp',chunk_time_interval=>INTERVAL '1 day',if_not_exists=>TRUE);
ALTER TABLE transformed."morpho_market_state" SET (timescaledb.compress, timescaledb.compress_segmentby = 'morpho_market_id', timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC');
SELECT add_compression_policy('transformed."morpho_market_state"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."morpho_market_state"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.morpho_market_state'; END $$;
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
COMMENT ON COLUMN transformed."morpho_market_state".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row (it identifies a git build, not a commit order). Not used by the transform refresh, which is queue-driven; carried through for lineage.';

CREATE TABLE IF NOT EXISTS transformed."_pending_morpho_market_state" AS SELECT "morpho_market_id", "block_number", "block_version", "processing_version", "timestamp" FROM public."morpho_market_state" WHERE false;
ALTER TABLE transformed."_pending_morpho_market_state" ADD COLUMN IF NOT EXISTS enqueued_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS "_pending_morpho_market_state_enqueued_at" ON transformed."_pending_morpho_market_state" (enqueued_at);
COMMENT ON TABLE transformed."_pending_morpho_market_state" IS '[Operational] Change queue for transformed.morpho_market_state. The AFTER INSERT trigger on public.morpho_market_state appends the raw PK here; transformed._run_morpho_market_state() drains it. A backlog with an old enqueued_at means the transform has stalled.';
CREATE OR REPLACE FUNCTION transformed."_enqueue_morpho_market_state"() RETURNS trigger
  LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, transformed, public AS $tg$
BEGIN
  INSERT INTO transformed."_pending_morpho_market_state"("morpho_market_id", "block_number", "block_version", "processing_version", "timestamp") VALUES (NEW."morpho_market_id", NEW."block_number", NEW."block_version", NEW."processing_version", NEW."timestamp");
  RETURN NEW;
END $tg$;
DROP TRIGGER IF EXISTS "_transform_enqueue" ON public."morpho_market_state";
CREATE TRIGGER "_transform_enqueue" AFTER INSERT ON public."morpho_market_state" FOR EACH ROW EXECUTE FUNCTION transformed."_enqueue_morpho_market_state"();
CREATE OR REPLACE FUNCTION transformed._run_morpho_market_state() RETURNS TABLE(consumed bigint, upserted bigint) AS $fn$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('transformed._run_morpho_market_state', 0));
  RETURN QUERY
  WITH batch AS (
    DELETE FROM transformed."_pending_morpho_market_state"
    WHERE ctid IN (SELECT ctid FROM transformed."_pending_morpho_market_state" LIMIT 10000)
    RETURNING "morpho_market_id", "block_number", "block_version", "processing_version", "timestamp"
  ), ins AS (
  INSERT INTO transformed."morpho_market_state" AS t ("chain_id", "protocol_id", "morpho_market_id", "block_number", "block_version", "block_timestamp", "total_supply_assets", "total_supply_shares", "total_borrow_assets", "total_borrow_shares", "last_update_at", "fee", "prev_borrow_rate", "interest_accrued", "fee_shares", "processing_version", "build_id")
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
  FROM public."morpho_market_state" s LEFT JOIN public."morpho_market" p ON p."id"=s."morpho_market_id"
  WHERE (s."morpho_market_id", s."block_number", s."block_version", s."processing_version", s."timestamp") IN (SELECT DISTINCT "morpho_market_id", "block_number", "block_version", "processing_version", "timestamp" FROM batch)
    AND s."timestamp" >= (SELECT min("timestamp") FROM batch)
    AND s."timestamp" <= (SELECT max("timestamp") FROM batch)
  ON CONFLICT ("morpho_market_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "total_supply_assets"=EXCLUDED."total_supply_assets", "total_supply_shares"=EXCLUDED."total_supply_shares", "total_borrow_assets"=EXCLUDED."total_borrow_assets", "total_borrow_shares"=EXCLUDED."total_borrow_shares", "last_update_at"=EXCLUDED."last_update_at", "fee"=EXCLUDED."fee", "prev_borrow_rate"=EXCLUDED."prev_borrow_rate", "interest_accrued"=EXCLUDED."interest_accrued", "fee_shares"=EXCLUDED."fee_shares", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."total_supply_assets", t."total_supply_shares", t."total_borrow_assets", t."total_borrow_shares", t."last_update_at", t."fee", t."prev_borrow_rate", t."interest_accrued", t."fee_shares", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."total_supply_assets", EXCLUDED."total_supply_shares", EXCLUDED."total_borrow_assets", EXCLUDED."total_borrow_shares", EXCLUDED."last_update_at", EXCLUDED."fee", EXCLUDED."prev_borrow_rate", EXCLUDED."interest_accrued", EXCLUDED."fee_shares", EXCLUDED."build_id")
  RETURNING 1)
  SELECT (SELECT count(*) FROM batch), (SELECT count(*) FROM ins);
END $fn$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION transformed._bootstrap_morpho_market_state(_from timestamptz, _to timestamptz) RETURNS bigint AS $fn$
DECLARE n bigint;
BEGIN
  WITH ins AS (
  INSERT INTO transformed."morpho_market_state" AS t ("chain_id", "protocol_id", "morpho_market_id", "block_number", "block_version", "block_timestamp", "total_supply_assets", "total_supply_shares", "total_borrow_assets", "total_borrow_shares", "last_update_at", "fee", "prev_borrow_rate", "interest_accrued", "fee_shares", "processing_version", "build_id")
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
  FROM public."morpho_market_state" s LEFT JOIN public."morpho_market" p ON p."id"=s."morpho_market_id"
  WHERE s."timestamp" >= _from AND s."timestamp" < _to
  ON CONFLICT ("morpho_market_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "total_supply_assets"=EXCLUDED."total_supply_assets", "total_supply_shares"=EXCLUDED."total_supply_shares", "total_borrow_assets"=EXCLUDED."total_borrow_assets", "total_borrow_shares"=EXCLUDED."total_borrow_shares", "last_update_at"=EXCLUDED."last_update_at", "fee"=EXCLUDED."fee", "prev_borrow_rate"=EXCLUDED."prev_borrow_rate", "interest_accrued"=EXCLUDED."interest_accrued", "fee_shares"=EXCLUDED."fee_shares", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."total_supply_assets", t."total_supply_shares", t."total_borrow_assets", t."total_borrow_shares", t."last_update_at", t."fee", t."prev_borrow_rate", t."interest_accrued", t."fee_shares", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."total_supply_assets", EXCLUDED."total_supply_shares", EXCLUDED."total_borrow_assets", EXCLUDED."total_borrow_shares", EXCLUDED."last_update_at", EXCLUDED."fee", EXCLUDED."prev_borrow_rate", EXCLUDED."interest_accrued", EXCLUDED."fee_shares", EXCLUDED."build_id")
  RETURNING 1)
  SELECT count(*) INTO n FROM ins;
  RETURN n;
END $fn$ LANGUAGE plpgsql;
INSERT INTO transformed._sources(source) VALUES ('morpho_market_state') ON CONFLICT DO NOTHING;

-- ===== morpho_market_position =====
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
SELECT create_hypertable('transformed."morpho_market_position"','block_timestamp',chunk_time_interval=>INTERVAL '1 day',if_not_exists=>TRUE);
ALTER TABLE transformed."morpho_market_position" SET (timescaledb.compress, timescaledb.compress_segmentby = 'morpho_market_id, user_id', timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC');
SELECT add_compression_policy('transformed."morpho_market_position"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."morpho_market_position"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.morpho_market_position'; END $$;
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
COMMENT ON COLUMN transformed."morpho_market_position".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row (it identifies a git build, not a commit order). Not used by the transform refresh, which is queue-driven; carried through for lineage.';

CREATE TABLE IF NOT EXISTS transformed."_pending_morpho_market_position" AS SELECT "user_id", "morpho_market_id", "block_number", "block_version", "processing_version", "timestamp" FROM public."morpho_market_position" WHERE false;
ALTER TABLE transformed."_pending_morpho_market_position" ADD COLUMN IF NOT EXISTS enqueued_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS "_pending_morpho_market_position_enqueued_at" ON transformed."_pending_morpho_market_position" (enqueued_at);
COMMENT ON TABLE transformed."_pending_morpho_market_position" IS '[Operational] Change queue for transformed.morpho_market_position. The AFTER INSERT trigger on public.morpho_market_position appends the raw PK here; transformed._run_morpho_market_position() drains it. A backlog with an old enqueued_at means the transform has stalled.';
CREATE OR REPLACE FUNCTION transformed."_enqueue_morpho_market_position"() RETURNS trigger
  LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, transformed, public AS $tg$
BEGIN
  INSERT INTO transformed."_pending_morpho_market_position"("user_id", "morpho_market_id", "block_number", "block_version", "processing_version", "timestamp") VALUES (NEW."user_id", NEW."morpho_market_id", NEW."block_number", NEW."block_version", NEW."processing_version", NEW."timestamp");
  RETURN NEW;
END $tg$;
DROP TRIGGER IF EXISTS "_transform_enqueue" ON public."morpho_market_position";
CREATE TRIGGER "_transform_enqueue" AFTER INSERT ON public."morpho_market_position" FOR EACH ROW EXECUTE FUNCTION transformed."_enqueue_morpho_market_position"();
CREATE OR REPLACE FUNCTION transformed._run_morpho_market_position() RETURNS TABLE(consumed bigint, upserted bigint) AS $fn$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('transformed._run_morpho_market_position', 0));
  RETURN QUERY
  WITH batch AS (
    DELETE FROM transformed."_pending_morpho_market_position"
    WHERE ctid IN (SELECT ctid FROM transformed."_pending_morpho_market_position" LIMIT 10000)
    RETURNING "user_id", "morpho_market_id", "block_number", "block_version", "processing_version", "timestamp"
  ), ins AS (
  INSERT INTO transformed."morpho_market_position" AS t ("chain_id", "protocol_id", "user_id", "morpho_market_id", "block_number", "block_version", "block_timestamp", "supply_shares", "borrow_shares", "collateral", "supply_assets", "borrow_assets", "processing_version", "build_id")
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
  FROM public."morpho_market_position" s LEFT JOIN public."morpho_market" p ON p."id"=s."morpho_market_id"
  WHERE (s."user_id", s."morpho_market_id", s."block_number", s."block_version", s."processing_version", s."timestamp") IN (SELECT DISTINCT "user_id", "morpho_market_id", "block_number", "block_version", "processing_version", "timestamp" FROM batch)
    AND s."timestamp" >= (SELECT min("timestamp") FROM batch)
    AND s."timestamp" <= (SELECT max("timestamp") FROM batch)
  ON CONFLICT ("user_id", "morpho_market_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "supply_shares"=EXCLUDED."supply_shares", "borrow_shares"=EXCLUDED."borrow_shares", "collateral"=EXCLUDED."collateral", "supply_assets"=EXCLUDED."supply_assets", "borrow_assets"=EXCLUDED."borrow_assets", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."supply_shares", t."borrow_shares", t."collateral", t."supply_assets", t."borrow_assets", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."supply_shares", EXCLUDED."borrow_shares", EXCLUDED."collateral", EXCLUDED."supply_assets", EXCLUDED."borrow_assets", EXCLUDED."build_id")
  RETURNING 1)
  SELECT (SELECT count(*) FROM batch), (SELECT count(*) FROM ins);
END $fn$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION transformed._bootstrap_morpho_market_position(_from timestamptz, _to timestamptz) RETURNS bigint AS $fn$
DECLARE n bigint;
BEGIN
  WITH ins AS (
  INSERT INTO transformed."morpho_market_position" AS t ("chain_id", "protocol_id", "user_id", "morpho_market_id", "block_number", "block_version", "block_timestamp", "supply_shares", "borrow_shares", "collateral", "supply_assets", "borrow_assets", "processing_version", "build_id")
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
  FROM public."morpho_market_position" s LEFT JOIN public."morpho_market" p ON p."id"=s."morpho_market_id"
  WHERE s."timestamp" >= _from AND s."timestamp" < _to
  ON CONFLICT ("user_id", "morpho_market_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "supply_shares"=EXCLUDED."supply_shares", "borrow_shares"=EXCLUDED."borrow_shares", "collateral"=EXCLUDED."collateral", "supply_assets"=EXCLUDED."supply_assets", "borrow_assets"=EXCLUDED."borrow_assets", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."supply_shares", t."borrow_shares", t."collateral", t."supply_assets", t."borrow_assets", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."supply_shares", EXCLUDED."borrow_shares", EXCLUDED."collateral", EXCLUDED."supply_assets", EXCLUDED."borrow_assets", EXCLUDED."build_id")
  RETURNING 1)
  SELECT count(*) INTO n FROM ins;
  RETURN n;
END $fn$ LANGUAGE plpgsql;
INSERT INTO transformed._sources(source) VALUES ('morpho_market_position') ON CONFLICT DO NOTHING;

-- ===== morpho_vault_state =====
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
SELECT create_hypertable('transformed."morpho_vault_state"','block_timestamp',chunk_time_interval=>INTERVAL '1 day',if_not_exists=>TRUE);
ALTER TABLE transformed."morpho_vault_state" SET (timescaledb.compress, timescaledb.compress_segmentby = 'morpho_vault_id', timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC');
SELECT add_compression_policy('transformed."morpho_vault_state"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."morpho_vault_state"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.morpho_vault_state'; END $$;
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
COMMENT ON COLUMN transformed."morpho_vault_state".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row (it identifies a git build, not a commit order). Not used by the transform refresh, which is queue-driven; carried through for lineage.';

CREATE TABLE IF NOT EXISTS transformed."_pending_morpho_vault_state" AS SELECT "morpho_vault_id", "block_number", "block_version", "processing_version", "timestamp" FROM public."morpho_vault_state" WHERE false;
ALTER TABLE transformed."_pending_morpho_vault_state" ADD COLUMN IF NOT EXISTS enqueued_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS "_pending_morpho_vault_state_enqueued_at" ON transformed."_pending_morpho_vault_state" (enqueued_at);
COMMENT ON TABLE transformed."_pending_morpho_vault_state" IS '[Operational] Change queue for transformed.morpho_vault_state. The AFTER INSERT trigger on public.morpho_vault_state appends the raw PK here; transformed._run_morpho_vault_state() drains it. A backlog with an old enqueued_at means the transform has stalled.';
CREATE OR REPLACE FUNCTION transformed."_enqueue_morpho_vault_state"() RETURNS trigger
  LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, transformed, public AS $tg$
BEGIN
  INSERT INTO transformed."_pending_morpho_vault_state"("morpho_vault_id", "block_number", "block_version", "processing_version", "timestamp") VALUES (NEW."morpho_vault_id", NEW."block_number", NEW."block_version", NEW."processing_version", NEW."timestamp");
  RETURN NEW;
END $tg$;
DROP TRIGGER IF EXISTS "_transform_enqueue" ON public."morpho_vault_state";
CREATE TRIGGER "_transform_enqueue" AFTER INSERT ON public."morpho_vault_state" FOR EACH ROW EXECUTE FUNCTION transformed."_enqueue_morpho_vault_state"();
CREATE OR REPLACE FUNCTION transformed._run_morpho_vault_state() RETURNS TABLE(consumed bigint, upserted bigint) AS $fn$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('transformed._run_morpho_vault_state', 0));
  RETURN QUERY
  WITH batch AS (
    DELETE FROM transformed."_pending_morpho_vault_state"
    WHERE ctid IN (SELECT ctid FROM transformed."_pending_morpho_vault_state" LIMIT 10000)
    RETURNING "morpho_vault_id", "block_number", "block_version", "processing_version", "timestamp"
  ), ins AS (
  INSERT INTO transformed."morpho_vault_state" AS t ("chain_id", "protocol_id", "morpho_vault_id", "block_number", "block_version", "block_timestamp", "total_assets", "total_shares", "fee_shares", "new_total_assets", "previous_total_assets", "management_fee_shares", "processing_version", "build_id")
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
  FROM public."morpho_vault_state" s LEFT JOIN public."morpho_vault" p ON p."id"=s."morpho_vault_id"
  WHERE (s."morpho_vault_id", s."block_number", s."block_version", s."processing_version", s."timestamp") IN (SELECT DISTINCT "morpho_vault_id", "block_number", "block_version", "processing_version", "timestamp" FROM batch)
    AND s."timestamp" >= (SELECT min("timestamp") FROM batch)
    AND s."timestamp" <= (SELECT max("timestamp") FROM batch)
  ON CONFLICT ("morpho_vault_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "total_assets"=EXCLUDED."total_assets", "total_shares"=EXCLUDED."total_shares", "fee_shares"=EXCLUDED."fee_shares", "new_total_assets"=EXCLUDED."new_total_assets", "previous_total_assets"=EXCLUDED."previous_total_assets", "management_fee_shares"=EXCLUDED."management_fee_shares", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."total_assets", t."total_shares", t."fee_shares", t."new_total_assets", t."previous_total_assets", t."management_fee_shares", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."total_assets", EXCLUDED."total_shares", EXCLUDED."fee_shares", EXCLUDED."new_total_assets", EXCLUDED."previous_total_assets", EXCLUDED."management_fee_shares", EXCLUDED."build_id")
  RETURNING 1)
  SELECT (SELECT count(*) FROM batch), (SELECT count(*) FROM ins);
END $fn$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION transformed._bootstrap_morpho_vault_state(_from timestamptz, _to timestamptz) RETURNS bigint AS $fn$
DECLARE n bigint;
BEGIN
  WITH ins AS (
  INSERT INTO transformed."morpho_vault_state" AS t ("chain_id", "protocol_id", "morpho_vault_id", "block_number", "block_version", "block_timestamp", "total_assets", "total_shares", "fee_shares", "new_total_assets", "previous_total_assets", "management_fee_shares", "processing_version", "build_id")
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
  FROM public."morpho_vault_state" s LEFT JOIN public."morpho_vault" p ON p."id"=s."morpho_vault_id"
  WHERE s."timestamp" >= _from AND s."timestamp" < _to
  ON CONFLICT ("morpho_vault_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "total_assets"=EXCLUDED."total_assets", "total_shares"=EXCLUDED."total_shares", "fee_shares"=EXCLUDED."fee_shares", "new_total_assets"=EXCLUDED."new_total_assets", "previous_total_assets"=EXCLUDED."previous_total_assets", "management_fee_shares"=EXCLUDED."management_fee_shares", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."total_assets", t."total_shares", t."fee_shares", t."new_total_assets", t."previous_total_assets", t."management_fee_shares", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."total_assets", EXCLUDED."total_shares", EXCLUDED."fee_shares", EXCLUDED."new_total_assets", EXCLUDED."previous_total_assets", EXCLUDED."management_fee_shares", EXCLUDED."build_id")
  RETURNING 1)
  SELECT count(*) INTO n FROM ins;
  RETURN n;
END $fn$ LANGUAGE plpgsql;
INSERT INTO transformed._sources(source) VALUES ('morpho_vault_state') ON CONFLICT DO NOTHING;

-- ===== morpho_vault_position =====
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
SELECT create_hypertable('transformed."morpho_vault_position"','block_timestamp',chunk_time_interval=>INTERVAL '1 day',if_not_exists=>TRUE);
ALTER TABLE transformed."morpho_vault_position" SET (timescaledb.compress, timescaledb.compress_segmentby = 'morpho_vault_id, user_id', timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC');
SELECT add_compression_policy('transformed."morpho_vault_position"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."morpho_vault_position"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.morpho_vault_position'; END $$;
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
COMMENT ON COLUMN transformed."morpho_vault_position".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row (it identifies a git build, not a commit order). Not used by the transform refresh, which is queue-driven; carried through for lineage.';

CREATE TABLE IF NOT EXISTS transformed."_pending_morpho_vault_position" AS SELECT "user_id", "morpho_vault_id", "block_number", "block_version", "processing_version", "timestamp" FROM public."morpho_vault_position" WHERE false;
ALTER TABLE transformed."_pending_morpho_vault_position" ADD COLUMN IF NOT EXISTS enqueued_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS "_pending_morpho_vault_position_enqueued_at" ON transformed."_pending_morpho_vault_position" (enqueued_at);
COMMENT ON TABLE transformed."_pending_morpho_vault_position" IS '[Operational] Change queue for transformed.morpho_vault_position. The AFTER INSERT trigger on public.morpho_vault_position appends the raw PK here; transformed._run_morpho_vault_position() drains it. A backlog with an old enqueued_at means the transform has stalled.';
CREATE OR REPLACE FUNCTION transformed."_enqueue_morpho_vault_position"() RETURNS trigger
  LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, transformed, public AS $tg$
BEGIN
  INSERT INTO transformed."_pending_morpho_vault_position"("user_id", "morpho_vault_id", "block_number", "block_version", "processing_version", "timestamp") VALUES (NEW."user_id", NEW."morpho_vault_id", NEW."block_number", NEW."block_version", NEW."processing_version", NEW."timestamp");
  RETURN NEW;
END $tg$;
DROP TRIGGER IF EXISTS "_transform_enqueue" ON public."morpho_vault_position";
CREATE TRIGGER "_transform_enqueue" AFTER INSERT ON public."morpho_vault_position" FOR EACH ROW EXECUTE FUNCTION transformed."_enqueue_morpho_vault_position"();
CREATE OR REPLACE FUNCTION transformed._run_morpho_vault_position() RETURNS TABLE(consumed bigint, upserted bigint) AS $fn$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('transformed._run_morpho_vault_position', 0));
  RETURN QUERY
  WITH batch AS (
    DELETE FROM transformed."_pending_morpho_vault_position"
    WHERE ctid IN (SELECT ctid FROM transformed."_pending_morpho_vault_position" LIMIT 10000)
    RETURNING "user_id", "morpho_vault_id", "block_number", "block_version", "processing_version", "timestamp"
  ), ins AS (
  INSERT INTO transformed."morpho_vault_position" AS t ("chain_id", "protocol_id", "user_id", "morpho_vault_id", "block_number", "block_version", "block_timestamp", "shares", "assets", "processing_version", "build_id")
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
  FROM public."morpho_vault_position" s LEFT JOIN public."morpho_vault" p ON p."id"=s."morpho_vault_id"
  WHERE (s."user_id", s."morpho_vault_id", s."block_number", s."block_version", s."processing_version", s."timestamp") IN (SELECT DISTINCT "user_id", "morpho_vault_id", "block_number", "block_version", "processing_version", "timestamp" FROM batch)
    AND s."timestamp" >= (SELECT min("timestamp") FROM batch)
    AND s."timestamp" <= (SELECT max("timestamp") FROM batch)
  ON CONFLICT ("user_id", "morpho_vault_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "shares"=EXCLUDED."shares", "assets"=EXCLUDED."assets", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."shares", t."assets", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."shares", EXCLUDED."assets", EXCLUDED."build_id")
  RETURNING 1)
  SELECT (SELECT count(*) FROM batch), (SELECT count(*) FROM ins);
END $fn$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION transformed._bootstrap_morpho_vault_position(_from timestamptz, _to timestamptz) RETURNS bigint AS $fn$
DECLARE n bigint;
BEGIN
  WITH ins AS (
  INSERT INTO transformed."morpho_vault_position" AS t ("chain_id", "protocol_id", "user_id", "morpho_vault_id", "block_number", "block_version", "block_timestamp", "shares", "assets", "processing_version", "build_id")
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
  FROM public."morpho_vault_position" s LEFT JOIN public."morpho_vault" p ON p."id"=s."morpho_vault_id"
  WHERE s."timestamp" >= _from AND s."timestamp" < _to
  ON CONFLICT ("user_id", "morpho_vault_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "shares"=EXCLUDED."shares", "assets"=EXCLUDED."assets", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."shares", t."assets", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."shares", EXCLUDED."assets", EXCLUDED."build_id")
  RETURNING 1)
  SELECT count(*) INTO n FROM ins;
  RETURN n;
END $fn$ LANGUAGE plpgsql;
INSERT INTO transformed._sources(source) VALUES ('morpho_vault_position') ON CONFLICT DO NOTHING;

-- ===== fluid_vault_state =====
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
SELECT create_hypertable('transformed."fluid_vault_state"','block_timestamp',chunk_time_interval=>INTERVAL '7 days',if_not_exists=>TRUE);
ALTER TABLE transformed."fluid_vault_state" SET (timescaledb.compress, timescaledb.compress_segmentby = 'fluid_vault_id', timescaledb.compress_orderby = 'block_timestamp DESC, processing_version DESC');
SELECT add_compression_policy('transformed."fluid_vault_state"', INTERVAL '14 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."fluid_vault_state"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.fluid_vault_state'; END $$;
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
COMMENT ON COLUMN transformed."fluid_vault_state".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row (it identifies a git build, not a commit order). Not used by the transform refresh, which is queue-driven; carried through for lineage.';

CREATE TABLE IF NOT EXISTS transformed."_pending_fluid_vault_state" AS SELECT "fluid_vault_id", "block_number", "block_version", "block_timestamp", "processing_version" FROM public."fluid_vault_state" WHERE false;
ALTER TABLE transformed."_pending_fluid_vault_state" ADD COLUMN IF NOT EXISTS enqueued_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS "_pending_fluid_vault_state_enqueued_at" ON transformed."_pending_fluid_vault_state" (enqueued_at);
COMMENT ON TABLE transformed."_pending_fluid_vault_state" IS '[Operational] Change queue for transformed.fluid_vault_state. The AFTER INSERT trigger on public.fluid_vault_state appends the raw PK here; transformed._run_fluid_vault_state() drains it. A backlog with an old enqueued_at means the transform has stalled.';
CREATE OR REPLACE FUNCTION transformed."_enqueue_fluid_vault_state"() RETURNS trigger
  LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, transformed, public AS $tg$
BEGIN
  INSERT INTO transformed."_pending_fluid_vault_state"("fluid_vault_id", "block_number", "block_version", "block_timestamp", "processing_version") VALUES (NEW."fluid_vault_id", NEW."block_number", NEW."block_version", NEW."block_timestamp", NEW."processing_version");
  RETURN NEW;
END $tg$;
DROP TRIGGER IF EXISTS "_transform_enqueue" ON public."fluid_vault_state";
CREATE TRIGGER "_transform_enqueue" AFTER INSERT ON public."fluid_vault_state" FOR EACH ROW EXECUTE FUNCTION transformed."_enqueue_fluid_vault_state"();
CREATE OR REPLACE FUNCTION transformed._run_fluid_vault_state() RETURNS TABLE(consumed bigint, upserted bigint) AS $fn$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('transformed._run_fluid_vault_state', 0));
  RETURN QUERY
  WITH batch AS (
    DELETE FROM transformed."_pending_fluid_vault_state"
    WHERE ctid IN (SELECT ctid FROM transformed."_pending_fluid_vault_state" LIMIT 10000)
    RETURNING "fluid_vault_id", "block_number", "block_version", "block_timestamp", "processing_version"
  ), ins AS (
  INSERT INTO transformed."fluid_vault_state" AS t ("chain_id", "protocol_id", "fluid_vault_id", "block_number", "block_version", "block_timestamp", "total_collateral", "total_debt", "supply_exchange_price", "borrow_exchange_price", "supply_rate", "borrow_rate", "processing_version", "build_id")
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
  FROM public."fluid_vault_state" s LEFT JOIN public."fluid_vault" p ON p."id"=s."fluid_vault_id"
  WHERE (s."fluid_vault_id", s."block_number", s."block_version", s."block_timestamp", s."processing_version") IN (SELECT DISTINCT "fluid_vault_id", "block_number", "block_version", "block_timestamp", "processing_version" FROM batch)
    AND s."block_timestamp" >= (SELECT min("block_timestamp") FROM batch)
    AND s."block_timestamp" <= (SELECT max("block_timestamp") FROM batch)
  ON CONFLICT ("fluid_vault_id", "block_number", "block_version", "block_timestamp", "processing_version") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "total_collateral"=EXCLUDED."total_collateral", "total_debt"=EXCLUDED."total_debt", "supply_exchange_price"=EXCLUDED."supply_exchange_price", "borrow_exchange_price"=EXCLUDED."borrow_exchange_price", "supply_rate"=EXCLUDED."supply_rate", "borrow_rate"=EXCLUDED."borrow_rate", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."total_collateral", t."total_debt", t."supply_exchange_price", t."borrow_exchange_price", t."supply_rate", t."borrow_rate", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."total_collateral", EXCLUDED."total_debt", EXCLUDED."supply_exchange_price", EXCLUDED."borrow_exchange_price", EXCLUDED."supply_rate", EXCLUDED."borrow_rate", EXCLUDED."build_id")
  RETURNING 1)
  SELECT (SELECT count(*) FROM batch), (SELECT count(*) FROM ins);
END $fn$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION transformed._bootstrap_fluid_vault_state(_from timestamptz, _to timestamptz) RETURNS bigint AS $fn$
DECLARE n bigint;
BEGIN
  WITH ins AS (
  INSERT INTO transformed."fluid_vault_state" AS t ("chain_id", "protocol_id", "fluid_vault_id", "block_number", "block_version", "block_timestamp", "total_collateral", "total_debt", "supply_exchange_price", "borrow_exchange_price", "supply_rate", "borrow_rate", "processing_version", "build_id")
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
  FROM public."fluid_vault_state" s LEFT JOIN public."fluid_vault" p ON p."id"=s."fluid_vault_id"
  WHERE s."block_timestamp" >= _from AND s."block_timestamp" < _to
  ON CONFLICT ("fluid_vault_id", "block_number", "block_version", "block_timestamp", "processing_version") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "total_collateral"=EXCLUDED."total_collateral", "total_debt"=EXCLUDED."total_debt", "supply_exchange_price"=EXCLUDED."supply_exchange_price", "borrow_exchange_price"=EXCLUDED."borrow_exchange_price", "supply_rate"=EXCLUDED."supply_rate", "borrow_rate"=EXCLUDED."borrow_rate", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."total_collateral", t."total_debt", t."supply_exchange_price", t."borrow_exchange_price", t."supply_rate", t."borrow_rate", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."total_collateral", EXCLUDED."total_debt", EXCLUDED."supply_exchange_price", EXCLUDED."borrow_exchange_price", EXCLUDED."supply_rate", EXCLUDED."borrow_rate", EXCLUDED."build_id")
  RETURNING 1)
  SELECT count(*) INTO n FROM ins;
  RETURN n;
END $fn$ LANGUAGE plpgsql;
INSERT INTO transformed._sources(source) VALUES ('fluid_vault_state') ON CONFLICT DO NOTHING;

-- ===== token_total_supply =====
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
SELECT create_hypertable('transformed."token_total_supply"','block_timestamp',chunk_time_interval=>INTERVAL '7 days',if_not_exists=>TRUE);
ALTER TABLE transformed."token_total_supply" SET (timescaledb.compress, timescaledb.compress_segmentby = 'chain_id, token_id', timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC');
SELECT add_compression_policy('transformed."token_total_supply"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."token_total_supply"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.token_total_supply'; END $$;
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
COMMENT ON COLUMN transformed."token_total_supply".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row (it identifies a git build, not a commit order). Not used by the transform refresh, which is queue-driven; carried through for lineage.';
COMMENT ON COLUMN transformed."token_total_supply".created_at IS 'Audit. Wall-clock time the raw row was written; carried through the transform.';

CREATE TABLE IF NOT EXISTS transformed."_pending_token_total_supply" AS SELECT "chain_id", "token_id", "block_number", "block_version", "processing_version", "block_timestamp" FROM public."token_total_supply" WHERE false;
ALTER TABLE transformed."_pending_token_total_supply" ADD COLUMN IF NOT EXISTS enqueued_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS "_pending_token_total_supply_enqueued_at" ON transformed."_pending_token_total_supply" (enqueued_at);
COMMENT ON TABLE transformed."_pending_token_total_supply" IS '[Operational] Change queue for transformed.token_total_supply. The AFTER INSERT trigger on public.token_total_supply appends the raw PK here; transformed._run_token_total_supply() drains it. A backlog with an old enqueued_at means the transform has stalled.';
CREATE OR REPLACE FUNCTION transformed."_enqueue_token_total_supply"() RETURNS trigger
  LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, transformed, public AS $tg$
BEGIN
  INSERT INTO transformed."_pending_token_total_supply"("chain_id", "token_id", "block_number", "block_version", "processing_version", "block_timestamp") VALUES (NEW."chain_id", NEW."token_id", NEW."block_number", NEW."block_version", NEW."processing_version", NEW."block_timestamp");
  RETURN NEW;
END $tg$;
DROP TRIGGER IF EXISTS "_transform_enqueue" ON public."token_total_supply";
CREATE TRIGGER "_transform_enqueue" AFTER INSERT ON public."token_total_supply" FOR EACH ROW EXECUTE FUNCTION transformed."_enqueue_token_total_supply"();
CREATE OR REPLACE FUNCTION transformed._run_token_total_supply() RETURNS TABLE(consumed bigint, upserted bigint) AS $fn$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('transformed._run_token_total_supply', 0));
  RETURN QUERY
  WITH batch AS (
    DELETE FROM transformed."_pending_token_total_supply"
    WHERE ctid IN (SELECT ctid FROM transformed."_pending_token_total_supply" LIMIT 10000)
    RETURNING "chain_id", "token_id", "block_number", "block_version", "processing_version", "block_timestamp"
  ), ins AS (
  INSERT INTO transformed."token_total_supply" AS t ("chain_id", "token_id", "total_supply", "scaled_total_supply", "block_number", "block_version", "block_timestamp", "source", "processing_version", "build_id", "created_at")
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
  FROM public."token_total_supply"
  WHERE ("chain_id", "token_id", "block_number", "block_version", "processing_version", "block_timestamp") IN (SELECT DISTINCT "chain_id", "token_id", "block_number", "block_version", "processing_version", "block_timestamp" FROM batch)
    AND "block_timestamp" >= (SELECT min("block_timestamp") FROM batch)
    AND "block_timestamp" <= (SELECT max("block_timestamp") FROM batch)
  ON CONFLICT ("chain_id", "token_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "total_supply"=EXCLUDED."total_supply", "scaled_total_supply"=EXCLUDED."scaled_total_supply", "source"=EXCLUDED."source", "build_id"=EXCLUDED."build_id", "created_at"=EXCLUDED."created_at"
    WHERE (t."total_supply", t."scaled_total_supply", t."source", t."build_id", t."created_at") IS DISTINCT FROM (EXCLUDED."total_supply", EXCLUDED."scaled_total_supply", EXCLUDED."source", EXCLUDED."build_id", EXCLUDED."created_at")
  RETURNING 1)
  SELECT (SELECT count(*) FROM batch), (SELECT count(*) FROM ins);
END $fn$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION transformed._bootstrap_token_total_supply(_from timestamptz, _to timestamptz) RETURNS bigint AS $fn$
DECLARE n bigint;
BEGIN
  WITH ins AS (
  INSERT INTO transformed."token_total_supply" AS t ("chain_id", "token_id", "total_supply", "scaled_total_supply", "block_number", "block_version", "block_timestamp", "source", "processing_version", "build_id", "created_at")
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
  FROM public."token_total_supply"
  WHERE "block_timestamp" >= _from AND "block_timestamp" < _to
  ON CONFLICT ("chain_id", "token_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "total_supply"=EXCLUDED."total_supply", "scaled_total_supply"=EXCLUDED."scaled_total_supply", "source"=EXCLUDED."source", "build_id"=EXCLUDED."build_id", "created_at"=EXCLUDED."created_at"
    WHERE (t."total_supply", t."scaled_total_supply", t."source", t."build_id", t."created_at") IS DISTINCT FROM (EXCLUDED."total_supply", EXCLUDED."scaled_total_supply", EXCLUDED."source", EXCLUDED."build_id", EXCLUDED."created_at")
  RETURNING 1)
  SELECT count(*) INTO n FROM ins;
  RETURN n;
END $fn$ LANGUAGE plpgsql;
INSERT INTO transformed._sources(source) VALUES ('token_total_supply') ON CONFLICT DO NOTHING;

-- ===== onchain_token_price =====
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
SELECT create_hypertable('transformed."onchain_token_price"','block_timestamp',chunk_time_interval=>INTERVAL '1 day',if_not_exists=>TRUE);
ALTER TABLE transformed."onchain_token_price" SET (timescaledb.compress, timescaledb.compress_segmentby = 'oracle_id, token_id', timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC');
SELECT add_compression_policy('transformed."onchain_token_price"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."onchain_token_price"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.onchain_token_price'; END $$;
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
COMMENT ON COLUMN transformed."onchain_token_price".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row (it identifies a git build, not a commit order). Not used by the transform refresh, which is queue-driven; carried through for lineage.';

CREATE TABLE IF NOT EXISTS transformed."_pending_onchain_token_price" AS SELECT "token_id", "oracle_id", "block_number", "block_version", "processing_version", "timestamp" FROM public."onchain_token_price" WHERE false;
ALTER TABLE transformed."_pending_onchain_token_price" ADD COLUMN IF NOT EXISTS enqueued_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS "_pending_onchain_token_price_enqueued_at" ON transformed."_pending_onchain_token_price" (enqueued_at);
COMMENT ON TABLE transformed."_pending_onchain_token_price" IS '[Operational] Change queue for transformed.onchain_token_price. The AFTER INSERT trigger on public.onchain_token_price appends the raw PK here; transformed._run_onchain_token_price() drains it. A backlog with an old enqueued_at means the transform has stalled.';
CREATE OR REPLACE FUNCTION transformed."_enqueue_onchain_token_price"() RETURNS trigger
  LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, transformed, public AS $tg$
BEGIN
  INSERT INTO transformed."_pending_onchain_token_price"("token_id", "oracle_id", "block_number", "block_version", "processing_version", "timestamp") VALUES (NEW."token_id", NEW."oracle_id", NEW."block_number", NEW."block_version", NEW."processing_version", NEW."timestamp");
  RETURN NEW;
END $tg$;
DROP TRIGGER IF EXISTS "_transform_enqueue" ON public."onchain_token_price";
CREATE TRIGGER "_transform_enqueue" AFTER INSERT ON public."onchain_token_price" FOR EACH ROW EXECUTE FUNCTION transformed."_enqueue_onchain_token_price"();
CREATE OR REPLACE FUNCTION transformed._run_onchain_token_price() RETURNS TABLE(consumed bigint, upserted bigint) AS $fn$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('transformed._run_onchain_token_price', 0));
  RETURN QUERY
  WITH batch AS (
    DELETE FROM transformed."_pending_onchain_token_price"
    WHERE ctid IN (SELECT ctid FROM transformed."_pending_onchain_token_price" LIMIT 10000)
    RETURNING "token_id", "oracle_id", "block_number", "block_version", "processing_version", "timestamp"
  ), ins AS (
  INSERT INTO transformed."onchain_token_price" AS t ("chain_id", "token_id", "oracle_id", "block_number", "block_version", "block_timestamp", "price_usd", "processing_version", "build_id")
SELECT p."chain_id",
       s."token_id",
       CAST(s."oracle_id" AS bigint) AS "oracle_id",
       s."block_number",
       CAST(s."block_version" AS integer) AS "block_version",
       s."timestamp" AS "block_timestamp",
       s."price_usd",
       s."processing_version",
       s."build_id"
  FROM public."onchain_token_price" s LEFT JOIN public."oracle" p ON p."id"=s."oracle_id"
  WHERE (s."token_id", s."oracle_id", s."block_number", s."block_version", s."processing_version", s."timestamp") IN (SELECT DISTINCT "token_id", "oracle_id", "block_number", "block_version", "processing_version", "timestamp" FROM batch)
    AND s."timestamp" >= (SELECT min("timestamp") FROM batch)
    AND s."timestamp" <= (SELECT max("timestamp") FROM batch)
  ON CONFLICT ("token_id", "oracle_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "price_usd"=EXCLUDED."price_usd", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."price_usd", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."price_usd", EXCLUDED."build_id")
  RETURNING 1)
  SELECT (SELECT count(*) FROM batch), (SELECT count(*) FROM ins);
END $fn$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION transformed._bootstrap_onchain_token_price(_from timestamptz, _to timestamptz) RETURNS bigint AS $fn$
DECLARE n bigint;
BEGIN
  WITH ins AS (
  INSERT INTO transformed."onchain_token_price" AS t ("chain_id", "token_id", "oracle_id", "block_number", "block_version", "block_timestamp", "price_usd", "processing_version", "build_id")
SELECT p."chain_id",
       s."token_id",
       CAST(s."oracle_id" AS bigint) AS "oracle_id",
       s."block_number",
       CAST(s."block_version" AS integer) AS "block_version",
       s."timestamp" AS "block_timestamp",
       s."price_usd",
       s."processing_version",
       s."build_id"
  FROM public."onchain_token_price" s LEFT JOIN public."oracle" p ON p."id"=s."oracle_id"
  WHERE s."timestamp" >= _from AND s."timestamp" < _to
  ON CONFLICT ("token_id", "oracle_id", "block_number", "block_version", "processing_version", "block_timestamp") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "price_usd"=EXCLUDED."price_usd", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."price_usd", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."price_usd", EXCLUDED."build_id")
  RETURNING 1)
  SELECT count(*) INTO n FROM ins;
  RETURN n;
END $fn$ LANGUAGE plpgsql;
INSERT INTO transformed._sources(source) VALUES ('onchain_token_price') ON CONFLICT DO NOTHING;

-- ===== maple_loan_state =====
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
SELECT create_hypertable('transformed."maple_loan_state"','snapshot_time',chunk_time_interval=>INTERVAL '7 days',if_not_exists=>TRUE);
ALTER TABLE transformed."maple_loan_state" SET (timescaledb.compress, timescaledb.compress_segmentby = 'maple_loan_id', timescaledb.compress_orderby = 'snapshot_time DESC, processing_version DESC');
SELECT add_compression_policy('transformed."maple_loan_state"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."maple_loan_state"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.maple_loan_state'; END $$;
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
COMMENT ON COLUMN transformed."maple_loan_state".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row (it identifies a git build, not a commit order). Not used by the transform refresh, which is queue-driven; carried through for lineage.';

CREATE TABLE IF NOT EXISTS transformed."_pending_maple_loan_state" AS SELECT "maple_loan_id", "synced_at", "processing_version" FROM public."maple_loan_state" WHERE false;
ALTER TABLE transformed."_pending_maple_loan_state" ADD COLUMN IF NOT EXISTS enqueued_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS "_pending_maple_loan_state_enqueued_at" ON transformed."_pending_maple_loan_state" (enqueued_at);
COMMENT ON TABLE transformed."_pending_maple_loan_state" IS '[Operational] Change queue for transformed.maple_loan_state. The AFTER INSERT trigger on public.maple_loan_state appends the raw PK here; transformed._run_maple_loan_state() drains it. A backlog with an old enqueued_at means the transform has stalled.';
CREATE OR REPLACE FUNCTION transformed."_enqueue_maple_loan_state"() RETURNS trigger
  LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, transformed, public AS $tg$
BEGIN
  INSERT INTO transformed."_pending_maple_loan_state"("maple_loan_id", "synced_at", "processing_version") VALUES (NEW."maple_loan_id", NEW."synced_at", NEW."processing_version");
  RETURN NEW;
END $tg$;
DROP TRIGGER IF EXISTS "_transform_enqueue" ON public."maple_loan_state";
CREATE TRIGGER "_transform_enqueue" AFTER INSERT ON public."maple_loan_state" FOR EACH ROW EXECUTE FUNCTION transformed."_enqueue_maple_loan_state"();
CREATE OR REPLACE FUNCTION transformed._run_maple_loan_state() RETURNS TABLE(consumed bigint, upserted bigint) AS $fn$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('transformed._run_maple_loan_state', 0));
  RETURN QUERY
  WITH batch AS (
    DELETE FROM transformed."_pending_maple_loan_state"
    WHERE ctid IN (SELECT ctid FROM transformed."_pending_maple_loan_state" LIMIT 10000)
    RETURNING "maple_loan_id", "synced_at", "processing_version"
  ), ins AS (
  INSERT INTO transformed."maple_loan_state" AS t ("chain_id", "protocol_id", "maple_loan_id", "snapshot_time", "state", "principal_owed", "acm_ratio", "processing_version", "build_id")
SELECT p."chain_id",
       p."protocol_id",
       s."maple_loan_id",
       s."synced_at" AS "snapshot_time",
       s."state",
       s."principal_owed",
       s."acm_ratio",
       s."processing_version",
       s."build_id"
  FROM public."maple_loan_state" s LEFT JOIN public."maple_loan" p ON p."id"=s."maple_loan_id"
  WHERE (s."maple_loan_id", s."synced_at", s."processing_version") IN (SELECT DISTINCT "maple_loan_id", "synced_at", "processing_version" FROM batch)
    AND s."synced_at" >= (SELECT min("synced_at") FROM batch)
    AND s."synced_at" <= (SELECT max("synced_at") FROM batch)
  ON CONFLICT ("maple_loan_id", "snapshot_time", "processing_version") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "state"=EXCLUDED."state", "principal_owed"=EXCLUDED."principal_owed", "acm_ratio"=EXCLUDED."acm_ratio", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."state", t."principal_owed", t."acm_ratio", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."state", EXCLUDED."principal_owed", EXCLUDED."acm_ratio", EXCLUDED."build_id")
  RETURNING 1)
  SELECT (SELECT count(*) FROM batch), (SELECT count(*) FROM ins);
END $fn$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION transformed._bootstrap_maple_loan_state(_from timestamptz, _to timestamptz) RETURNS bigint AS $fn$
DECLARE n bigint;
BEGIN
  WITH ins AS (
  INSERT INTO transformed."maple_loan_state" AS t ("chain_id", "protocol_id", "maple_loan_id", "snapshot_time", "state", "principal_owed", "acm_ratio", "processing_version", "build_id")
SELECT p."chain_id",
       p."protocol_id",
       s."maple_loan_id",
       s."synced_at" AS "snapshot_time",
       s."state",
       s."principal_owed",
       s."acm_ratio",
       s."processing_version",
       s."build_id"
  FROM public."maple_loan_state" s LEFT JOIN public."maple_loan" p ON p."id"=s."maple_loan_id"
  WHERE s."synced_at" >= _from AND s."synced_at" < _to
  ON CONFLICT ("maple_loan_id", "snapshot_time", "processing_version") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "state"=EXCLUDED."state", "principal_owed"=EXCLUDED."principal_owed", "acm_ratio"=EXCLUDED."acm_ratio", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."state", t."principal_owed", t."acm_ratio", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."state", EXCLUDED."principal_owed", EXCLUDED."acm_ratio", EXCLUDED."build_id")
  RETURNING 1)
  SELECT count(*) INTO n FROM ins;
  RETURN n;
END $fn$ LANGUAGE plpgsql;
INSERT INTO transformed._sources(source) VALUES ('maple_loan_state') ON CONFLICT DO NOTHING;

-- ===== maple_loan_collateral =====
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
SELECT create_hypertable('transformed."maple_loan_collateral"','snapshot_time',chunk_time_interval=>INTERVAL '7 days',if_not_exists=>TRUE);
ALTER TABLE transformed."maple_loan_collateral" SET (timescaledb.compress, timescaledb.compress_segmentby = 'maple_loan_id', timescaledb.compress_orderby = 'snapshot_time DESC, processing_version DESC');
SELECT add_compression_policy('transformed."maple_loan_collateral"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."maple_loan_collateral"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.maple_loan_collateral'; END $$;
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
COMMENT ON COLUMN transformed."maple_loan_collateral".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row (it identifies a git build, not a commit order). Not used by the transform refresh, which is queue-driven; carried through for lineage.';

CREATE TABLE IF NOT EXISTS transformed."_pending_maple_loan_collateral" AS SELECT "maple_loan_id", "synced_at", "processing_version" FROM public."maple_loan_collateral" WHERE false;
ALTER TABLE transformed."_pending_maple_loan_collateral" ADD COLUMN IF NOT EXISTS enqueued_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS "_pending_maple_loan_collateral_enqueued_at" ON transformed."_pending_maple_loan_collateral" (enqueued_at);
COMMENT ON TABLE transformed."_pending_maple_loan_collateral" IS '[Operational] Change queue for transformed.maple_loan_collateral. The AFTER INSERT trigger on public.maple_loan_collateral appends the raw PK here; transformed._run_maple_loan_collateral() drains it. A backlog with an old enqueued_at means the transform has stalled.';
CREATE OR REPLACE FUNCTION transformed."_enqueue_maple_loan_collateral"() RETURNS trigger
  LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, transformed, public AS $tg$
BEGIN
  INSERT INTO transformed."_pending_maple_loan_collateral"("maple_loan_id", "synced_at", "processing_version") VALUES (NEW."maple_loan_id", NEW."synced_at", NEW."processing_version");
  RETURN NEW;
END $tg$;
DROP TRIGGER IF EXISTS "_transform_enqueue" ON public."maple_loan_collateral";
CREATE TRIGGER "_transform_enqueue" AFTER INSERT ON public."maple_loan_collateral" FOR EACH ROW EXECUTE FUNCTION transformed."_enqueue_maple_loan_collateral"();
CREATE OR REPLACE FUNCTION transformed._run_maple_loan_collateral() RETURNS TABLE(consumed bigint, upserted bigint) AS $fn$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('transformed._run_maple_loan_collateral', 0));
  RETURN QUERY
  WITH batch AS (
    DELETE FROM transformed."_pending_maple_loan_collateral"
    WHERE ctid IN (SELECT ctid FROM transformed."_pending_maple_loan_collateral" LIMIT 10000)
    RETURNING "maple_loan_id", "synced_at", "processing_version"
  ), ins AS (
  INSERT INTO transformed."maple_loan_collateral" AS t ("chain_id", "protocol_id", "maple_loan_id", "snapshot_time", "asset_symbol", "asset_amount", "asset_decimals", "asset_value_usd", "state", "custodian", "liquidation_level", "processing_version", "build_id")
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
  FROM public."maple_loan_collateral" s LEFT JOIN public."maple_loan" p ON p."id"=s."maple_loan_id"
  WHERE (s."maple_loan_id", s."synced_at", s."processing_version") IN (SELECT DISTINCT "maple_loan_id", "synced_at", "processing_version" FROM batch)
    AND s."synced_at" >= (SELECT min("synced_at") FROM batch)
    AND s."synced_at" <= (SELECT max("synced_at") FROM batch)
  ON CONFLICT ("maple_loan_id", "snapshot_time", "processing_version") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "asset_symbol"=EXCLUDED."asset_symbol", "asset_amount"=EXCLUDED."asset_amount", "asset_decimals"=EXCLUDED."asset_decimals", "asset_value_usd"=EXCLUDED."asset_value_usd", "state"=EXCLUDED."state", "custodian"=EXCLUDED."custodian", "liquidation_level"=EXCLUDED."liquidation_level", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."asset_symbol", t."asset_amount", t."asset_decimals", t."asset_value_usd", t."state", t."custodian", t."liquidation_level", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."asset_symbol", EXCLUDED."asset_amount", EXCLUDED."asset_decimals", EXCLUDED."asset_value_usd", EXCLUDED."state", EXCLUDED."custodian", EXCLUDED."liquidation_level", EXCLUDED."build_id")
  RETURNING 1)
  SELECT (SELECT count(*) FROM batch), (SELECT count(*) FROM ins);
END $fn$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION transformed._bootstrap_maple_loan_collateral(_from timestamptz, _to timestamptz) RETURNS bigint AS $fn$
DECLARE n bigint;
BEGIN
  WITH ins AS (
  INSERT INTO transformed."maple_loan_collateral" AS t ("chain_id", "protocol_id", "maple_loan_id", "snapshot_time", "asset_symbol", "asset_amount", "asset_decimals", "asset_value_usd", "state", "custodian", "liquidation_level", "processing_version", "build_id")
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
  FROM public."maple_loan_collateral" s LEFT JOIN public."maple_loan" p ON p."id"=s."maple_loan_id"
  WHERE s."synced_at" >= _from AND s."synced_at" < _to
  ON CONFLICT ("maple_loan_id", "snapshot_time", "processing_version") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "asset_symbol"=EXCLUDED."asset_symbol", "asset_amount"=EXCLUDED."asset_amount", "asset_decimals"=EXCLUDED."asset_decimals", "asset_value_usd"=EXCLUDED."asset_value_usd", "state"=EXCLUDED."state", "custodian"=EXCLUDED."custodian", "liquidation_level"=EXCLUDED."liquidation_level", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."asset_symbol", t."asset_amount", t."asset_decimals", t."asset_value_usd", t."state", t."custodian", t."liquidation_level", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."asset_symbol", EXCLUDED."asset_amount", EXCLUDED."asset_decimals", EXCLUDED."asset_value_usd", EXCLUDED."state", EXCLUDED."custodian", EXCLUDED."liquidation_level", EXCLUDED."build_id")
  RETURNING 1)
  SELECT count(*) INTO n FROM ins;
  RETURN n;
END $fn$ LANGUAGE plpgsql;
INSERT INTO transformed._sources(source) VALUES ('maple_loan_collateral') ON CONFLICT DO NOTHING;

-- ===== maple_pool_state =====
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
SELECT create_hypertable('transformed."maple_pool_state"','snapshot_time',chunk_time_interval=>INTERVAL '7 days',if_not_exists=>TRUE);
ALTER TABLE transformed."maple_pool_state" SET (timescaledb.compress, timescaledb.compress_segmentby = 'maple_pool_id', timescaledb.compress_orderby = 'snapshot_time DESC, processing_version DESC');
SELECT add_compression_policy('transformed."maple_pool_state"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."maple_pool_state"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.maple_pool_state'; END $$;
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
COMMENT ON COLUMN transformed."maple_pool_state".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row (it identifies a git build, not a commit order). Not used by the transform refresh, which is queue-driven; carried through for lineage.';

CREATE TABLE IF NOT EXISTS transformed."_pending_maple_pool_state" AS SELECT "maple_pool_id", "synced_at", "processing_version" FROM public."maple_pool_state" WHERE false;
ALTER TABLE transformed."_pending_maple_pool_state" ADD COLUMN IF NOT EXISTS enqueued_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS "_pending_maple_pool_state_enqueued_at" ON transformed."_pending_maple_pool_state" (enqueued_at);
COMMENT ON TABLE transformed."_pending_maple_pool_state" IS '[Operational] Change queue for transformed.maple_pool_state. The AFTER INSERT trigger on public.maple_pool_state appends the raw PK here; transformed._run_maple_pool_state() drains it. A backlog with an old enqueued_at means the transform has stalled.';
CREATE OR REPLACE FUNCTION transformed."_enqueue_maple_pool_state"() RETURNS trigger
  LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, transformed, public AS $tg$
BEGIN
  INSERT INTO transformed."_pending_maple_pool_state"("maple_pool_id", "synced_at", "processing_version") VALUES (NEW."maple_pool_id", NEW."synced_at", NEW."processing_version");
  RETURN NEW;
END $tg$;
DROP TRIGGER IF EXISTS "_transform_enqueue" ON public."maple_pool_state";
CREATE TRIGGER "_transform_enqueue" AFTER INSERT ON public."maple_pool_state" FOR EACH ROW EXECUTE FUNCTION transformed."_enqueue_maple_pool_state"();
CREATE OR REPLACE FUNCTION transformed._run_maple_pool_state() RETURNS TABLE(consumed bigint, upserted bigint) AS $fn$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('transformed._run_maple_pool_state', 0));
  RETURN QUERY
  WITH batch AS (
    DELETE FROM transformed."_pending_maple_pool_state"
    WHERE ctid IN (SELECT ctid FROM transformed."_pending_maple_pool_state" LIMIT 10000)
    RETURNING "maple_pool_id", "synced_at", "processing_version"
  ), ins AS (
  INSERT INTO transformed."maple_pool_state" AS t ("chain_id", "protocol_id", "maple_pool_id", "snapshot_time", "tvl", "liquid_assets", "collateral_value_usd", "principal_out", "utilization", "monthly_apy", "spot_apy", "processing_version", "build_id")
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
  FROM public."maple_pool_state" s LEFT JOIN public."maple_pool" p ON p."id"=s."maple_pool_id"
  WHERE (s."maple_pool_id", s."synced_at", s."processing_version") IN (SELECT DISTINCT "maple_pool_id", "synced_at", "processing_version" FROM batch)
    AND s."synced_at" >= (SELECT min("synced_at") FROM batch)
    AND s."synced_at" <= (SELECT max("synced_at") FROM batch)
  ON CONFLICT ("maple_pool_id", "snapshot_time", "processing_version") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "tvl"=EXCLUDED."tvl", "liquid_assets"=EXCLUDED."liquid_assets", "collateral_value_usd"=EXCLUDED."collateral_value_usd", "principal_out"=EXCLUDED."principal_out", "utilization"=EXCLUDED."utilization", "monthly_apy"=EXCLUDED."monthly_apy", "spot_apy"=EXCLUDED."spot_apy", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."tvl", t."liquid_assets", t."collateral_value_usd", t."principal_out", t."utilization", t."monthly_apy", t."spot_apy", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."tvl", EXCLUDED."liquid_assets", EXCLUDED."collateral_value_usd", EXCLUDED."principal_out", EXCLUDED."utilization", EXCLUDED."monthly_apy", EXCLUDED."spot_apy", EXCLUDED."build_id")
  RETURNING 1)
  SELECT (SELECT count(*) FROM batch), (SELECT count(*) FROM ins);
END $fn$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION transformed._bootstrap_maple_pool_state(_from timestamptz, _to timestamptz) RETURNS bigint AS $fn$
DECLARE n bigint;
BEGIN
  WITH ins AS (
  INSERT INTO transformed."maple_pool_state" AS t ("chain_id", "protocol_id", "maple_pool_id", "snapshot_time", "tvl", "liquid_assets", "collateral_value_usd", "principal_out", "utilization", "monthly_apy", "spot_apy", "processing_version", "build_id")
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
  FROM public."maple_pool_state" s LEFT JOIN public."maple_pool" p ON p."id"=s."maple_pool_id"
  WHERE s."synced_at" >= _from AND s."synced_at" < _to
  ON CONFLICT ("maple_pool_id", "snapshot_time", "processing_version") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "tvl"=EXCLUDED."tvl", "liquid_assets"=EXCLUDED."liquid_assets", "collateral_value_usd"=EXCLUDED."collateral_value_usd", "principal_out"=EXCLUDED."principal_out", "utilization"=EXCLUDED."utilization", "monthly_apy"=EXCLUDED."monthly_apy", "spot_apy"=EXCLUDED."spot_apy", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."tvl", t."liquid_assets", t."collateral_value_usd", t."principal_out", t."utilization", t."monthly_apy", t."spot_apy", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."tvl", EXCLUDED."liquid_assets", EXCLUDED."collateral_value_usd", EXCLUDED."principal_out", EXCLUDED."utilization", EXCLUDED."monthly_apy", EXCLUDED."spot_apy", EXCLUDED."build_id")
  RETURNING 1)
  SELECT count(*) INTO n FROM ins;
  RETURN n;
END $fn$ LANGUAGE plpgsql;
INSERT INTO transformed._sources(source) VALUES ('maple_pool_state') ON CONFLICT DO NOTHING;

-- ===== maple_sky_strategy_state =====
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
SELECT create_hypertable('transformed."maple_sky_strategy_state"','snapshot_time',chunk_time_interval=>INTERVAL '7 days',if_not_exists=>TRUE);
ALTER TABLE transformed."maple_sky_strategy_state" SET (timescaledb.compress, timescaledb.compress_segmentby = 'maple_sky_strategy_id', timescaledb.compress_orderby = 'snapshot_time DESC, processing_version DESC');
SELECT add_compression_policy('transformed."maple_sky_strategy_state"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."maple_sky_strategy_state"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.maple_sky_strategy_state'; END $$;
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
COMMENT ON COLUMN transformed."maple_sky_strategy_state".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row (it identifies a git build, not a commit order). Not used by the transform refresh, which is queue-driven; carried through for lineage.';

CREATE TABLE IF NOT EXISTS transformed."_pending_maple_sky_strategy_state" AS SELECT "maple_sky_strategy_id", "synced_at", "processing_version" FROM public."maple_sky_strategy_state" WHERE false;
ALTER TABLE transformed."_pending_maple_sky_strategy_state" ADD COLUMN IF NOT EXISTS enqueued_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS "_pending_maple_sky_strategy_state_enqueued_at" ON transformed."_pending_maple_sky_strategy_state" (enqueued_at);
COMMENT ON TABLE transformed."_pending_maple_sky_strategy_state" IS '[Operational] Change queue for transformed.maple_sky_strategy_state. The AFTER INSERT trigger on public.maple_sky_strategy_state appends the raw PK here; transformed._run_maple_sky_strategy_state() drains it. A backlog with an old enqueued_at means the transform has stalled.';
CREATE OR REPLACE FUNCTION transformed."_enqueue_maple_sky_strategy_state"() RETURNS trigger
  LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, transformed, public AS $tg$
BEGIN
  INSERT INTO transformed."_pending_maple_sky_strategy_state"("maple_sky_strategy_id", "synced_at", "processing_version") VALUES (NEW."maple_sky_strategy_id", NEW."synced_at", NEW."processing_version");
  RETURN NEW;
END $tg$;
DROP TRIGGER IF EXISTS "_transform_enqueue" ON public."maple_sky_strategy_state";
CREATE TRIGGER "_transform_enqueue" AFTER INSERT ON public."maple_sky_strategy_state" FOR EACH ROW EXECUTE FUNCTION transformed."_enqueue_maple_sky_strategy_state"();
CREATE OR REPLACE FUNCTION transformed._run_maple_sky_strategy_state() RETURNS TABLE(consumed bigint, upserted bigint) AS $fn$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('transformed._run_maple_sky_strategy_state', 0));
  RETURN QUERY
  WITH batch AS (
    DELETE FROM transformed."_pending_maple_sky_strategy_state"
    WHERE ctid IN (SELECT ctid FROM transformed."_pending_maple_sky_strategy_state" LIMIT 10000)
    RETURNING "maple_sky_strategy_id", "synced_at", "processing_version"
  ), ins AS (
  INSERT INTO transformed."maple_sky_strategy_state" AS t ("chain_id", "protocol_id", "maple_sky_strategy_id", "snapshot_time", "state", "currently_deployed", "deposited_assets", "withdrawn_assets", "strategy_fee_rate", "total_fees_collected", "processing_version", "build_id")
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
  FROM public."maple_sky_strategy_state" s LEFT JOIN public."maple_sky_strategy" p ON p."id"=s."maple_sky_strategy_id"
  WHERE (s."maple_sky_strategy_id", s."synced_at", s."processing_version") IN (SELECT DISTINCT "maple_sky_strategy_id", "synced_at", "processing_version" FROM batch)
    AND s."synced_at" >= (SELECT min("synced_at") FROM batch)
    AND s."synced_at" <= (SELECT max("synced_at") FROM batch)
  ON CONFLICT ("maple_sky_strategy_id", "snapshot_time", "processing_version") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "state"=EXCLUDED."state", "currently_deployed"=EXCLUDED."currently_deployed", "deposited_assets"=EXCLUDED."deposited_assets", "withdrawn_assets"=EXCLUDED."withdrawn_assets", "strategy_fee_rate"=EXCLUDED."strategy_fee_rate", "total_fees_collected"=EXCLUDED."total_fees_collected", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."state", t."currently_deployed", t."deposited_assets", t."withdrawn_assets", t."strategy_fee_rate", t."total_fees_collected", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."state", EXCLUDED."currently_deployed", EXCLUDED."deposited_assets", EXCLUDED."withdrawn_assets", EXCLUDED."strategy_fee_rate", EXCLUDED."total_fees_collected", EXCLUDED."build_id")
  RETURNING 1)
  SELECT (SELECT count(*) FROM batch), (SELECT count(*) FROM ins);
END $fn$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION transformed._bootstrap_maple_sky_strategy_state(_from timestamptz, _to timestamptz) RETURNS bigint AS $fn$
DECLARE n bigint;
BEGIN
  WITH ins AS (
  INSERT INTO transformed."maple_sky_strategy_state" AS t ("chain_id", "protocol_id", "maple_sky_strategy_id", "snapshot_time", "state", "currently_deployed", "deposited_assets", "withdrawn_assets", "strategy_fee_rate", "total_fees_collected", "processing_version", "build_id")
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
  FROM public."maple_sky_strategy_state" s LEFT JOIN public."maple_sky_strategy" p ON p."id"=s."maple_sky_strategy_id"
  WHERE s."synced_at" >= _from AND s."synced_at" < _to
  ON CONFLICT ("maple_sky_strategy_id", "snapshot_time", "processing_version") DO UPDATE SET "chain_id"=EXCLUDED."chain_id", "protocol_id"=EXCLUDED."protocol_id", "state"=EXCLUDED."state", "currently_deployed"=EXCLUDED."currently_deployed", "deposited_assets"=EXCLUDED."deposited_assets", "withdrawn_assets"=EXCLUDED."withdrawn_assets", "strategy_fee_rate"=EXCLUDED."strategy_fee_rate", "total_fees_collected"=EXCLUDED."total_fees_collected", "build_id"=EXCLUDED."build_id"
    WHERE (t."chain_id", t."protocol_id", t."state", t."currently_deployed", t."deposited_assets", t."withdrawn_assets", t."strategy_fee_rate", t."total_fees_collected", t."build_id") IS DISTINCT FROM (EXCLUDED."chain_id", EXCLUDED."protocol_id", EXCLUDED."state", EXCLUDED."currently_deployed", EXCLUDED."deposited_assets", EXCLUDED."withdrawn_assets", EXCLUDED."strategy_fee_rate", EXCLUDED."total_fees_collected", EXCLUDED."build_id")
  RETURNING 1)
  SELECT count(*) INTO n FROM ins;
  RETURN n;
END $fn$ LANGUAGE plpgsql;
INSERT INTO transformed._sources(source) VALUES ('maple_sky_strategy_state') ON CONFLICT DO NOTHING;

-- ===== maple_syrup_global_state =====
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
SELECT create_hypertable('transformed."maple_syrup_global_state"','snapshot_time',chunk_time_interval=>INTERVAL '7 days',if_not_exists=>TRUE);
ALTER TABLE transformed."maple_syrup_global_state" SET (timescaledb.compress, timescaledb.compress_segmentby = 'chain_id', timescaledb.compress_orderby = 'snapshot_time DESC, processing_version DESC');
SELECT add_compression_policy('transformed."maple_syrup_global_state"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."maple_syrup_global_state"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.maple_syrup_global_state'; END $$;
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
COMMENT ON COLUMN transformed."maple_syrup_global_state".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row (it identifies a git build, not a commit order). Not used by the transform refresh, which is queue-driven; carried through for lineage.';

CREATE TABLE IF NOT EXISTS transformed."_pending_maple_syrup_global_state" AS SELECT "chain_id", "synced_at", "processing_version" FROM public."maple_syrup_global_state" WHERE false;
ALTER TABLE transformed."_pending_maple_syrup_global_state" ADD COLUMN IF NOT EXISTS enqueued_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS "_pending_maple_syrup_global_state_enqueued_at" ON transformed."_pending_maple_syrup_global_state" (enqueued_at);
COMMENT ON TABLE transformed."_pending_maple_syrup_global_state" IS '[Operational] Change queue for transformed.maple_syrup_global_state. The AFTER INSERT trigger on public.maple_syrup_global_state appends the raw PK here; transformed._run_maple_syrup_global_state() drains it. A backlog with an old enqueued_at means the transform has stalled.';
CREATE OR REPLACE FUNCTION transformed."_enqueue_maple_syrup_global_state"() RETURNS trigger
  LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, transformed, public AS $tg$
BEGIN
  INSERT INTO transformed."_pending_maple_syrup_global_state"("chain_id", "synced_at", "processing_version") VALUES (NEW."chain_id", NEW."synced_at", NEW."processing_version");
  RETURN NEW;
END $tg$;
DROP TRIGGER IF EXISTS "_transform_enqueue" ON public."maple_syrup_global_state";
CREATE TRIGGER "_transform_enqueue" AFTER INSERT ON public."maple_syrup_global_state" FOR EACH ROW EXECUTE FUNCTION transformed."_enqueue_maple_syrup_global_state"();
CREATE OR REPLACE FUNCTION transformed._run_maple_syrup_global_state() RETURNS TABLE(consumed bigint, upserted bigint) AS $fn$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('transformed._run_maple_syrup_global_state', 0));
  RETURN QUERY
  WITH batch AS (
    DELETE FROM transformed."_pending_maple_syrup_global_state"
    WHERE ctid IN (SELECT ctid FROM transformed."_pending_maple_syrup_global_state" LIMIT 10000)
    RETURNING "chain_id", "synced_at", "processing_version"
  ), ins AS (
  INSERT INTO transformed."maple_syrup_global_state" AS t ("chain_id", "snapshot_time", "tvl", "apy", "collateral_apy", "pool_apy", "drips_yield_boost", "processing_version", "build_id")
SELECT "chain_id",
       "synced_at" AS "snapshot_time",
       "tvl",
       "apy",
       "collateral_apy",
       "pool_apy",
       "drips_yield_boost",
       "processing_version",
       "build_id"
  FROM public."maple_syrup_global_state"
  WHERE ("chain_id", "synced_at", "processing_version") IN (SELECT DISTINCT "chain_id", "synced_at", "processing_version" FROM batch)
    AND "synced_at" >= (SELECT min("synced_at") FROM batch)
    AND "synced_at" <= (SELECT max("synced_at") FROM batch)
  ON CONFLICT ("chain_id", "snapshot_time", "processing_version") DO UPDATE SET "tvl"=EXCLUDED."tvl", "apy"=EXCLUDED."apy", "collateral_apy"=EXCLUDED."collateral_apy", "pool_apy"=EXCLUDED."pool_apy", "drips_yield_boost"=EXCLUDED."drips_yield_boost", "build_id"=EXCLUDED."build_id"
    WHERE (t."tvl", t."apy", t."collateral_apy", t."pool_apy", t."drips_yield_boost", t."build_id") IS DISTINCT FROM (EXCLUDED."tvl", EXCLUDED."apy", EXCLUDED."collateral_apy", EXCLUDED."pool_apy", EXCLUDED."drips_yield_boost", EXCLUDED."build_id")
  RETURNING 1)
  SELECT (SELECT count(*) FROM batch), (SELECT count(*) FROM ins);
END $fn$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION transformed._bootstrap_maple_syrup_global_state(_from timestamptz, _to timestamptz) RETURNS bigint AS $fn$
DECLARE n bigint;
BEGIN
  WITH ins AS (
  INSERT INTO transformed."maple_syrup_global_state" AS t ("chain_id", "snapshot_time", "tvl", "apy", "collateral_apy", "pool_apy", "drips_yield_boost", "processing_version", "build_id")
SELECT "chain_id",
       "synced_at" AS "snapshot_time",
       "tvl",
       "apy",
       "collateral_apy",
       "pool_apy",
       "drips_yield_boost",
       "processing_version",
       "build_id"
  FROM public."maple_syrup_global_state"
  WHERE "synced_at" >= _from AND "synced_at" < _to
  ON CONFLICT ("chain_id", "snapshot_time", "processing_version") DO UPDATE SET "tvl"=EXCLUDED."tvl", "apy"=EXCLUDED."apy", "collateral_apy"=EXCLUDED."collateral_apy", "pool_apy"=EXCLUDED."pool_apy", "drips_yield_boost"=EXCLUDED."drips_yield_boost", "build_id"=EXCLUDED."build_id"
    WHERE (t."tvl", t."apy", t."collateral_apy", t."pool_apy", t."drips_yield_boost", t."build_id") IS DISTINCT FROM (EXCLUDED."tvl", EXCLUDED."apy", EXCLUDED."collateral_apy", EXCLUDED."pool_apy", EXCLUDED."drips_yield_boost", EXCLUDED."build_id")
  RETURNING 1)
  SELECT count(*) INTO n FROM ins;
  RETURN n;
END $fn$ LANGUAGE plpgsql;
INSERT INTO transformed._sources(source) VALUES ('maple_syrup_global_state') ON CONFLICT DO NOTHING;

-- ===== offchain_token_price =====
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
SELECT create_hypertable('transformed."offchain_token_price"','snapshot_time',chunk_time_interval=>INTERVAL '1 day',if_not_exists=>TRUE);
ALTER TABLE transformed."offchain_token_price" SET (timescaledb.compress, timescaledb.compress_segmentby = 'token_id', timescaledb.compress_orderby = 'snapshot_time DESC, processing_version DESC');
SELECT add_compression_policy('transformed."offchain_token_price"', INTERVAL '2 days', if_not_exists => TRUE);
DO $$ BEGIN PERFORM add_tiering_policy('transformed."offchain_token_price"', INTERVAL '1 year', if_not_exists => TRUE); EXCEPTION WHEN undefined_function THEN RAISE NOTICE 'add_tiering_policy not available, skipping tiering for transformed.offchain_token_price'; END $$;
COMMENT ON TABLE transformed."offchain_token_price" IS
  '[Hypertable] Canonical transform of public.offchain_token_price (VEC-484): API-polled token prices from off-chain providers (CoinGecko), used for cross-validation of on-chain oracle prices. No chain_id (off-chain, required-key exempt); source_id widened to bigint from the raw smallint; raw "timestamp" renamed to snapshot_time. Partition: snapshot_time.';
COMMENT ON COLUMN transformed."offchain_token_price".token_id IS 'FK->token.id. Part of PK.';
COMMENT ON COLUMN transformed."offchain_token_price".source_id IS 'FK->offchain_price_source.id. Part of PK. Widened to bigint from the raw smallint (known raw inconsistency).';
COMMENT ON COLUMN transformed."offchain_token_price".snapshot_time IS 'Partition. API observation time, renamed from raw "timestamp". Part of PK.';
COMMENT ON COLUMN transformed."offchain_token_price".price_usd IS 'USD price from off-chain provider. Divergence >5% from onchain_token_price signals a potentially stale oracle.';
COMMENT ON COLUMN transformed."offchain_token_price".market_cap_usd IS 'Market capitalization in USD from the provider; mirrored 1:1 from public.offchain_token_price (raw column undocumented - verify scale before computing).';
COMMENT ON COLUMN transformed."offchain_token_price".volume_usd IS '24h trading volume. Currently always null; not populated by the CoinGecko integration.';
COMMENT ON COLUMN transformed."offchain_token_price".processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by snapshot_time DESC, processing_version DESC for the latest snapshot.';
COMMENT ON COLUMN transformed."offchain_token_price".build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row (it identifies a git build, not a commit order). Not used by the transform refresh, which is queue-driven; carried through for lineage.';

CREATE TABLE IF NOT EXISTS transformed."_pending_offchain_token_price" AS SELECT "token_id", "source_id", "processing_version", "timestamp" FROM public."offchain_token_price" WHERE false;
ALTER TABLE transformed."_pending_offchain_token_price" ADD COLUMN IF NOT EXISTS enqueued_at timestamptz NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS "_pending_offchain_token_price_enqueued_at" ON transformed."_pending_offchain_token_price" (enqueued_at);
COMMENT ON TABLE transformed."_pending_offchain_token_price" IS '[Operational] Change queue for transformed.offchain_token_price. The AFTER INSERT trigger on public.offchain_token_price appends the raw PK here; transformed._run_offchain_token_price() drains it. A backlog with an old enqueued_at means the transform has stalled.';
CREATE OR REPLACE FUNCTION transformed."_enqueue_offchain_token_price"() RETURNS trigger
  LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, transformed, public AS $tg$
BEGIN
  INSERT INTO transformed."_pending_offchain_token_price"("token_id", "source_id", "processing_version", "timestamp") VALUES (NEW."token_id", NEW."source_id", NEW."processing_version", NEW."timestamp");
  RETURN NEW;
END $tg$;
DROP TRIGGER IF EXISTS "_transform_enqueue" ON public."offchain_token_price";
CREATE TRIGGER "_transform_enqueue" AFTER INSERT ON public."offchain_token_price" FOR EACH ROW EXECUTE FUNCTION transformed."_enqueue_offchain_token_price"();
CREATE OR REPLACE FUNCTION transformed._run_offchain_token_price() RETURNS TABLE(consumed bigint, upserted bigint) AS $fn$
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('transformed._run_offchain_token_price', 0));
  RETURN QUERY
  WITH batch AS (
    DELETE FROM transformed."_pending_offchain_token_price"
    WHERE ctid IN (SELECT ctid FROM transformed."_pending_offchain_token_price" LIMIT 10000)
    RETURNING "token_id", "source_id", "processing_version", "timestamp"
  ), ins AS (
  INSERT INTO transformed."offchain_token_price" AS t ("token_id", "source_id", "snapshot_time", "price_usd", "market_cap_usd", "volume_usd", "processing_version", "build_id")
SELECT "token_id",
       CAST("source_id" AS bigint) AS "source_id",
       "timestamp" AS "snapshot_time",
       "price_usd",
       "market_cap_usd",
       "volume_usd",
       "processing_version",
       "build_id"
  FROM public."offchain_token_price"
  WHERE ("token_id", "source_id", "processing_version", "timestamp") IN (SELECT DISTINCT "token_id", "source_id", "processing_version", "timestamp" FROM batch)
    AND "timestamp" >= (SELECT min("timestamp") FROM batch)
    AND "timestamp" <= (SELECT max("timestamp") FROM batch)
  ON CONFLICT ("token_id", "source_id", "processing_version", "snapshot_time") DO UPDATE SET "price_usd"=EXCLUDED."price_usd", "market_cap_usd"=EXCLUDED."market_cap_usd", "volume_usd"=EXCLUDED."volume_usd", "build_id"=EXCLUDED."build_id"
    WHERE (t."price_usd", t."market_cap_usd", t."volume_usd", t."build_id") IS DISTINCT FROM (EXCLUDED."price_usd", EXCLUDED."market_cap_usd", EXCLUDED."volume_usd", EXCLUDED."build_id")
  RETURNING 1)
  SELECT (SELECT count(*) FROM batch), (SELECT count(*) FROM ins);
END $fn$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION transformed._bootstrap_offchain_token_price(_from timestamptz, _to timestamptz) RETURNS bigint AS $fn$
DECLARE n bigint;
BEGIN
  WITH ins AS (
  INSERT INTO transformed."offchain_token_price" AS t ("token_id", "source_id", "snapshot_time", "price_usd", "market_cap_usd", "volume_usd", "processing_version", "build_id")
SELECT "token_id",
       CAST("source_id" AS bigint) AS "source_id",
       "timestamp" AS "snapshot_time",
       "price_usd",
       "market_cap_usd",
       "volume_usd",
       "processing_version",
       "build_id"
  FROM public."offchain_token_price"
  WHERE "timestamp" >= _from AND "timestamp" < _to
  ON CONFLICT ("token_id", "source_id", "processing_version", "snapshot_time") DO UPDATE SET "price_usd"=EXCLUDED."price_usd", "market_cap_usd"=EXCLUDED."market_cap_usd", "volume_usd"=EXCLUDED."volume_usd", "build_id"=EXCLUDED."build_id"
    WHERE (t."price_usd", t."market_cap_usd", t."volume_usd", t."build_id") IS DISTINCT FROM (EXCLUDED."price_usd", EXCLUDED."market_cap_usd", EXCLUDED."volume_usd", EXCLUDED."build_id")
  RETURNING 1)
  SELECT count(*) INTO n FROM ins;
  RETURN n;
END $fn$ LANGUAGE plpgsql;
INSERT INTO transformed._sources(source) VALUES ('offchain_token_price') ON CONFLICT DO NOTHING;

-- Aggregate queue depth / age across all sources; drives the stalled-transform alert
-- (a source whose oldest_enqueued_at is far in the past is not draining).
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
SELECT 'offchain_token_price'::text AS source, count(*) AS pending, min(enqueued_at) AS oldest_enqueued_at FROM transformed."_pending_offchain_token_price";
COMMENT ON VIEW transformed._queue_status IS '[Operational] Per-source pending-row count and oldest enqueue time across the transform change queues. oldest_enqueued_at lagging wall-clock = a stalled transform.';

-- Raw-vs-transformed parity backstop (checkpointed incremental).
-- A ledger holds a verified (raw, transformed, pending) count per source per UTC
-- DAY-bucket -- a stable time boundary, so raw and transformed (whose chunk
-- intervals differ, e.g. on maple) are compared over the same window, never a
-- chunk boundary. A companion table (_parity_chunk_activity) holds each raw chunk's
-- pg_stat_all_tables insert/delete baseline, so the worker does not full-count every
-- table each tick: _parity_refresh re-counts only the day-buckets overlapping the
-- head chunk plus any local chunk whose insert/delete stats moved (a signal the
-- enqueue trigger does NOT own, so a broken trigger still surfaces) or reset. Tiered
-- chunks' day-buckets are verified once at bootstrap (_parity_verify_all, tiered
-- reads on) and frozen. drift = sum(raw - transformed - pending) over the ledger; 0
-- in a consistent snapshot, nonzero = a row that reached neither the transformed
-- table nor its queue.
CREATE TABLE IF NOT EXISTS transformed._parity_ledger(
  source        text        NOT NULL,
  bucket_start  timestamptz NOT NULL,
  bucket_end    timestamptz NOT NULL,
  raw_count     bigint      NOT NULL,
  xform_count   bigint      NOT NULL,
  pending_count bigint      NOT NULL,
  frozen        boolean     NOT NULL DEFAULT false,
  verified_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (source, bucket_start)
);
COMMENT ON TABLE transformed._parity_ledger IS '[Operational] Checkpointed parity: verified raw/transformed/pending counts per source per UTC day-bucket. Tiered day-buckets are frozen (verified once at bootstrap).';
COMMENT ON COLUMN transformed._parity_ledger.source IS 'PK. Transformed table name this bucket belongs to.';
COMMENT ON COLUMN transformed._parity_ledger.bucket_start IS 'PK. Start of the UTC day this bucket covers (date_trunc(''day'')). A time boundary, not a chunk boundary, so raw and transformed -- which have different chunk intervals -- are compared over the same window.';
COMMENT ON COLUMN transformed._parity_ledger.frozen IS 'Tiered day-bucket verified once at bootstrap; never re-counted by the per-tick refresh (would re-read S3).';

-- Per-chunk insert/delete baseline (n_tup_ins + n_tup_del at last verify). The
-- per-tick refresh recounts a day-bucket only when a raw chunk overlapping it has an
-- activity value different from the one stored here (moved or reset). Kept per chunk,
-- not per day, so a reset in one chunk is never masked by another chunk in the same
-- day.
CREATE TABLE IF NOT EXISTS transformed._parity_chunk_activity(
  source     text   NOT NULL,
  chunk_name text   NOT NULL,
  activity   bigint NOT NULL,
  PRIMARY KEY (source, chunk_name)
);
COMMENT ON TABLE transformed._parity_chunk_activity IS '[Operational] Per raw-chunk n_tup_ins+n_tup_del baseline that decides when _parity_refresh re-counts the day-buckets a chunk overlaps.';

-- _parity_count: count rows of _tbl whose time column _col is in [_lo, _hi).
CREATE OR REPLACE FUNCTION transformed._parity_count(_tbl text, _col text, _lo timestamptz, _hi timestamptz) RETURNS bigint AS $fn$
DECLARE n bigint;
BEGIN
  EXECUTE format('SELECT count(*) FROM %s WHERE %I >= $1 AND %I < $2', _tbl, _col, _col) INTO n USING _lo, _hi;
  RETURN n;
END $fn$ LANGUAGE plpgsql;

-- _parity_refresh: steady-state incremental refresh for one source. Iterates only
-- LOCAL chunks (never references the tiered/OSM catalog, so it is safe where OSM is
-- absent, e.g. the OSS test image). Re-counts the day-buckets overlapping the head
-- chunk plus any local chunk whose activity moved, reset, or is new; skips days
-- already frozen (tiered). Compares on day boundaries, not chunk boundaries.
CREATE OR REPLACE FUNCTION transformed._parity_refresh(_source text) RETURNS void AS $fn$
DECLARE
  raw_col text; xf_col text; head_chunk text; day_ts timestamptz; prev_activity bigint;
  raw_tbl  text := format('public.%I', _source);
  xf_tbl   text := format('transformed.%I', _source);
  pend_tbl text := format('transformed.%I', '_pending_' || _source);
  c record;
BEGIN
  SELECT column_name INTO raw_col FROM timescaledb_information.dimensions
    WHERE hypertable_schema='public' AND hypertable_name=_source;
  SELECT column_name INTO xf_col FROM timescaledb_information.dimensions
    WHERE hypertable_schema='transformed' AND hypertable_name=_source;
  IF raw_col IS NULL OR xf_col IS NULL THEN
    RAISE EXCEPTION 'parity_refresh: no time dimension for %', _source;
  END IF;

  SELECT chunk_name INTO head_chunk FROM timescaledb_information.chunks
    WHERE hypertable_schema='public' AND hypertable_name=_source
    ORDER BY range_start DESC LIMIT 1;

  -- One pass over local chunks. Read each chunk's activity ONCE and use that same
  -- value both to decide whether to recount and to advance the baseline, so a chunk
  -- can never be marked clean against a newer baseline than its recount used (a
  -- second pg_stat read could differ, since pg_stat_all_tables is not transactional).
  FOR c IN
    SELECT ch.chunk_name AS name, ch.range_start AS rs, ch.range_end AS re,
           COALESCE(st.n_tup_ins,0) + COALESCE(st.n_tup_del,0) AS activity
    FROM timescaledb_information.chunks ch
    LEFT JOIN pg_stat_all_tables st
      ON st.schemaname = ch.chunk_schema AND st.relname = ch.chunk_name
    WHERE ch.hypertable_schema='public' AND ch.hypertable_name=_source
  LOOP
    SELECT b.activity INTO prev_activity FROM transformed._parity_chunk_activity b
      WHERE b.source=_source AND b.chunk_name=c.name;

    -- Recount the head chunk always, plus any chunk that is new (no baseline) or whose
    -- activity moved or reset. Recount each non-frozen day the chunk overlaps: a day is
    -- a stable time boundary, so raw and transformed (different chunk intervals) compare
    -- over the same window.
    IF c.name IS NOT DISTINCT FROM head_chunk OR prev_activity IS DISTINCT FROM c.activity THEN
      FOR day_ts IN
        SELECT gs FROM generate_series(date_trunc('day', c.rs),
                                       date_trunc('day', c.re - interval '1 microsecond'),
                                       interval '1 day') gs
        WHERE NOT EXISTS (SELECT 1 FROM transformed._parity_ledger l
                           WHERE l.source=_source AND l.bucket_start=gs AND l.frozen)
      LOOP
        INSERT INTO transformed._parity_ledger
          (source, bucket_start, bucket_end, raw_count, xform_count, pending_count, frozen, verified_at)
        VALUES (_source, day_ts, day_ts + interval '1 day',
                transformed._parity_count(raw_tbl,  raw_col, day_ts, day_ts + interval '1 day'),
                transformed._parity_count(xf_tbl,   xf_col,  day_ts, day_ts + interval '1 day'),
                transformed._parity_count(pend_tbl, raw_col, day_ts, day_ts + interval '1 day'),
                false, now())
        ON CONFLICT (source, bucket_start) DO UPDATE SET
          bucket_end=EXCLUDED.bucket_end, raw_count=EXCLUDED.raw_count,
          xform_count=EXCLUDED.xform_count, pending_count=EXCLUDED.pending_count,
          frozen=false, verified_at=now();
      END LOOP;
    END IF;

    -- Advance the baseline to the value just observed (the same read the decision used).
    INSERT INTO transformed._parity_chunk_activity (source, chunk_name, activity)
    VALUES (_source, c.name, c.activity)
    ON CONFLICT (source, chunk_name) DO UPDATE SET activity=EXCLUDED.activity;
  END LOOP;
END $fn$ LANGUAGE plpgsql;

-- _parity_verify_all: full verify for one source, for bootstrap (run with tiered
-- reads ON). Re-counts every day-bucket spanned by local chunks (frozen=false) and
-- every day-bucket spanned by tiered chunks (frozen=true, so the per-tick refresh
-- never re-reads S3), and seeds the per-chunk activity baseline. The tiered/OSM
-- catalog is referenced only through a guarded dynamic query, so the tiered pass is
-- a no-op where OSM is absent.
CREATE OR REPLACE FUNCTION transformed._parity_verify_all(_source text) RETURNS void AS $fn$
DECLARE
  raw_col text; xf_col text; day_ts timestamptz;
  raw_tbl  text := format('public.%I', _source);
  xf_tbl   text := format('transformed.%I', _source);
  pend_tbl text := format('transformed.%I', '_pending_' || _source);
BEGIN
  SELECT column_name INTO raw_col FROM timescaledb_information.dimensions
    WHERE hypertable_schema='public' AND hypertable_name=_source;
  SELECT column_name INTO xf_col FROM timescaledb_information.dimensions
    WHERE hypertable_schema='transformed' AND hypertable_name=_source;
  IF raw_col IS NULL OR xf_col IS NULL THEN
    RAISE EXCEPTION 'parity_verify_all: no time dimension for %', _source;
  END IF;

  -- Local day-buckets (frozen=false).
  FOR day_ts IN
    SELECT DISTINCT gs
    FROM timescaledb_information.chunks ch,
         LATERAL generate_series(date_trunc('day', ch.range_start),
                                 date_trunc('day', ch.range_end - interval '1 microsecond'),
                                 interval '1 day') gs
    WHERE ch.hypertable_schema='public' AND ch.hypertable_name=_source
    ORDER BY gs
  LOOP
    INSERT INTO transformed._parity_ledger
      (source, bucket_start, bucket_end, raw_count, xform_count, pending_count, frozen, verified_at)
    VALUES (_source, day_ts, day_ts + interval '1 day',
            transformed._parity_count(raw_tbl,  raw_col, day_ts, day_ts + interval '1 day'),
            transformed._parity_count(xf_tbl,   xf_col,  day_ts, day_ts + interval '1 day'),
            transformed._parity_count(pend_tbl, raw_col, day_ts, day_ts + interval '1 day'),
            false, now())
    ON CONFLICT (source, bucket_start) DO UPDATE SET
      bucket_end=EXCLUDED.bucket_end, raw_count=EXCLUDED.raw_count,
      xform_count=EXCLUDED.xform_count, pending_count=EXCLUDED.pending_count,
      frozen=false, verified_at=now();
  END LOOP;

  -- Seed the per-chunk activity baseline for all local chunks.
  INSERT INTO transformed._parity_chunk_activity (source, chunk_name, activity)
  SELECT _source, ch.chunk_name,
         COALESCE(st.n_tup_ins,0) + COALESCE(st.n_tup_del,0)
  FROM timescaledb_information.chunks ch
  LEFT JOIN pg_stat_all_tables st
    ON st.schemaname = ch.chunk_schema AND st.relname = ch.chunk_name
  WHERE ch.hypertable_schema='public' AND ch.hypertable_name=_source
  ON CONFLICT (source, chunk_name) DO UPDATE SET activity=EXCLUDED.activity;

  -- Tiered day-buckets (frozen=true), guarded so this is a no-op without OSM.
  IF to_regclass('timescaledb_osm.tiered_chunks') IS NOT NULL THEN
    FOR day_ts IN EXECUTE format(
      'SELECT DISTINCT gs FROM timescaledb_osm.tiered_chunks t,
         LATERAL generate_series(date_trunc(''day'', t.range_start),
                                 date_trunc(''day'', t.range_end - interval ''1 microsecond''),
                                 interval ''1 day'') gs
       WHERE t.hypertable_schema=%L AND t.hypertable_name=%L ORDER BY gs',
      'public', _source)
    LOOP
      INSERT INTO transformed._parity_ledger
        (source, bucket_start, bucket_end, raw_count, xform_count, pending_count, frozen, verified_at)
      VALUES (_source, day_ts, day_ts + interval '1 day',
              transformed._parity_count(raw_tbl,  raw_col, day_ts, day_ts + interval '1 day'),
              transformed._parity_count(xf_tbl,   xf_col,  day_ts, day_ts + interval '1 day'),
              transformed._parity_count(pend_tbl, raw_col, day_ts, day_ts + interval '1 day'),
              true, now())
      ON CONFLICT (source, bucket_start) DO UPDATE SET
        bucket_end=EXCLUDED.bucket_end, raw_count=EXCLUDED.raw_count,
        xform_count=EXCLUDED.xform_count, pending_count=EXCLUDED.pending_count,
        frozen=true, verified_at=now();
    END LOOP;
  END IF;
END $fn$ LANGUAGE plpgsql;

-- Per-source drift, summed from the ledger (cheap; no table scan).
CREATE OR REPLACE VIEW transformed._parity_status AS
SELECT source,
       sum(raw_count)     AS raw_rows,
       sum(xform_count)   AS transformed_rows,
       sum(pending_count) AS pending_rows,
       sum(raw_count - xform_count - pending_count) AS drift
FROM transformed._parity_ledger
GROUP BY source;
COMMENT ON VIEW transformed._parity_status IS '[Operational] Per-source raw vs transformed vs pending counts and drift (raw - transformed - pending), summed from transformed._parity_ledger. drift is 0 in a consistent snapshot after bootstrap; nonzero = a silent queue/trigger/bootstrap gap.';

-- Grants: the worker connects as stl_readwrite and drains the queues via _run_<t>()
-- (SECURITY INVOKER), so it needs schema usage, table read/write, and EXECUTE. The enqueue
-- triggers are SECURITY DEFINER, so raw writers need no grant on the queue tables to insert.
-- stl_readonly gets read access for downstream consumers of the transformed layer.
GRANT USAGE ON SCHEMA transformed TO stl_readonly, stl_readwrite;
GRANT SELECT ON ALL TABLES IN SCHEMA transformed TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA transformed TO stl_readwrite;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA transformed TO stl_readwrite;
ALTER DEFAULT PRIVILEGES IN SCHEMA transformed GRANT SELECT ON TABLES TO stl_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA transformed GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO stl_readwrite;
ALTER DEFAULT PRIVILEGES IN SCHEMA transformed GRANT EXECUTE ON FUNCTIONS TO stl_readwrite;

INSERT INTO migrations (filename)
VALUES ('20260706_140000_create_transformed_bucket1.sql')
ON CONFLICT (filename) DO NOTHING;
