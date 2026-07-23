package main

// This file is the static (non-per-table) SQL of the bucket-1 migration, embedded
// verbatim from db/migrations/20260706_140000_create_transformed_bucket1.sql: the
// header (schema + _sources registry) and the tail (queue-status view, parity
// ledger machinery, grants, migration-tracking insert). These do not vary per
// table, so the generator emits them unchanged; the regen-diff test compares them
// (normalised) against the committed migration.

const header = `-- Transformation layer, bucket 1 (VEC-484): 13 governed tables canonicalised 1:1 from raw
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

-- Fail fast rather than convoy ingestion: a bounded lock_timeout makes the migration abort
-- and roll back if it cannot acquire a lock promptly, instead of blocking raw writers.
SET LOCAL lock_timeout = '10s';

CREATE SCHEMA IF NOT EXISTS transformed;

-- Registry of transformed tables the worker iterates (replaces the _watermark row set).
CREATE TABLE IF NOT EXISTS transformed._sources(source text PRIMARY KEY);
COMMENT ON TABLE transformed._sources IS
  '[Operational] One row per transformed table. The transform worker lists these and calls transformed._run_<source>() for each.';
COMMENT ON COLUMN transformed._sources.source IS 'PK. Transformed table name whose _run_<source>()/_bootstrap_<source>() functions and _pending_<source> queue exist.';


-- ===== morpho_market_state =====
`

const tail = `CREATE OR REPLACE VIEW transformed._queue_status AS
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

-- _parity_counts: raw, transformed and pending counts for one day-bucket, produced by
-- a SINGLE statement so all three share one snapshot. Counting them as three separate
-- statements let a drain commit between the transformed and pending reads (moving a row
-- out of pending and into transformed) and record a false nonzero drift; one statement
-- closes that window. raw and pending key on the raw time column, transformed on its own.
CREATE OR REPLACE FUNCTION transformed._parity_counts(
  _raw_tbl text, _raw_col text, _xf_tbl text, _xf_col text, _pend_tbl text,
  _lo timestamptz, _hi timestamptz,
  OUT raw_n bigint, OUT xform_n bigint, OUT pend_n bigint) AS $fn$
BEGIN
  EXECUTE format(
    'SELECT (SELECT count(*) FROM %s WHERE %I >= $1 AND %I < $2),
            (SELECT count(*) FROM %s WHERE %I >= $1 AND %I < $2),
            (SELECT count(*) FROM %s WHERE %I >= $1 AND %I < $2)',
    _raw_tbl,  _raw_col, _raw_col,
    _xf_tbl,   _xf_col,  _xf_col,
    _pend_tbl, _raw_col, _raw_col)
  INTO raw_n, xform_n, pend_n USING _lo, _hi;
END $fn$ LANGUAGE plpgsql;

-- _parity_refresh: steady-state incremental refresh for one source. Iterates only
-- LOCAL chunks (never references the tiered/OSM catalog, so it is safe where OSM is
-- absent, e.g. the OSS test image). Re-counts the day-buckets overlapping the head
-- chunk plus any local chunk whose activity moved, reset, or is new; skips days
-- already frozen (tiered). Compares on day boundaries, not chunk boundaries.
CREATE OR REPLACE FUNCTION transformed._parity_refresh(_source text) RETURNS void SET timezone TO 'UTC' AS $fn$
DECLARE
  raw_col text; xf_col text; head_chunk text; day_ts timestamptz; prev_activity bigint;
  raw_tbl  text := format('public.%I', _source);
  xf_tbl   text := format('transformed.%I', _source);
  pend_tbl text := format('transformed.%I', '_pending_' || _source);
  c record;
BEGIN
  SELECT column_name INTO raw_col FROM timescaledb_information.dimensions
    WHERE hypertable_schema='public' AND hypertable_name=_source AND dimension_number = 1;
  SELECT column_name INTO xf_col FROM timescaledb_information.dimensions
    WHERE hypertable_schema='transformed' AND hypertable_name=_source AND dimension_number = 1;
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
        SELECT _source, day_ts, day_ts + interval '1 day', pc.raw_n, pc.xform_n, pc.pend_n, false, now()
        FROM transformed._parity_counts(raw_tbl, raw_col, xf_tbl, xf_col, pend_tbl,
                                        day_ts, day_ts + interval '1 day') pc
        ON CONFLICT (source, bucket_start) DO UPDATE SET
          bucket_end=EXCLUDED.bucket_end, raw_count=EXCLUDED.raw_count,
          xform_count=EXCLUDED.xform_count, pending_count=EXCLUDED.pending_count,
          frozen=false, verified_at=now();
      END LOOP;
    END IF;

    -- Advance the baseline to the value just observed (the same read the decision used).
    -- The IS DISTINCT FROM guard makes an unchanged chunk a no-op write, so the steady
    -- state does not rewrite every baseline row each tick.
    INSERT INTO transformed._parity_chunk_activity (source, chunk_name, activity)
    VALUES (_source, c.name, c.activity)
    ON CONFLICT (source, chunk_name) DO UPDATE SET activity=EXCLUDED.activity
      WHERE transformed._parity_chunk_activity.activity IS DISTINCT FROM EXCLUDED.activity;
  END LOOP;
END $fn$ LANGUAGE plpgsql;

-- _parity_verify_all: full verify for one source, for bootstrap (run with tiered
-- reads ON). Re-counts every day-bucket spanned by local chunks (frozen=false) and
-- every day-bucket spanned by tiered chunks (frozen=true, so the per-tick refresh
-- never re-reads S3), and seeds the per-chunk activity baseline. The tiered/OSM
-- catalog is referenced only through a guarded dynamic query, so the tiered pass is
-- a no-op where OSM is absent.
CREATE OR REPLACE FUNCTION transformed._parity_verify_all(_source text) RETURNS void SET timezone TO 'UTC' AS $fn$
DECLARE
  raw_col text; xf_col text; day_ts timestamptz;
  raw_tbl  text := format('public.%I', _source);
  xf_tbl   text := format('transformed.%I', _source);
  pend_tbl text := format('transformed.%I', '_pending_' || _source);
BEGIN
  SELECT column_name INTO raw_col FROM timescaledb_information.dimensions
    WHERE hypertable_schema='public' AND hypertable_name=_source AND dimension_number = 1;
  SELECT column_name INTO xf_col FROM timescaledb_information.dimensions
    WHERE hypertable_schema='transformed' AND hypertable_name=_source AND dimension_number = 1;
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
    SELECT _source, day_ts, day_ts + interval '1 day', pc.raw_n, pc.xform_n, pc.pend_n, false, now()
    FROM transformed._parity_counts(raw_tbl, raw_col, xf_tbl, xf_col, pend_tbl,
                                    day_ts, day_ts + interval '1 day') pc
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
  ON CONFLICT (source, chunk_name) DO UPDATE SET activity=EXCLUDED.activity
    WHERE transformed._parity_chunk_activity.activity IS DISTINCT FROM EXCLUDED.activity;

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
      SELECT _source, day_ts, day_ts + interval '1 day', pc.raw_n, pc.xform_n, pc.pend_n, true, now()
      FROM transformed._parity_counts(raw_tbl, raw_col, xf_tbl, xf_col, pend_tbl,
                                      day_ts, day_ts + interval '1 day') pc
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
`
