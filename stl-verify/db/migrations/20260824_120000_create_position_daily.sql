-- position_daily (VEC-636): one row per (position, UTC date), the winning observation for that position
-- on that day. Only OBSERVED dates get a row -- there is no carry-forward, so a query for a specific
-- date may correctly return nothing.

-- Bounds the wait for CREATE TRIGGER's SHARE ROW EXCLUSIVE on position_state, which every ingest
-- INSERT conflicts with. Never mark this file `migrate: no-transaction`: SET LOCAL would be inert.
SET LOCAL lock_timeout = '10s';

CREATE TABLE IF NOT EXISTS position_daily (
    position_id        bytea       NOT NULL,
    as_of_date         date        NOT NULL,
    chain_id           integer,
    protocol_id        bigint,
    instrument_key     text        NOT NULL,
    holder_id          text        NOT NULL,
    quantity           numeric     NOT NULL,
    block_number       bigint      NOT NULL,
    block_version      integer     NOT NULL,
    processing_version integer     NOT NULL,
    block_timestamp    timestamptz NOT NULL,
    projection         text        NOT NULL,
    build_id           integer     NOT NULL,
    run_id             bigint,
    deal_type          text,
    CONSTRAINT position_daily_pkey PRIMARY KEY (position_id, as_of_date),
    -- The one constraint that is not a copy of a position_state guard: it pins both writers' date
    -- derivation together, and on a hypertable a wrong value would also seat the row in the wrong chunk.
    CONSTRAINT position_daily_as_of_date_chk CHECK (as_of_date = (block_timestamp AT TIME ZONE 'utc')::date)
);

-- Hypertable on as_of_date, converted while the table is still empty. 7-day chunks rather than
-- position_state's 1-day: chunk count drives planning and per-position fan-out. No default index --
-- chunk exclusion on as_of_date does that job.
SELECT create_hypertable('position_daily', 'as_of_date', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE, create_default_indexes => FALSE);

-- Tier cold chunks to S3 after 1 year. Only the two absent-capability codes are tolerated;
-- everything else is fatal, because a swallowed error would ship an untiered table and the production
-- migrator installs no notice handler to surface it.
DO $tier$
BEGIN
    PERFORM add_tiering_policy('position_daily', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function OR feature_not_supported THEN
    RAISE NOTICE 'add_tiering_policy unavailable (%), skipping tiering for position_daily', SQLERRM;
END;
$tier$;

-- Compression at 30 days, past the window the trigger keeps rewriting: a bulk upsert into a compressed
-- chunk exceeds max_tuples_decompressed_per_dml_transaction (measured: fails at 100,001 on 150k rows),
-- so both writers lift that limit for their own statement rather than the table forgoing compression.
ALTER TABLE position_daily SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'position_id',
    timescaledb.compress_orderby = 'as_of_date DESC'
);
SELECT add_compression_policy('position_daily', INTERVAL '30 days', if_not_exists => TRUE);

COMMENT ON TABLE position_daily IS '[Hypertable] Partition key: as_of_date, 7-day chunks. One row per (position, UTC date): the winning observation for that position on that day (VEC-636). Only OBSERVED dates get a row -- no carry-forward, so a query for one date may correctly return nothing. Rebuildable with CALL rebuild_position_daily(), a FORWARD-ONLY merge: it raises a row and never lowers or removes one. Compressed after 30 days; both writers lift max_tuples_decompressed_per_dml_transaction so an upsert into a compressed chunk is not capped. Point-in-time questions are answered from position_state.';
COMMENT ON COLUMN position_daily.position_id IS 'Roles: PK. The bytea(32) native position identity from position_id() (VEC-400).';
COMMENT ON COLUMN position_daily.as_of_date IS 'Roles: PK, Partition. UTC date of the winning observation''s block_timestamp, pinned to it by a CHECK.';
COMMENT ON COLUMN position_daily.chain_id IS 'Roles: Derived (copy of position_state.chain_id). NULL is a materializer convention for an off-chain source, not missing data.';
COMMENT ON COLUMN position_daily.protocol_id IS 'Roles: Derived (copy of position_state.protocol_id). NULL only for an off-chain source, which has no protocol row.';
COMMENT ON COLUMN position_daily.instrument_key IS 'Roles: Derived (copy of position_state.instrument_key). The instrument''s native, globally-unique id.';
COMMENT ON COLUMN position_daily.holder_id IS 'Roles: Derived (copy of position_state.holder_id). Native on-chain holder, lowercase hex, no 0x.';
COMMENT ON COLUMN position_daily.quantity IS 'Roles: Derived (copy of position_state.quantity). Native units on that date; a zero is a real closing observation, not an absence.';
COMMENT ON COLUMN position_daily.block_number IS 'Roles: Derived. Block of the winning observation; the leading leg of the newer-wins comparison.';
COMMENT ON COLUMN position_daily.block_version IS 'Roles: Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN position_daily.processing_version IS 'Roles: Derived. Correction version of that row (0 = original, N = Nth reprocess); part of the newer-wins comparison.';
COMMENT ON COLUMN position_daily.block_timestamp IS 'Roles: Derived. On-chain time of the winning observation; the last leg of the newer-wins comparison, so the pick is total.';
COMMENT ON COLUMN position_daily.projection IS 'Roles: Audit. Which projection view wrote the winning observation.';
COMMENT ON COLUMN position_daily.deal_type IS 'Roles: Derived (copy of position_state.deal_type). The deal type of that day''s winning observation.';
COMMENT ON COLUMN position_daily.build_id IS 'Roles: Audit. Which build wrote the winning observation (build_registry.id; 0 = pre-tracking).';
COMMENT ON COLUMN position_daily.run_id IS 'Roles: Audit (copy of position_state.run_id). Which writer run appended the winning observation (writer_run.id; NULL means it predates run tracking).';

-- Trigger-only cache, like position_current and allocation_position_current: the app role reads and the
-- SECURITY DEFINER maintainer writes, so no caller needs a write grant and the cache cannot fork from
-- history. ALTER DEFAULT PRIVILEGES (20260122_140100) grants full DML, so the REVOKE is what closes it.
GRANT SELECT ON position_daily TO stl_readonly;
GRANT SELECT ON position_daily TO stl_readwrite;
REVOKE INSERT, UPDATE, DELETE ON position_daily FROM stl_readwrite;

-- SECURITY DEFINER so the upsert runs as the owner: the role appending to position_state holds no write
-- grant here. search_path is then mandatory, so a caller's path cannot bind these names to its own objects.
CREATE OR REPLACE FUNCTION upsert_position_daily() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path = pg_catalog, public
    -- A reprocess re-emits old observations, so one statement can rewrite rows in a compressed chunk.
    SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0
AS $fn$
BEGIN
    INSERT INTO public.position_daily AS cur
        (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
         block_number, block_version, processing_version, block_timestamp, projection, build_id,
         run_id,
         deal_type)
    -- One upsert per STATEMENT over the transition table, ordered by this table's PK: a total order the
    -- rebuild cannot cross, where a row trigger would fire in the writer's own insertion order.
    SELECT DISTINCT ON (n.position_id, (n.block_timestamp AT TIME ZONE 'utc')::date)
           n.position_id, (n.block_timestamp AT TIME ZONE 'utc')::date, n.chain_id, n.protocol_id,
           n.instrument_key, n.holder_id, n.quantity, n.block_number, n.block_version,
           n.processing_version, n.block_timestamp, n.projection, n.build_id, n.run_id, n.deal_type
    FROM newrows n
    ORDER BY n.position_id, (n.block_timestamp AT TIME ZONE 'utc')::date,
             n.block_number DESC, n.block_version DESC, n.processing_version DESC, n.block_timestamp DESC
    ON CONFLICT (position_id, as_of_date) DO UPDATE SET
        chain_id           = EXCLUDED.chain_id,
        protocol_id        = EXCLUDED.protocol_id,
        instrument_key     = EXCLUDED.instrument_key,
        holder_id          = EXCLUDED.holder_id,
        quantity           = EXCLUDED.quantity,
        block_number       = EXCLUDED.block_number,
        block_version      = EXCLUDED.block_version,
        processing_version = EXCLUDED.processing_version,
        block_timestamp    = EXCLUDED.block_timestamp,
        projection         = EXCLUDED.projection,
        build_id           = EXCLUDED.build_id,
        run_id             = EXCLUDED.run_id,
        deal_type          = EXCLUDED.deal_type
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version, EXCLUDED.block_timestamp)
        > (cur.block_number, cur.block_version, cur.processing_version, cur.block_timestamp);
    RETURN NULL;
END;
$fn$;

COMMENT ON FUNCTION upsert_position_daily() IS '[Operational] Keeps position_daily at the winning observation per (position, UTC date) (VEC-636). AFTER INSERT FOR EACH STATEMENT on position_state, one upsert over the transition table ordered by this table''s PK, guarded by (block_number, block_version, processing_version, block_timestamp). SECURITY DEFINER: the appending role holds no write grant on the cache.';

-- The rebuild an operator re-runs, as a procedure so its settings cannot be forgotten or stepped over
-- and there is no second call site to keep in step: enable_tiered_reads because newest-per-key over
-- local chunks alone reads a PARTIAL table, work_mem because the DISTINCT ON sorts the whole spine.
CREATE OR REPLACE PROCEDURE rebuild_position_daily()
    LANGUAGE sql
    SET search_path = pg_catalog, public
    SET timescaledb.enable_tiered_reads = 'on'
    SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0
    SET lock_timeout = '10s'
    SET work_mem = '64MB'
AS $proc$
    INSERT INTO public.position_daily
        (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
         block_number, block_version, processing_version, block_timestamp, projection, build_id,
         run_id,
         deal_type)
    SELECT DISTINCT ON (p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date)
           p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date, p.chain_id, p.protocol_id,
           p.instrument_key, p.holder_id, p.quantity, p.block_number, p.block_version,
           p.processing_version, p.block_timestamp, p.projection, p.build_id, p.run_id, p.deal_type
    FROM public.position_state p
    ORDER BY p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date,
             p.block_number DESC, p.block_version DESC, p.processing_version DESC, p.block_timestamp DESC
    ON CONFLICT (position_id, as_of_date) DO UPDATE SET
        chain_id           = EXCLUDED.chain_id,
        protocol_id        = EXCLUDED.protocol_id,
        instrument_key     = EXCLUDED.instrument_key,
        holder_id          = EXCLUDED.holder_id,
        quantity           = EXCLUDED.quantity,
        block_number       = EXCLUDED.block_number,
        block_version      = EXCLUDED.block_version,
        processing_version = EXCLUDED.processing_version,
        block_timestamp    = EXCLUDED.block_timestamp,
        projection         = EXCLUDED.projection,
        build_id           = EXCLUDED.build_id,
        run_id             = EXCLUDED.run_id,
        deal_type          = EXCLUDED.deal_type
    -- Forward-only: raise a stale row, never lower one. No equal-coordinate arm is needed now that the
    -- cache has no write channel outside these two writers, which cannot disagree on one coordinate.
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version, EXCLUDED.block_timestamp)
        > (position_daily.block_number, position_daily.block_version, position_daily.processing_version, position_daily.block_timestamp);
$proc$;

COMMENT ON PROCEDURE rebuild_position_daily() IS '[Operational] Rebuilds position_daily from position_state (VEC-636): CALL rebuild_position_daily(). Forward-only, so it raises a stale row and never lowers or removes one; it cannot repair a row ahead of history or a row whose position has no history left. Pins enable_tiered_reads so newest-per-key is computed over the whole table, tiered chunks included. Requires a quiet window on position_state.';

-- Guarded like every other DDL statement here, so a re-run does not fail with "trigger already exists".
DROP TRIGGER IF EXISTS trigger_upsert_position_daily ON position_state;
CREATE TRIGGER trigger_upsert_position_daily
    AFTER INSERT ON position_state
    REFERENCING NEW TABLE AS newrows
    FOR EACH STATEMENT
EXECUTE FUNCTION upsert_position_daily();

CALL rebuild_position_daily();

-- Built AFTER the backfill: created first, every backfilled row pays a random btree insert with its own
-- WAL instead of one bulk build. Serves the holder filter the PK cannot; as_of_date trails so a
-- holder's series is ordered by the index too.
CREATE INDEX IF NOT EXISTS position_daily_holder_idx ON public.position_daily (holder_id, as_of_date);

ANALYZE public.position_daily;

INSERT INTO public.migrations (filename) VALUES ('20260824_120000_create_position_daily.sql') ON CONFLICT (filename) DO NOTHING;
