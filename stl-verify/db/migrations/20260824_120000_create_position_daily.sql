-- position_daily (VEC-636): one row per (position, UTC date), the winning observation for that
-- position on that day. Only OBSERVED dates get a row -- there is no carry-forward, so a query for a
-- specific date may correctly return nothing. Grain, limits and measurements: table COMMENT and #752.


-- Bounds the wait for CREATE TRIGGER's SHARE ROW EXCLUSIVE on position_state, which every ingest
-- INSERT conflicts with. Never mark this file `migrate: no-transaction`: SET LOCAL would be inert.
SET LOCAL lock_timeout = '10s';

-- The backfill must see S3-tiered history: over local chunks only it computes newest-per-key across a
-- partial table. position_state partitions on block_timestamp, so a historical backfill writes chunks
-- already past the 1-year window and this reads tiered history from the first policy run.
SET LOCAL timescaledb.enable_tiered_reads = 'on';
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
    CONSTRAINT position_daily_pkey PRIMARY KEY (position_id, as_of_date),

    -- position_state's guards, because stl_readwrite can INSERT here directly. NaN needs both
    -- clauses: NaN >= 0 is TRUE in numeric ordering, and `< 'Infinity'` is what rejects it.
    CONSTRAINT position_daily_id_len_chk CHECK (octet_length(position_id) = 32),
    CONSTRAINT position_daily_qty_nonneg_chk CHECK (quantity >= 0 AND quantity <> 'NaN'::numeric AND quantity < 'Infinity'::numeric),
    -- Mirrors position_state exactly: a 20-byte address as 40 lowercase hex characters. The unanchored
    -- form also admits a decimal surrogate rendered as text ('12345' is all hex digits), and a cache that
    -- accepted a holder the spine rejects could never be reproduced from the spine.
    CONSTRAINT position_daily_holder_hex_chk CHECK (holder_id ~ '^[0-9a-f]{40}$'),
    -- instrument_key feeds the position_id hash and is copied into every derived cache. A sanity
    -- bound on an unbounded text column: no legitimate source form approaches it. position_state
    -- carries the same cap, justified there by the now-decommissioned bridge (ADR-0005).
    CONSTRAINT position_daily_instrument_key_len_chk CHECK (char_length(instrument_key) <= 2000),
    CONSTRAINT position_daily_coord_nonneg_chk CHECK (block_number >= 0 AND block_version >= 0 AND processing_version >= 0 AND build_id >= 0),
    CONSTRAINT position_daily_chain_pos_chk CHECK (chain_id IS NULL OR chain_id > 0),
    CONSTRAINT position_daily_protocol_pos_chk CHECK (protocol_id IS NULL OR protocol_id > 0),
    CONSTRAINT position_daily_ts_sane_chk CHECK (block_timestamp >= '2009-01-03 00:00:00+00'::timestamptz),
    -- Both writers derive as_of_date from block_timestamp; this pins them together, and on a
    -- hypertable a wrong value would also seat the row in the wrong chunk.
    CONSTRAINT position_daily_as_of_date_chk CHECK (as_of_date = (block_timestamp AT TIME ZONE 'utc')::date)
);

-- Hypertable on as_of_date, converted while the table is still empty. 7-day chunks rather than
-- position_state's 1-day: chunk count drives planning and per-position fan-out. No default index --
-- chunk exclusion on as_of_date does that job. Measurements in #752.
SELECT create_hypertable('position_daily', 'as_of_date', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE, create_default_indexes => FALSE);

-- Tier cold chunks to S3 after 1 year. Only the two absent-capability codes are tolerated;
-- everything else is fatal, because a swallowed error would ship a 39 GB table with no tiering and
-- the production migrator installs no notice handler to surface it.
DO $tier$
BEGIN
    PERFORM add_tiering_policy('position_daily', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function OR feature_not_supported THEN
    RAISE NOTICE 'add_tiering_policy unavailable (%), skipping tiering for position_daily', SQLERRM;
END;
$tier$;

-- NO compression policy, deliberately: this table's trigger is ON CONFLICT DO UPDATE, which rewrites
-- stored rows, and a bulk upsert exceeds max_tuples_decompressed_per_dml_transaction where
-- position_state's DO NOTHING costs nothing. VEC-566 carries the exception. Numbers in the COMMENT.

COMMENT ON TABLE position_daily IS '[Hypertable] Partition key: as_of_date, 7-day chunks. One row per (position, UTC date): the winning observation for that position on that day (VEC-636). Only OBSERVED dates get a row -- no carry-forward, so a query for one date may correctly return nothing. NO COMPRESSION POLICY, against the default rule: the trigger upserts in place and a bulk reprocess exceeds max_tuples_decompressed_per_dml_transaction, where position_state''s DO NOTHING is free; VEC-566 carries the exception. S3 tiering at 1 year IS set. Re-running the REBUILD region in 20260824_120000 is a FORWARD-ONLY merge: a row ahead of history and an orphan row do not converge, and TRUNCATE is owner-only. Point-in-time questions are answered from position_state.';
COMMENT ON COLUMN position_daily.position_id IS 'PK. The bytea(32) native position identity from position_id() (VEC-400).';
COMMENT ON COLUMN position_daily.as_of_date IS 'PK, Partition. The hypertable partition column. UTC date of the winning observation''s block_timestamp. Business today is UTC, never CURRENT_DATE.';
COMMENT ON COLUMN position_daily.chain_id IS 'Derived (copy of position_state.chain_id). Native chain id; nullable per the position_id convention.';
COMMENT ON COLUMN position_daily.protocol_id IS 'Derived (copy of position_state.protocol_id). Native protocol id; nullable per convention.';
COMMENT ON COLUMN position_daily.instrument_key IS 'Derived (copy of position_state.instrument_key). The instrument''s native, globally-unique id.';
COMMENT ON COLUMN position_daily.holder_id IS 'Derived (copy of position_state.holder_id). Native on-chain holder, lowercase hex, no 0x.';
COMMENT ON COLUMN position_daily.quantity IS 'Derived (copy of position_state.quantity). Holder balance in the instrument''s native units on that date; a zero is a real observation of an emptied position, not a gap. Scale is source-defined and NOT normalized across protocols, exactly as on position_state.quantity, so a consumer must not SUM across heterogeneous instruments without normalizing first.';
COMMENT ON COLUMN position_daily.block_number IS 'Derived. Block of the winning observation; part of the newer-wins comparison.';
COMMENT ON COLUMN position_daily.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN position_daily.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); part of the newer-wins comparison.';
COMMENT ON COLUMN position_daily.block_timestamp IS 'Derived. On-chain time of the winning observation; the last leg of the newer-wins comparison, so the pick is total when two rows share the other four key columns.';
COMMENT ON COLUMN position_daily.projection IS 'Audit + ownership. Which projection view wrote the winning observation.';
COMMENT ON COLUMN position_daily.build_id IS 'Audit. Which build wrote the winning observation (build_registry.id; 0 = pre-tracking).';

-- Outside the strict append-only rule (AGENTS.md here): a derived cache of the newest-row query per
-- (position, date), holding UPDATE and taking ON CONFLICT DO UPDATE. position_state stays untouched.
GRANT SELECT ON position_daily TO stl_readonly;
GRANT SELECT, INSERT, UPDATE ON position_daily TO stl_readwrite;
REVOKE DELETE ON position_daily FROM stl_readwrite;

-- Holder access path: the PK serves position_id, not the holder filter the enriched views use.
-- as_of_date trails so a holder's series is ordered by the index too. Measurements in #752.
CREATE INDEX IF NOT EXISTS position_daily_holder_idx ON position_daily (holder_id, as_of_date);

CREATE OR REPLACE FUNCTION upsert_position_daily() RETURNS trigger
    LANGUAGE plpgsql
    -- Explicit, not FROM CURRENT: FROM CURRENT bakes in whatever the applying session had, so the same
    -- migration yields a differently-pinned function under the per-test-schema harness than in prod.
    -- Nothing here needs a user schema, so pinning costs nothing.
    SET search_path = pg_catalog, public
AS $fn$
BEGIN
    INSERT INTO public.position_daily AS cur
        (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
         block_number, block_version, processing_version, block_timestamp, projection, build_id)
    -- One upsert per STATEMENT over the transition table, ordered by this table's PK: a total order
    -- the rebuild cannot cross, where a row trigger would fire in the writer's own insertion order.
    -- NEW is unavailable in a statement trigger. Deadlock measurements in #752.
    SELECT DISTINCT ON (n.position_id, (n.block_timestamp AT TIME ZONE 'utc')::date)
           n.position_id, (n.block_timestamp AT TIME ZONE 'utc')::date, n.chain_id, n.protocol_id,
           n.instrument_key, n.holder_id, n.quantity, n.block_number, n.block_version,
           n.processing_version, n.block_timestamp, n.projection, n.build_id
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
        build_id           = EXCLUDED.build_id
    WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version, EXCLUDED.block_timestamp)
        > (cur.block_number, cur.block_version, cur.processing_version, cur.block_timestamp);
    RETURN NULL;
END;
$fn$;

COMMENT ON FUNCTION upsert_position_daily() IS '[Operational] Keeps position_daily at the winning observation per (position, UTC date) (VEC-409), by (block_number, block_version, processing_version, block_timestamp). AFTER INSERT on position_state; an out-of-order insert cannot regress a day''s row.';

CREATE TRIGGER trigger_upsert_position_daily
    AFTER INSERT ON position_state
    REFERENCING NEW TABLE AS newrows
    FOR EACH STATEMENT
EXECUTE FUNCTION upsert_position_daily();

-- Backfill / rebuild, carrying the same newer-wins guard as the trigger. FORWARD-ONLY: it raises a
-- cached row and never lowers or removes one, so a row ahead of history (repairable in-role by
-- UPDATE) and an orphan row (owner-only) do not converge. Requires a quiet window; see #752.

-- REBUILD-BEGIN position_daily

-- All three SETs live INSIDE the markers: the COMMENT points operators here, the test extracts
-- exactly this region, and SET LOCAL dies with its transaction. The guard catches the
-- statement-at-a-time path, where every SET LOCAL degrades to a warning the driver discards.
SET LOCAL lock_timeout = '10s';
SET LOCAL timescaledb.enable_tiered_reads = 'on';
SET LOCAL search_path = public;

DO $rebuild_guard$
BEGIN
    IF current_setting('lock_timeout') <> '10s' THEN
        RAISE EXCEPTION 'run the REBUILD region inside ONE transaction (BEGIN; ... COMMIT;): outside a transaction block every SET LOCAL is an inert warning, so this would run with no tiered-read guarantee and no lock bound';
    END IF;
END
$rebuild_guard$;

INSERT INTO public.position_daily
    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
     block_number, block_version, processing_version, block_timestamp, projection, build_id)
SELECT DISTINCT ON (p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date)
       p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date, p.chain_id, p.protocol_id,
       p.instrument_key, p.holder_id, p.quantity, p.block_number, p.block_version,
       p.processing_version, p.block_timestamp, p.projection, p.build_id
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
    build_id           = EXCLUDED.build_id
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version, EXCLUDED.block_timestamp)
    > (position_daily.block_number, position_daily.block_version, position_daily.processing_version, position_daily.block_timestamp);
-- REBUILD-END position_daily


ANALYZE position_daily;

INSERT INTO migrations (filename) VALUES ('20260824_120000_create_position_daily.sql') ON CONFLICT (filename) DO NOTHING;
