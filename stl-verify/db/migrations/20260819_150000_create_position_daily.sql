-- position_daily (VEC-409): one row per (position, UTC date) — the winning observation for that
-- position on that day, with reorg and reprocess versions resolved. Every historical date is kept;
-- this is not a latest-only cache.
--
-- Mechanics follow the current-position tables in #733 (VEC-577) exactly: the history stays
-- append-only and untouched, a plain table holds the winning row per key, an AFTER INSERT trigger on
-- the history keeps it fresh, the trigger is created BEFORE the backfill so no row can commit in the
-- gap, and the table is rebuildable from history at any time (TRUNCATE and re-run the backfill at the
-- bottom of this file). The only difference from #733 is the key: (position_id, as_of_date) rather
-- than the logical key alone, because the point is a per-day series, not just current state.
--
-- Size, so nobody mistakes this for a small cache. #733's caches are a few thousand rows; this one
-- collapses only the superseded versions, so it is a large fraction of the history. Measured on a
-- fresh database with a reprocess every 5th day and a reorg every 7th: 205,000 history rows ->
-- 150,000 daily rows, 73%. What it buys is read cost: a 7-day window over all positions was 3,535
-- buffers here against 75,680 for the equivalent DISTINCT ON over position_state, a factor of 21.
--
-- No index on as_of_date, deliberately. A wide window touches a large fraction of the table, where a
-- seq scan wins: the same 7-day query measured 3,535 buffers by seq scan against 54,390 through an
-- (as_of_date, position_id) index, which random-fetches a third of the heap. One position's history is
-- served by the PK. Add a narrow-window index only when a consumer with a measured need appears.
--
-- Newer-wins compares the FULL observation key including block_timestamp. position_state's PK has five
-- columns because block_timestamp is the partition column, so two rows can legally share the first
-- four and differ in the fifth; comparing only the first four picked the OLDER row (reproduced against
-- the view this migration originally created: quantity 11 where the newest observation was 22).
--
-- as_of_date is derived in UTC, matching the repo's business-today rule (never CURRENT_DATE).

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
    CONSTRAINT position_daily_pkey PRIMARY KEY (position_id, as_of_date)
);

COMMENT ON TABLE position_daily IS '[Operational] One row per (position, UTC date): the winning observation for that position on that day, reorg and reprocess versions resolved (VEC-409). Every historical date is retained. Derived cache of the position_state history; rebuildable from it at any time (TRUNCATE and re-run the backfill in 20260819_150000). Consumers read this table or position_current, never the history.';
COMMENT ON COLUMN position_daily.position_id IS 'PK. The bytea(32) native position identity from position_id() (VEC-400).';
COMMENT ON COLUMN position_daily.as_of_date IS 'PK. UTC date of the winning observation''s block_timestamp. Business today is UTC, never CURRENT_DATE.';
COMMENT ON COLUMN position_daily.chain_id IS 'Derived (copy of position_state.chain_id). Native chain id; nullable per the position_id convention.';
COMMENT ON COLUMN position_daily.protocol_id IS 'Derived (copy of position_state.protocol_id). Native protocol id; nullable per convention.';
COMMENT ON COLUMN position_daily.instrument_key IS 'Derived (copy of position_state.instrument_key). The instrument''s native, globally-unique id.';
COMMENT ON COLUMN position_daily.holder_id IS 'Derived (copy of position_state.holder_id). Native on-chain holder, lowercase hex, no 0x.';
COMMENT ON COLUMN position_daily.quantity IS 'Derived (copy of position_state.quantity). Holder balance in the instrument''s native units on that date; 0 means closed.';
COMMENT ON COLUMN position_daily.block_number IS 'Derived. Block of the winning observation; part of the newer-wins comparison.';
COMMENT ON COLUMN position_daily.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN position_daily.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); part of the newer-wins comparison.';
COMMENT ON COLUMN position_daily.block_timestamp IS 'Derived. On-chain time of the winning observation; the last leg of the newer-wins comparison, so the pick is total when two rows share the other four key columns.';
COMMENT ON COLUMN position_daily.projection IS 'Derived. Which projection view wrote the winning observation.';
COMMENT ON COLUMN position_daily.build_id IS 'Derived. Which build wrote the winning observation (build_registry.id; 0 = pre-tracking).';

-- Mutable by design, like #733's caches: a derived copy, so overwriting loses nothing the history does
-- not still hold.
GRANT SELECT ON position_daily TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON position_daily TO stl_readwrite;

CREATE OR REPLACE FUNCTION upsert_position_daily() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
AS $fn$
BEGIN
    INSERT INTO public.position_daily AS cur
        (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
         block_number, block_version, processing_version, block_timestamp, projection, build_id)
    VALUES
        (NEW.position_id, (NEW.block_timestamp AT TIME ZONE 'utc')::date, NEW.chain_id, NEW.protocol_id,
         NEW.instrument_key, NEW.holder_id, NEW.quantity, NEW.block_number, NEW.block_version,
         NEW.processing_version, NEW.block_timestamp, NEW.projection, NEW.build_id)
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
    FOR EACH ROW
EXECUTE FUNCTION upsert_position_daily();

-- Backfill / rebuild. Empty on first apply (20260818_130000 creates position_state empty); this is the
-- recovery path referenced in the table's COMMENT.
INSERT INTO position_daily
    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
     block_number, block_version, processing_version, block_timestamp, projection, build_id)
SELECT DISTINCT ON (p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date)
       p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date, p.chain_id, p.protocol_id,
       p.instrument_key, p.holder_id, p.quantity, p.block_number, p.block_version,
       p.processing_version, p.block_timestamp, p.projection, p.build_id
FROM position_state p
ORDER BY p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date,
         p.block_number DESC, p.block_version DESC, p.processing_version DESC, p.block_timestamp DESC
ON CONFLICT (position_id, as_of_date) DO NOTHING;

-- Current state is the latest day per position. Cheap as a view because position_daily has already
-- collapsed the version noise: measured 1,570 buffers for 5,000 positions, against a DISTINCT ON over
-- the raw history. It is a view, not a second table, so there is only one derived copy to keep honest.
CREATE OR REPLACE VIEW position_current AS
SELECT DISTINCT ON (position_id) *
FROM position_daily
ORDER BY position_id, as_of_date DESC;

COMMENT ON VIEW position_current IS '[Operational] Current state per position (VEC-409): the latest day''s row per position_id from position_daily. A zero quantity means the position is closed as of that date.';

GRANT SELECT ON position_current TO stl_readonly;
GRANT SELECT ON position_current TO stl_readwrite;

INSERT INTO migrations (filename) VALUES ('20260819_150000_create_position_daily.sql') ON CONFLICT (filename) DO NOTHING;
