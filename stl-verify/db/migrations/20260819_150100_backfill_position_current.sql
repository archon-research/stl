-- Initial backfill of position_current (VEC-409), and the statement an operator re-runs to converge
-- it. Separate from 20260819_150000 so CREATE TRIGGER's lock is not held across this full-history
-- scan; that file commits first, so the trigger is live throughout. Limits and measurements: #644.

-- REBUILD-BEGIN position_current

-- All three SETs live INSIDE the markers: the region is what operators re-run and what the test
-- extracts, and SET LOCAL dies with its transaction. enable_tiered_reads is set explicitly because a
-- rebuild over local chunks only computes newest-per-key across a PARTIAL table.
SET LOCAL lock_timeout = '10s';
SET LOCAL timescaledb.enable_tiered_reads = 'on';
-- Resolution pinned for anything added later; the two relations below are already qualified. Without
-- it a hand-run rebuild under a search_path with a shadowing schema ahead of public silently upserts
-- into the shadow table and reports INSERT 0 0 -- byte-identical to a healthy converged run (measured).
SET LOCAL search_path = public;
-- Stamp the LIVE transaction id, which the guard below compares against its own statement's xid: the
-- pair is what proves the region is running inside ONE transaction. Why an xid and not a sentinel, and
-- the three measured cases, are in 20260819_150000 where the guard is defined.
SELECT set_config('position_current.rebuild_xid', pg_current_xact_id()::text, true);

-- Standalone AND inside the INSERT below: neither alone suffices. A separate statement is stepped
-- over by a client running the file one statement at a time, and the INSERT's qual is dropped when
-- TimescaleDB excludes every chunk at plan time. Measured on 2.25.1-pg17 (#644).
SELECT public.position_current_rebuild_guard();

INSERT INTO public.position_current
    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
     block_number, block_version, processing_version, block_timestamp, projection, build_id)
SELECT DISTINCT ON (p.position_id)
       p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date, p.chain_id, p.protocol_id,
       p.instrument_key, p.holder_id, p.quantity, p.block_number, p.block_version,
       p.processing_version, p.block_timestamp, p.projection, p.build_id
FROM public.position_state p
-- The same check, inside the statement that depends on it, so a stepped apply cannot write these rows.
-- Uncorrelated, so it is an InitPlan/One-Time Filter evaluated once per statement, not once per row.
WHERE (SELECT public.position_current_rebuild_guard())
-- position_id FIRST, which is both the DISTINCT ON key and the lock order. The statement trigger orders
-- its upsert the same way, so the two writers sweep position_current's PK in one key-derived total order.
ORDER BY p.position_id,
         p.block_number DESC, p.block_version DESC, p.processing_version DESC, p.block_timestamp DESC
ON CONFLICT (position_id) DO UPDATE SET
    as_of_date         = EXCLUDED.as_of_date,
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
-- Raise on a newer observation; CONVERGE on an equal one whose payload drifted, which is why the two
-- unrepairable classes above are only two. IS DISTINCT FROM, not <>, because chain_id and protocol_id
-- are nullable. A cache-only UPDATE is therefore self-reverting -- append a spine row instead (#644).
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version, EXCLUDED.block_timestamp)
    > (position_current.block_number, position_current.block_version, position_current.processing_version, position_current.block_timestamp)
   OR ((EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version, EXCLUDED.block_timestamp)
        = (position_current.block_number, position_current.block_version, position_current.processing_version, position_current.block_timestamp)
       AND (EXCLUDED.quantity, EXCLUDED.as_of_date, EXCLUDED.chain_id, EXCLUDED.protocol_id, EXCLUDED.instrument_key, EXCLUDED.holder_id, EXCLUDED.projection, EXCLUDED.build_id)
           IS DISTINCT FROM
           (position_current.quantity, position_current.as_of_date, position_current.chain_id, position_current.protocol_id, position_current.instrument_key, position_current.holder_id, position_current.projection, position_current.build_id));
-- REBUILD-END position_current

-- SET LOCAL lives until the transaction ends, not until the marker, so without this the statements below
-- would resolve under a hardcoded `public` instead of the applying session's own search_path.
RESET search_path;
-- Schema-qualified BECAUSE of the RESET above: an unqualified name would resolve under the applying
-- session's path, and a shadowing schema ahead of public silently takes the write (measured, #644).

-- Built AFTER the backfill: created first, every backfilled row pays a random btree insert with its
-- own WAL instead of one bulk build. Serves the holder filter the PK cannot. Measurements in #644.
CREATE INDEX IF NOT EXISTS position_current_holder_idx ON public.position_current (holder_id);

ANALYZE public.position_current;

INSERT INTO public.migrations (filename) VALUES ('20260819_150100_backfill_position_current.sql') ON CONFLICT (filename) DO NOTHING;
