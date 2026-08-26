-- Initial backfill of position_current (VEC-409), and the statement an operator re-runs to converge it.
--
-- Separate from 20260819_150000, which creates the table and the trigger, because the migrator runs a
-- whole file in one transaction. Together, CREATE TRIGGER's SHARE ROW EXCLUSIVE on position_state was
-- held for the length of the full-history scan below, and that lock conflicts with the ROW EXCLUSIVE
-- every ingest INSERT takes -- so every writer queued for the duration, and `SET LOCAL lock_timeout`
-- bounds only the acquisition, never the hold. Split, the statement below holds ACCESS SHARE on
-- position_state, which conflicts with nothing ingest does.
--
-- The no-gap invariant survives the split: 20260819_150000 commits before this file starts, so the
-- trigger is live for the whole of this scan. Where the two overlap, whichever resolves second is a
-- guarded no-op, which is the same idempotency that makes re-running this statement safe.
--
-- Re-running this statement is a FORWARD-ONLY MERGE, not a full rebuild. The newer-wins guard only
-- raises a cached row; it never lowers one and never removes one, so it cannot repair a row that is
-- AHEAD of history (a wrong direct INSERT, or a restore whose cache backup is newer than its history
-- backup) nor an orphan row whose position has no history at all. Both were reproduced. A true rebuild
-- is TRUNCATE then this statement, and TRUNCATE is owner-only -- stl_readwrite holds neither DELETE nor
-- TRUNCATE. The ORPHAN class therefore needs the owner; the ahead-of-history class does NOT -- a plain
-- UPDATE repairs it, because UPDATE is unguarded in both directions, so the same grant that creates that
-- drift also removes it (both verified).
--
-- Safe to re-run against live ingest, because both writers sweep this table's PK in position_id order
-- (see the trigger in 20260819_150000): measured 0 deadlocks over 120 rounds and 952 ingest batches at
-- 210,000 cached positions, against 16/20 for a matched control differing only in trigger granularity.
-- Two residuals stand. A writer splitting one batch across several statements in one transaction still
-- crosses (6/20, the REBUILD as victim), and a converged no-op run still takes an exclusive row lock on
-- every row it examines (INSERT 0 0, 1.6M buffer touches, 1.3 s) -- so a caller needs the 40P01 retry
-- the spine documents (retry.Do / isRetryableTxError, #739).
-- REBUILD-BEGIN position_current
-- All three SETs live INSIDE the markers deliberately. The table COMMENT tells operators to re-run the
-- marked region, the integration test extracts exactly that region, and SET LOCAL dies with its
-- transaction -- so a region that omitted them would silently drop the tiered-read guarantee this
-- statement depends on (a rebuild over local chunks only computes "newest per key" across a PARTIAL
-- table) and the lock_timeout that bounds the row-lock waits above.
--
-- Tiered reads: position_state adds a 1-year tiering policy and partitions on block_timestamp (on-chain
-- time, not insert time), so a historical backfill writes chunks ALREADY older than the policy window
-- -- this reads tiered history from the first policy run, not in a year's time. Set explicitly rather
-- than relied on: the measured default is 'on' for TimescaleDB 2.25.1-pg17 (the CI pin) and 2.27.2-pg18
-- (the PostgreSQL major prod runs), so on those engines it is a no-op -- it is here so correctness does
-- not rest on a GUC default that has changed before and that a Cloud service can set per-instance.
-- cmd/backfillers/transform-bootstrap treats failing to set it as fatal for the same reason.
SET LOCAL lock_timeout = '10s';
SET LOCAL timescaledb.enable_tiered_reads = 'on';
-- Resolution pinned for anything added later; the two relations below are already qualified. Without
-- it a hand-run rebuild under a search_path with a shadowing schema ahead of public silently upserts
-- into the shadow table and reports INSERT 0 0 -- byte-identical to a healthy converged run (measured).
SET LOCAL search_path = public;
-- Stamp the LIVE transaction id into a transaction-local setting. The guard below re-reads it in a
-- SEPARATE statement, so the pair proves the region is running inside ONE transaction -- and it cannot
-- be satisfied by a pre-seeded default, which is why it is an xid rather than a fixed sentinel value.
-- Measured on 2.25.1-pg17: inside BEGIN/COMMIT the stamp equals the live xid and the guard passes; with
-- no transaction block the stamp is rolled back with its own implicit transaction and reads NULL, so the
-- guard fires; and `ALTER ROLE ... SET position_current.rebuild_xid` pre-seeds a value that can never
-- equal the live xid, so the guard still fires. A fixed sentinel fails that third case -- ALTER ROLE
-- accepts a custom GUC and current_setting then returns it with every SET LOCAL inert (measured).
SELECT set_config('position_current.rebuild_xid', pg_current_xact_id()::text, true);

INSERT INTO public.position_current
    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
     block_number, block_version, processing_version, block_timestamp, projection, build_id)
SELECT DISTINCT ON (p.position_id)
       p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date, p.chain_id, p.protocol_id,
       p.instrument_key, p.holder_id, p.quantity, p.block_number, p.block_version,
       p.processing_version, p.block_timestamp, p.projection, p.build_id
FROM public.position_state p
-- The precondition check, evaluated INSIDE the statement that depends on it. As a preceding DO block it
-- was steppable: psql without ON_ERROR_STOP raised and then ran this INSERT anyway, writing every row
-- after reporting the error (measured -- 12 rows written on a fixture that should have written none).
-- The subquery is uncorrelated, so it is an InitPlan/One-Time Filter evaluated once per statement, not
-- once per row; the check and the write now stand or fall together. Defined in 20260819_150000.
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
WHERE (EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version, EXCLUDED.block_timestamp)
    > (position_current.block_number, position_current.block_version, position_current.processing_version, position_current.block_timestamp);
-- REBUILD-END position_current

-- SET LOCAL lives until the transaction ends, not until the marker, so without this the statements below
-- would resolve under a hardcoded `public` instead of the applying session's own search_path.
RESET search_path;

-- Holder access path, built AFTER the backfill: created first, every backfilled row would pay a random
-- btree insert with its own WAL instead of one bulk build, and no read inside this file needs it. The PK
-- serves position_id lookups; the enriched views planned on this layer filter by holder, which the PK
-- cannot serve. Measured at 200,000 positions: a holder's current positions went from 4,652 buffers and
-- 8.9 ms (seq scan) to 1 buffer and 0.046 ms, for 2.8 MB.
CREATE INDEX IF NOT EXISTS position_current_holder_idx ON position_current (holder_id);

ANALYZE position_current;

INSERT INTO migrations (filename) VALUES ('20260819_150100_backfill_position_current.sql') ON CONFLICT (filename) DO NOTHING;
