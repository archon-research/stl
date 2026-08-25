-- position_current (VEC-409): one row per position, its latest observation, with reorg and reprocess
-- versions resolved. A derived cache of the position_state history and nothing more -- it answers
-- "now", never "as of block N".
--
-- Shape follows the current-position caches in #733 (VEC-577) exactly: a PLAIN table, one row per key,
-- no time dimension and therefore no hypertable, no compression policy and no tiering policy -- the
-- hypertable rule is about time-series tables and this is not one. The history stays append-only and
-- untouched; an AFTER INSERT trigger on position_state keeps this fresh; the trigger is created BEFORE
-- the backfill so no row can commit in the gap; and the backfill carries the SAME newer-wins guard as
-- the trigger, so re-running it IS the rebuild -- idempotent, safe against a live cache, no TRUNCATE.
--
-- Newer-wins compares the FULL observation key including block_timestamp. position_state documents
-- block_timestamp as invariant per logical key (a drifted re-emission is stored-and-warned, never
-- applied), so through the sanctioned write path the four-column key is already unique -- but the
-- five-column PK does not ENFORCE that and stl_readwrite can INSERT directly, so the fourth leg is
-- defensive: it keeps the pick total even for a pair the PK admits.
--
-- Ordering is block_number first, per the house rule for selecting the latest row. One consequence is
-- worth stating because the COMMENT below says "reorg resolved": a reorg whose replacement lands at a
-- LOWER block number than the cached row does NOT win, so an orphaned-branch observation can stay
-- current. Same-block reorgs resolve correctly on block_version. Changing that needs a house-level
-- decision about the ordering, not a local deviation here.
--
-- Why a table rather than deriving it on read. A recursive-CTE "loose index scan" over the history is an
-- optimization FENCE, so a predicate cannot be pushed into it: measured at 200,000 positions, a
-- single-position lookup through such a view cost 624 ms against 0.047 ms here, and every enriched view
-- built on this layer would inherit that. Reading all current positions is 4,652 buffers / 14.6 ms from
-- this table against 654 ms derived. For a per-day series rather than "now", read position_daily
-- (VEC-636); for every observation of a position, read position_state directly -- its PK leads with
-- position_id, so one position's full history is 243 buffers / 2.2 ms even on fully compressed chunks.
--
-- as_of_date is derived in UTC, matching the repo's business-today rule (never CURRENT_DATE), and a
-- CHECK pins it to that derivation so a direct INSERT cannot date an observation to any day it likes.

-- Fail fast rather than convoy ingestion. CREATE TRIGGER below takes SHARE ROW EXCLUSIVE on
-- position_state, held to commit and propagated to its chunks, and the migrator runs this whole file in
-- one transaction -- so without a bound this waits out every in-flight writer while queueing all new
-- INSERTs behind it. Same rationale and value as the current-position caches in #733; re-run in a
-- quieter window. This file must therefore never be marked `-- migrate: no-transaction`: outside a
-- transaction block SET LOCAL only warns and changes nothing, silently restoring both hazards these two
-- statements exist to prevent. Also set inside the REBUILD markers below, because a hand-run rebuild
-- needs them just as much and SET LOCAL does not outlive its transaction.
SET LOCAL lock_timeout = '10s';

-- The backfill below must see S3-tiered history: a backfill that reads local chunks only computes
-- "newest per key" over a partial table, and a position whose newest observation has been tiered gets a
-- stale row or none at all. position_state adds a 1-year tiering policy, and it partitions on
-- block_timestamp (on-chain time, not insert time), so a historical backfill writes chunks ALREADY older
-- than the policy window -- this reads tiered history from the first policy run, not in a year's time.
--
-- Set explicitly rather than relied on. Measured default is 'on' for TimescaleDB 2.25.1-pg17 (the CI
-- pin) and 2.27.2-pg18 (the PostgreSQL major prod runs), so on those engines this is a no-op -- it is
-- here so correctness does not rest on a GUC default that has changed before and that a Cloud service
-- can set per-instance. cmd/backfillers/transform-bootstrap treats failing to set it as fatal for the
-- same reason.
SET LOCAL timescaledb.enable_tiered_reads = 'on';

CREATE TABLE IF NOT EXISTS position_current (
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
    CONSTRAINT position_current_pkey PRIMARY KEY (position_id),

    -- The same guards position_state carries, because these tables are the surface consumers are told
    -- to read and stl_readwrite can INSERT into them directly, not only through the trigger. Without
    -- them a hand-written INSERT can seat a NaN quantity, a wrong-width position_id that joins to
    -- nothing, or an uppercase holder_id that forks one wallet into two identities -- in the copy,
    -- while the history stays clean. NaN needs both clauses: NaN >= 0 is TRUE in numeric ordering, and
    -- it is `< 'Infinity'` that rejects it.
    CONSTRAINT position_current_id_len_chk CHECK (octet_length(position_id) = 32),
    CONSTRAINT position_current_qty_nonneg_chk CHECK (quantity >= 0 AND quantity <> 'NaN'::numeric AND quantity < 'Infinity'::numeric),
    -- Mirrors position_state exactly: a 20-byte address as 40 lowercase hex characters. The
    -- unanchored form also admitted a decimal surrogate rendered as text, and a cache that accepted a
    -- holder the spine rejects could never be reproduced from the spine.
    CONSTRAINT position_current_holder_hex_chk CHECK (holder_id ~ '^[0-9a-f]{40}$'),
    -- Mirrors position_state: a key longer than the bridge's PK btree can store resolves to nothing.
    CONSTRAINT position_current_instrument_key_len_chk CHECK (char_length(instrument_key) <= 2000),
    CONSTRAINT position_current_coord_nonneg_chk CHECK (block_number >= 0 AND block_version >= 0 AND processing_version >= 0 AND build_id >= 0),
    CONSTRAINT position_current_chain_pos_chk CHECK (chain_id IS NULL OR chain_id > 0),
    CONSTRAINT position_current_protocol_pos_chk CHECK (protocol_id IS NULL OR protocol_id > 0),
    CONSTRAINT position_current_ts_sane_chk CHECK (block_timestamp >= '2009-01-03 00:00:00+00'::timestamptz),
    -- as_of_date is DERIVED from block_timestamp by both writers, so pin the two together rather than
    -- trusting them to agree. Without this a direct INSERT can date an observation to any day it likes,
    -- timezone(text, timestamptz) is
    -- IMMUTABLE, so the expression is CHECK-legal.
    CONSTRAINT position_current_as_of_date_chk CHECK (as_of_date = (block_timestamp AT TIME ZONE 'utc')::date)
);

COMMENT ON TABLE position_current IS '[Operational] One row per position: its newest observation by (block_number, block_version, processing_version, block_timestamp) (VEC-409). Derived cache of the position_state history, maintained by trigger_upsert_position_current. Deliberately a PLAIN table, no hypertable/compression/tiering: it carries one row per position and no time dimension, so chunking buys nothing -- history lives in position_state. Ordering is block_number FIRST, so a reorg replacement landing at a LOWER block does NOT win: the orphaned observation stays current until a higher block supersedes it. Re-running the statement between the REBUILD-BEGIN/END position_current markers in 20260819_150000 is a FORWARD-ONLY merge -- it raises rows, never lowers or removes them, so it cannot repair a row ahead of history or an orphan; a true rebuild is TRUNCATE first, and TRUNCATE is owner-only. Re-running the marked statement against live ingest measured 0 deadlocks over 120 rounds and 952 batches, because the trigger and this statement both sweep this table''s PK in position_id order; the residual is a writer that splits one batch across several statements in one transaction, which measured 6/20 with the REBUILD as the victim, so the 40P01 retry the spine documents still applies. Every point-in-time question (what was X at block N) is answered from position_state, never from here. A zero quantity is a real observation of an emptied position as of as_of_date, not a gap and not a lifecycle close.';
COMMENT ON COLUMN position_current.position_id IS 'PK. The bytea(32) native position identity from position_id() (VEC-400).';
COMMENT ON COLUMN position_current.as_of_date IS 'Derived. UTC date of the latest observation''s block_timestamp — which day this state is as of. Business today is UTC, never CURRENT_DATE.';
COMMENT ON COLUMN position_current.chain_id IS 'Derived (copy of position_state.chain_id). Native chain id; nullable per the position_id convention.';
COMMENT ON COLUMN position_current.protocol_id IS 'Derived (copy of position_state.protocol_id). Native protocol id; nullable per convention.';
COMMENT ON COLUMN position_current.instrument_key IS 'Derived (copy of position_state.instrument_key). The instrument''s native, globally-unique id.';
COMMENT ON COLUMN position_current.holder_id IS 'Derived (copy of position_state.holder_id). Native on-chain holder, lowercase hex, no 0x.';
COMMENT ON COLUMN position_current.quantity IS 'Derived (copy of position_state.quantity). Holder balance in the instrument''s native units; a zero is a real observation of an emptied position, not a gap. Scale is source-defined and NOT normalized across protocols, exactly as on position_state.quantity, so a consumer must not SUM across heterogeneous instruments without normalizing first.';
COMMENT ON COLUMN position_current.block_number IS 'Derived. Block of the latest observation; part of the newer-wins comparison.';
COMMENT ON COLUMN position_current.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN position_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); part of the newer-wins comparison.';
COMMENT ON COLUMN position_current.block_timestamp IS 'Derived. On-chain time of the latest observation; the last leg of the newer-wins comparison, so the pick is total when two rows share the other four key columns.';
COMMENT ON COLUMN position_current.projection IS 'Audit. Which projection view wrote the latest observation.';
COMMENT ON COLUMN position_current.build_id IS 'Audit. Which build wrote the latest observation (build_registry.id; 0 = pre-tracking).';

-- Relationship to the strict append-only rule (see AGENTS.md in this directory). This table sits
-- OUTSIDE it, on the same footing as the transformed layer (20260706_140000_create_transformed_bucket1)
-- and the current-position caches in #733. The rule governs HISTORY -- ingest tables whose rows are
-- observations, where "current" must stay a query so reorgs roll back and replays are order-independent.
-- These are a derived cache of exactly that query: position_state stays append-only and untouched,
-- nothing here is a source of truth, and every row is reproducible by re-running the backfill below.
-- They therefore DO take ON CONFLICT DO UPDATE and hold the UPDATE grant. The corollary is a hard limit
-- on how it may be read: position_current answers "now", never "as of block N". The per-day series
-- lives in position_daily (VEC-636).
--
-- SELECT, INSERT, UPDATE and no DELETE, matching the current-position caches in #733: the trigger and
-- the backfill only ever insert or overwrite, so a delete channel would be unused reach. TRUNCATE is
-- not granted either, so the remove-rows path is owner-only by construction.

GRANT SELECT ON position_current TO stl_readonly;
GRANT SELECT, INSERT, UPDATE ON position_current TO stl_readwrite;
-- The GRANT above does NOT remove DELETE, and withholding it is not the same as revoking it:
-- 20260122_140100 uses ALTER DEFAULT PRIVILEGES to grant SELECT, INSERT, UPDATE, DELETE on every new
-- public table to stl_readwrite, so this table arrives with DELETE already held and the narrowed GRANT
-- is a no-op. Only the explicit REVOKE closes it. (Caught by TestPositionCurrent asserting the ACL from
-- the catalogue rather than trusting the GRANT list -- the comment above claimed "no DELETE" while
-- has_table_privilege said otherwise. #733's caches have the same latent gap.) TRUNCATE is not in the
-- default grant, so it needs no REVOKE and stays owner-only.
REVOKE DELETE ON position_current FROM stl_readwrite;

-- Holder access path. The PK serves position_id lookups; the enriched views planned on this layer
-- filter by holder, which the PK cannot serve. Measured at 200,000 positions: a holder's current
-- positions went from 4,652 buffers and 8.9 ms (seq scan) to 1 buffer and 0.046 ms, for 2.8 MB.
CREATE INDEX IF NOT EXISTS position_current_holder_idx ON position_current (holder_id);

CREATE OR REPLACE FUNCTION upsert_position_current() RETURNS trigger
    LANGUAGE plpgsql
    -- Explicit, not FROM CURRENT: FROM CURRENT bakes in whatever the applying session happened to have,
    -- so the same migration yields a differently-pinned function under the per-test-schema harness than
    -- in prod. Nothing here needs a user schema -- the body names public.position_current and newrows is
    -- a transition table -- so pinning it costs nothing and removes the dependency on the migrator.
    SET search_path = pg_catalog, public
AS $fn$
BEGIN
    INSERT INTO public.position_current AS cur
        (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
         block_number, block_version, processing_version, block_timestamp, projection, build_id)
    -- One upsert per STATEMENT over the transition table, ordered by position_id. NEW is not available
    -- in a statement trigger; newrows is.
    --
    -- Lock ORDER is why, and a row trigger could not deliver it. A row trigger fires in the writer's
    -- insertion order, which the spine pins to (block_timestamp, position_id) to keep ITS chunk locks
    -- aligned with the compression and tiering jobs. The rebuild picks each position's newest row and can
    -- only order by the STORED timestamp. Those orders coincide only when a batch re-observes its
    -- positions in the same relative time order as their previous observations -- and live ingest is
    -- typically the opposite, because every row in one block shares a block_timestamp, collapsing the
    -- writer's order to position_id while the rebuild sorts by differing stored timestamps. Measured:
    -- ordering the rebuild by (block_timestamp, position_id) to chase a row trigger took one batch shape
    -- to 0/20 deadlocks and left another at 3/20, and produced a case where it deadlocked where the
    -- unordered form did not.
    --
    -- position_id is a sha256, so ordering BOTH writers by it is a total order no timestamp can permute.
    -- Verified 0/20 on both batch shapes at 210,000 cached positions. It also collapses a 10,000-row
    -- batch from 10,000 invocations to one, removing a measured 6.7x per-row upsert cost. Residual: the
    -- total order is per STATEMENT, so a writer splitting one batch across several statements in a
    -- transaction can still cross, and the 40P01 retry the spine documents stays necessary.
    SELECT DISTINCT ON (n.position_id)
           n.position_id, (n.block_timestamp AT TIME ZONE 'utc')::date, n.chain_id, n.protocol_id,
           n.instrument_key, n.holder_id, n.quantity, n.block_number, n.block_version,
           n.processing_version, n.block_timestamp, n.projection, n.build_id
    FROM newrows n
    ORDER BY n.position_id,
             n.block_number DESC, n.block_version DESC, n.processing_version DESC, n.block_timestamp DESC
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
        > (cur.block_number, cur.block_version, cur.processing_version, cur.block_timestamp);
    RETURN NULL;
END;
$fn$;

COMMENT ON FUNCTION upsert_position_current() IS '[Operational] Keeps position_current at the latest observation per position (VEC-409), by (block_number, block_version, processing_version, block_timestamp). AFTER INSERT on position_state, once per STATEMENT over its transition table and ordered by position_id so the rebuild cannot cross it; an out-of-order insert cannot regress a position.';

CREATE TRIGGER trigger_upsert_position_current
    AFTER INSERT ON position_state
    REFERENCING NEW TABLE AS newrows
    FOR EACH STATEMENT
EXECUTE FUNCTION upsert_position_current();

-- Backfill / rebuild. Carries the SAME newer-wins guard as the trigger, so it is idempotent and safe to
-- re-run against a live cache with ingest running: whichever of the two runs second is a guarded no-op.
-- Re-running this statement is a FORWARD-ONLY MERGE, not a full rebuild. The newer-wins guard only
-- raises a cached row; it never lowers one and never removes one, so it cannot repair a row that is
-- AHEAD of history (a wrong direct INSERT, or a restore whose cache backup is newer than its history
-- backup) nor an orphan row whose position has no history at all. Both were reproduced. A true rebuild
-- is TRUNCATE then this statement, and TRUNCATE is owner-only -- stl_readwrite holds neither DELETE nor
-- TRUNCATE. The ORPHAN class therefore needs the owner; the ahead-of-history class does NOT -- a plain
-- UPDATE repairs it, because UPDATE is unguarded in both directions, so the same grant that creates that
-- drift also removes it (both verified).
--
-- REQUIRES A QUIET WINDOW. Re-running this against live ingest deadlocks, and the ingest batch is the
-- victim: measured 14 of 20 rounds on 2.25.1-pg17 at 210,000 cached positions, 0 of 12 when the two do
-- not overlap in time. The cause was lock ORDER, not row selection: both writers now sweep this table's PK
-- in position_id order (see the trigger above). Re-measured on this form: 0 deadlocks over 120 rounds and
-- 952 ingest batches, across single-block and multi-timestamp batch shapes and up to 6 concurrent
-- writers, with overlap confirmed per round -- against 16/20 for a matched control differing only in
-- trigger granularity. The residual the trigger comment names was measured too: a writer splitting one
-- batch across several statements in one transaction deadlocks 6/20, and the REBUILD is the victim. A converged no-op run still takes an exclusive row lock on every row it
-- examines (measured: INSERT 0 0, 210,000 conflicting tuples, 1.6M buffer touches, 1.3 s), so a caller
-- still needs the 40P01 retry the spine documents (retry.Do / isRetryableTxError, #739).
-- REBUILD-BEGIN position_current
-- Both SETs live INSIDE the markers deliberately. The table COMMENT tells operators to re-run the marked
-- region, the integration test extracts exactly that region, and SET LOCAL dies with its transaction --
-- so a region that omitted them would silently drop the tiered-read guarantee this statement depends on
-- (a rebuild over local chunks only computes "newest per key" across a PARTIAL table) and the
-- lock_timeout that bounds the row-lock waits above.
SET LOCAL lock_timeout = '10s';
SET LOCAL timescaledb.enable_tiered_reads = 'on';
-- Resolution pinned for anything added later; the two relations below are already qualified. Without
-- it a hand-run rebuild under a search_path with a shadowing schema ahead of public silently upserts
-- into the shadow table and reports INSERT 0 0 -- byte-identical to a healthy converged run (measured).
SET LOCAL search_path = public;

-- A run that read a PARTIAL history is indistinguishable from a converged one -- both report INSERT 0 N,
-- no error -- and that is the failure the tiered-reads guard above exists to prevent, so it is checked
-- rather than assumed. It also catches the copy-paste path, where both SET LOCALs degrade to warnings the
-- driver discards: lock_timeout is 0 outside a transaction block.
DO $rebuild_guard$
BEGIN
    IF current_setting('lock_timeout') <> '10s' THEN
        RAISE EXCEPTION 'run the REBUILD region inside ONE transaction (BEGIN; ... COMMIT;): outside a transaction block both SET LOCALs are inert warnings, so this would run with no tiered-read guarantee and no lock bound';
    END IF;
END
$rebuild_guard$;
INSERT INTO public.position_current
    (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
     block_number, block_version, processing_version, block_timestamp, projection, build_id)
SELECT DISTINCT ON (p.position_id)
       p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date, p.chain_id, p.protocol_id,
       p.instrument_key, p.holder_id, p.quantity, p.block_number, p.block_version,
       p.processing_version, p.block_timestamp, p.projection, p.build_id
FROM public.position_state p
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


ANALYZE position_current;

INSERT INTO migrations (filename) VALUES ('20260819_150000_create_position_current.sql') ON CONFLICT (filename) DO NOTHING;
