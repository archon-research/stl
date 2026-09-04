-- position_current (VEC-409): one row per position, its latest observation, with reorg and reprocess
-- versions resolved. A derived cache of the position_state history and nothing more -- it answers
-- "now", never "as of block N".
--
-- SHAPE follows the current-position caches in #733 (VEC-577): a PLAIN table, one row per key, no time
-- dimension and therefore no hypertable, no compression policy and no tiering policy -- the hypertable
-- rule is about time-series tables and this is not one. The GRANTS deliberately differ; see the REVOKE
-- below. The history stays append-only and
-- untouched, and an AFTER INSERT trigger on position_state keeps this fresh.
--
-- This file creates the table and its maintainer only. The initial backfill is a SEPARATE migration,
-- 20260819_150100_backfill_position_current, and the split is load-bearing: the migrator runs a whole
-- file in one transaction, so keeping the two together held CREATE TRIGGER's lock on position_state for
-- the length of a full-history scan and queued every ingest writer behind it. This file commits first,
-- so the trigger is live for the whole of that backfill and no row can commit in a gap. Where the two
-- overlap, the newer-wins guard each carries makes the second one a no-op.
--
-- Newer-wins compares the FULL observation key including block_timestamp. position_state documents
-- block_timestamp as invariant per logical key (a drifted re-emission is stored-and-warned, never
-- applied), so through the sanctioned write path the four-column key is already unique -- but the
-- five-column PK does not ENFORCE that and stl_readwrite can INSERT directly, so the fourth leg is
-- defensive: it keeps the pick total even for a pair the PK admits.
--
-- Ordering is block_number first, per the house rule for selecting the latest row. One consequence is
-- worth stating because this file opens by calling reorg and reprocess versions "resolved": a reorg
-- whose replacement lands at a
-- LOWER block number than the cached row does NOT win, so an orphaned-branch observation can stay
-- current. Same-block reorgs resolve correctly on block_version. Changing that needs a house-level
-- decision about the ordering, not a local deviation here.
--
-- Why a table rather than deriving it on read. A recursive-CTE "loose index scan" over the history is an
-- optimization FENCE, so a predicate cannot be pushed into it, and every enriched view built on this
-- layer would inherit that: measured at 200,000 positions, a single-position lookup cost 624 ms through
-- such a view against 0.047 ms here. For every observation of a position, read position_state
-- directly.
--
-- as_of_date is derived in UTC, matching the repo's business-today rule (never CURRENT_DATE), and a
-- CHECK pins it to that derivation so a direct INSERT cannot date an observation to any day it likes.

-- Fail fast rather than convoy ingestion. CREATE TRIGGER below takes SHARE ROW EXCLUSIVE on
-- position_state, which conflicts with the ROW EXCLUSIVE every ingest INSERT takes, and the migrator
-- runs this whole file in one transaction -- so without a bound this waits out every in-flight writer
-- while queueing all new INSERTs behind it. A FOR EACH STATEMENT trigger is not propagated to chunks
-- (only ROW triggers are), so the hold is the catalogue write alone once the lock is acquired; the wait
-- to acquire it is what this bounds. This file must therefore never be marked
-- `-- migrate: no-transaction`: outside a transaction block SET LOCAL only warns and changes nothing.
SET LOCAL lock_timeout = '10s';

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
    -- Mirrors position_state exactly: a 20-byte address as 40 lowercase hex characters. An unanchored
    -- form also admits a decimal surrogate rendered as text, and a cache that accepted a holder the
    -- spine rejects could never be reproduced from the spine.
    CONSTRAINT position_current_holder_hex_chk CHECK (holder_id ~ '^[0-9a-f]{40}$'),
    -- Mirrors position_state: a key longer than the bridge's PK btree can store resolves to nothing.
    CONSTRAINT position_current_instrument_key_len_chk CHECK (char_length(instrument_key) <= 2000),
    CONSTRAINT position_current_coord_nonneg_chk CHECK (block_number >= 0 AND block_version >= 0 AND processing_version >= 0 AND build_id >= 0),
    CONSTRAINT position_current_chain_pos_chk CHECK (chain_id IS NULL OR chain_id > 0),
    CONSTRAINT position_current_protocol_pos_chk CHECK (protocol_id IS NULL OR protocol_id > 0),
    CONSTRAINT position_current_ts_sane_chk CHECK (block_timestamp >= '2009-01-03 00:00:00+00'::timestamptz),
    -- as_of_date is DERIVED from block_timestamp by both writers, so pin the two together rather than
    -- trusting them to agree. Without this a direct INSERT can date an observation to any day it likes.
    -- timezone(text, timestamptz) is IMMUTABLE, so the expression is CHECK-legal.
    CONSTRAINT position_current_as_of_date_chk CHECK (as_of_date = (block_timestamp AT TIME ZONE 'utc')::date)
);

COMMENT ON TABLE position_current IS '[Operational] One row per position: its newest observation by (block_number, block_version, processing_version, block_timestamp) (VEC-409). Derived cache of the position_state history, maintained by trigger_upsert_position_current. Deliberately a PLAIN table, no hypertable/compression/tiering: it carries one row per position and no time dimension, so chunking buys nothing -- history lives in position_state. Ordering is block_number FIRST, so a reorg replacement landing at a LOWER block does NOT win: the orphaned observation stays current until a higher block supersedes it -- and that bound is only finite if the materializer emits a same-or-higher-block row for EVERY position it touched in the orphaned range. No contract here or on the spine requires that, so a position whose replacement branch carries no event at all (a reorged-out deposit into a position then abandoned) keeps the orphaned quantity as current indefinitely, and it sums into every exposure total taken from this table. Re-running the statement between the REBUILD-BEGIN/END position_current markers in 20260819_150100 is a FORWARD-ONLY merge -- it raises rows and converges a payload that drifted at equal coordinates, but never lowers or removes them, so it cannot repair a row ahead of history or an orphan; a true rebuild is TRUNCATE first, and TRUNCATE is owner-only. Re-running the marked statement against live ingest measured 0 deadlocks over 120 rounds and 952 batches, because the trigger and this statement both sweep this table''s PK in position_id order; the residual is a writer that splits one batch across several statements in one transaction, which measured 6/20 with the REBUILD as the victim, so the 40P01 retry the spine documents still applies. Every point-in-time question (what was X at block N) is answered from position_state, never from here. A zero quantity is a real observation of an emptied position as of as_of_date, not a gap and not a lifecycle close.';
COMMENT ON COLUMN position_current.position_id IS 'PK. The bytea(32) native position identity from position_id() (VEC-400).';
COMMENT ON COLUMN position_current.as_of_date IS 'Derived. UTC date of the latest observation''s block_timestamp — the date of the LAST OBSERVATION, not a snapshot date. Business today is UTC, never CURRENT_DATE. Do not equality-filter or date-join on it: `WHERE as_of_date = <today>` silently drops every position not re-observed today (for an event-driven projection that is most of them, and exposure collapses toward zero with no error), `GROUP BY as_of_date` yields a histogram of last-touch dates rather than a daily series, and joining it to a daily price table pairs a live quantity with a stale price. A genuine per-day series is a different grain and this table does not serve it. It is also not homogeneous across sources: block_timestamp is chain time for chain projections but sync time where the spine documents an event-time source stamping synced_at, so this column means chain day for some rows and sync day for others and any date-keyed join mixes the two calendars.';
COMMENT ON COLUMN position_current.chain_id IS 'Derived (copy of position_state.chain_id). Native chain id; NULL is a deliberate materializer convention, not missing data. Consequence for readers: `WHERE chain_id = $1` excludes the NULL-convention rows and `GROUP BY chain_id` buckets them under NULL, so per-chain aggregates do not reconcile against the unfiltered total -- reconcile any grouped sum against `SUM(...)` over the whole table before reporting a discrepancy as a data bug.';
COMMENT ON COLUMN position_current.protocol_id IS 'Derived (copy of position_state.protocol_id). Native protocol id; NULL is a deliberate materializer convention, not missing data, and carries the same reconciliation caveat as chain_id: `WHERE protocol_id = $1` and `GROUP BY protocol_id` both drop or re-bucket those rows, so a per-protocol total will not match the unfiltered sum.';
COMMENT ON COLUMN position_current.instrument_key IS 'Derived (copy of position_state.instrument_key). The instrument''s native, globally-unique id.';
COMMENT ON COLUMN position_current.holder_id IS 'Derived (copy of position_state.holder_id). Native on-chain holder, lowercase hex, no 0x.';
COMMENT ON COLUMN position_current.quantity IS 'Derived (copy of position_state.quantity). Holder balance in the instrument''s native units; a zero is a real observation of an emptied position, not a gap. Scale is source-defined and NOT normalized across protocols, exactly as on position_state.quantity, so a consumer must not SUM across heterogeneous instruments without normalizing first. This is the LAST observation, not a live value: if a projection stops observing a position, its last nonzero quantity stays here indefinitely, and reconciling against the spine cannot detect it because the spine agrees. Nothing records that a projection completed a sweep, so the only staleness signal is as_of_date age, and an exposure-style sum should weigh it.';
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
-- This is a derived cache of exactly that query: position_state stays append-only and untouched, and
-- nothing here is a source of truth. What the maintainer provides is an idempotent FORWARD-ONLY merge:
-- it raises a cached row to a newer observation, converges one whose payload drifted at equal
-- coordinates, and never lowers or removes one -- so a row that is ahead of history or has no history at
-- all does not converge on its own (see 20260819_150100, which documents both classes and who can
-- repair them). It therefore DOES take ON CONFLICT DO UPDATE and
-- hold the UPDATE grant. The corollary is a hard limit on how it may be read: position_current answers
-- "now", never "as of block N".
--
-- SELECT, INSERT and UPDATE, with DELETE revoked: the trigger and the backfill in 20260819_150100 only
-- ever insert or overwrite, so a delete channel would be unused reach. TRUNCATE is not granted either, so the
-- remove-rows path is owner-only by construction. This is deliberately STRICTER than the
-- current-position caches in #733, which take the same DO UPDATE arm but keep the DELETE they arrive
-- with (see the REVOKE below).
--
-- UPDATE here is a precondition for INSERT into position_state, not an independent grant. The trigger's
-- upsert carries an ON CONFLICT ... DO UPDATE arm and the function is SECURITY INVOKER, so per
-- AGENTS.md in this directory that arm "requires UPDATE privilege and is refused at executor start
-- whether or not a conflict occurs" -- with the writer's own privileges. A role that can INSERT into
-- position_state but lacks UPDATE here therefore cannot write history at all: every materializer
-- statement fails at executor start, not on conflict. Any narrower materializer role (VEC-562) must be
-- granted UPDATE on this table in the same migration that grants it INSERT on position_state.
-- TestPositionStateWritersHoldUpdateOnPositionCurrent asserts that coupling from the catalogue.

GRANT SELECT ON position_current TO stl_readonly;
GRANT SELECT, INSERT, UPDATE ON position_current TO stl_readwrite;
-- The GRANT above does NOT remove DELETE, and withholding it is not the same as revoking it:
-- 20260122_140100 uses ALTER DEFAULT PRIVILEGES to grant SELECT, INSERT, UPDATE, DELETE on every new
-- public table to stl_readwrite, so this table arrives with DELETE already held and the narrowed GRANT
-- is a no-op. Only the explicit REVOKE closes it, and the integration suite asserts the resulting ACL
-- from the catalogue rather than from the GRANT list, because the two disagree. #733's caches have the
-- same latent gap and do not revoke it. TRUNCATE is not in the default grant, so it needs no REVOKE and
-- stays owner-only.
REVOKE DELETE ON position_current FROM stl_readwrite;

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
    -- aligned with the compression and tiering jobs. The rebuild in 20260819_150100 picks each position's
    -- newest row and can only order by the STORED timestamp. Those orders coincide only when a batch
    -- re-observes its positions in the same relative time order as their previous observations -- and
    -- live ingest is typically the opposite, because every row in one block shares a block_timestamp,
    -- collapsing the writer's order to position_id while the rebuild sorts by differing stored timestamps.
    --
    -- position_id is a sha256, so ordering BOTH writers by it is a total order no timestamp can permute;
    -- 20260819_150100 carries the deadlock measurement. It also collapses an N-row batch to one
    -- invocation, which is the cheaper shape: at 200,000 rows in one statement a FOR EACH ROW equivalent
    -- of this function cost 3.6x as much trigger-attributable time (2.2x the whole statement). Residual:
    -- the total order is per STATEMENT, so a writer splitting one batch across several statements in a
    -- transaction can still cross, and the 40P01 retry the spine documents stays necessary.
    --
    -- Cost of a FIRST whole-history materialization: the transition table holds the rows actually
    -- inserted, so a converged re-projection materializes nothing (measured: INSERT 0 0, zero
    -- trigger-attributable temp bytes), but a first run spills ~340 bytes per row of temporary files and
    -- roughly doubles the statement (measured linear at 1M and 3M rows). Bounding that by chunking the
    -- materializer's re-projection into per-position_id-range statements is a runner change, not a
    -- schema one, and is tracked with the runner.
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
        > (cur.block_number, cur.block_version, cur.processing_version, cur.block_timestamp)
       OR ((EXCLUDED.block_number, EXCLUDED.block_version, EXCLUDED.processing_version, EXCLUDED.block_timestamp)
            = (cur.block_number, cur.block_version, cur.processing_version, cur.block_timestamp)
           AND (EXCLUDED.quantity, EXCLUDED.as_of_date, EXCLUDED.chain_id, EXCLUDED.protocol_id, EXCLUDED.instrument_key, EXCLUDED.holder_id, EXCLUDED.projection, EXCLUDED.build_id)
               IS DISTINCT FROM
               (cur.quantity, cur.as_of_date, cur.chain_id, cur.protocol_id, cur.instrument_key, cur.holder_id, cur.projection, cur.build_id));
    RETURN NULL;
END;
$fn$;

COMMENT ON FUNCTION upsert_position_current() IS '[Operational] Keeps position_current at the latest observation per position (VEC-409), by (block_number, block_version, processing_version, block_timestamp). AFTER INSERT on position_state, once per STATEMENT over its transition table and ordered by position_id so the rebuild cannot cross it; an out-of-order insert cannot regress a position.';

-- Precondition for the REBUILD region in 20260819_150100. Called from inside that INSERT so a stepped
-- apply cannot write past it, and standalone as well because a qual is dropped when TimescaleDB excludes
-- every chunk at plan time; where the qual is dropped there is nothing to write.
-- Branch 1 proves the region is in one transaction -- outside one every SET LOCAL is an inert warning.
-- An xid, not a sentinel: ALTER ROLE ... SET can pre-seed a custom GUC, an xid it cannot.
-- Branch 2 checks the setting the guard exists to protect, by name. Both reads use missing_ok so an
-- engine without the extension gets this message, not "unrecognized configuration parameter".
-- VOLATILE because the body calls pg_current_xact_id(), which assigns an xid when there is none; it is
-- free, since the once-per-statement plan comes from the sublink being uncorrelated. Measurements in #644.
CREATE OR REPLACE FUNCTION public.position_current_rebuild_guard()
RETURNS boolean LANGUAGE plpgsql VOLATILE
SET search_path = pg_catalog, public
AS $guard$
BEGIN
    IF current_setting('position_current.rebuild_xid', true) IS DISTINCT FROM pg_current_xact_id()::text THEN
        RAISE EXCEPTION 'run the REBUILD region inside ONE transaction (BEGIN; ... COMMIT;): the transaction stamp does not match this statement''s transaction, so every SET LOCAL in the region is an inert warning -- no tiered-read guarantee and no lock bound';
    END IF;
    IF current_setting('timescaledb.enable_tiered_reads', true) IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION 'timescaledb.enable_tiered_reads is %, not on: this would compute "newest per key" over local chunks only and report a byte-identical INSERT 0 N ((unset) means the timescaledb extension is not loaded on this database)', coalesce(current_setting('timescaledb.enable_tiered_reads', true), '(unset)');
    END IF;
    RETURN true;
END
$guard$;

COMMENT ON FUNCTION public.position_current_rebuild_guard() IS
    '[Operational] Precondition for the REBUILD region of 20260819_150100 (VEC-409). Called both standalone and from inside that INSERT, so neither a stepped apply nor plan-time chunk exclusion can skip it; raises unless the region runs in one transaction with tiered reads on.';


-- Guarded like every other DDL statement in this file, so a re-run -- a manual apply, a restore, a
-- partial-apply recovery -- does not fail with "trigger already exists".
DROP TRIGGER IF EXISTS trigger_upsert_position_current ON position_state;
CREATE TRIGGER trigger_upsert_position_current
    AFTER INSERT ON position_state
    REFERENCING NEW TABLE AS newrows
    FOR EACH STATEMENT
EXECUTE FUNCTION upsert_position_current();

-- KNOWN GAP, no DDL fix. This trigger stays at the ORIGIN default because TimescaleDB refuses to enable
-- or disable a trigger on a hypertable at all, and enabling it before compression is not open either --
-- position_state and its columnstore settings come from earlier, applied migrations.
-- The consequence: at ORIGIN the trigger does not fire under session_replication_role = 'replica', which
-- pg_restore --disable-triggers sets. A plain INSERT and a COPY both land rows and skip it, with no
-- error, so history advances and this cache does not. (Logical replication is out of scope for another
-- reason: its apply worker never fires statement-level triggers, whatever tgenabled says.)
-- Nothing repairs it downstream -- the transition table is empty for committed rows and the materializer
-- is NOT EXISTS + DO NOTHING. Recovery is the REBUILD region in 20260819_150100 -- run it after any
-- restore, and after any bulk load done under the replica role.
-- The two refusals -- on a plain hypertable and on one with columnstore enabled -- and the replica-role
-- skip are measured in #644.

INSERT INTO migrations (filename) VALUES ('20260819_150000_create_position_current.sql') ON CONFLICT (filename) DO NOTHING;
