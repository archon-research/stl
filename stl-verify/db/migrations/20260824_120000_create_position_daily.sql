-- position_daily (VEC-636): one row per (position, UTC date) -- the winning observation for that
-- position on that day, with reorg and reprocess versions resolved. A derived cache of the
-- position_state history (VEC-402).
--
-- GRAIN, precisely, because the name invites a wrong reading. This is NOT a dense daily series. Only
-- dates on which the position was OBSERVED get a row; there is no carry-forward, so a query for one
-- specific date may correctly return nothing for a position that last moved a month earlier. Read a
-- position's series and take the most recent row at or before the date you want, or read
-- position_current (VEC-409) for "now".
--
-- WHICH TABLE TO READ. position_state answers "every observation of this position" and is the source of
-- truth; position_current (VEC-409) answers "now", one row per position; this table answers "the
-- resolved state per day", which is the shape a time series or a period-over-period comparison wants.
-- Use it for a date-bounded series across one position, one holder, or all positions -- a 7-day window
-- over every position measured 652 buffers / 2.74 ms here against 16,977 / 15.4 ms on an equivalent
-- plain table, and it collapses several same-day observations to the one that survived, which a reader
-- of position_state would otherwise have to do itself. Do NOT use it to reconstruct history at a block:
-- a date is not a version, and limit 2 below is the case where a date-keyed row and the history diverge.
--
-- Mechanics follow the current-position caches in #733 (VEC-577) and position_current: the history stays
-- append-only and untouched, an AFTER INSERT trigger on position_state keeps this fresh, the trigger is
-- created BEFORE the backfill so no row can commit in the gap, and the backfill carries the SAME
-- newer-wins guard as the trigger, so re-running it IS the rebuild -- idempotent, safe against a live
-- cache, no TRUNCATE.
--
-- Newer-wins compares the FULL observation key including block_timestamp. position_state documents
-- block_timestamp as invariant per logical key, so through the sanctioned write path the four-column key
-- is already unique -- but the five-column PK does not ENFORCE that and stl_readwrite can INSERT
-- directly, so the fourth leg is defensive: it keeps the pick total even for a pair the PK admits.
--
-- THREE LIMITS, stated because the COMMENT below says "reorg and reprocess versions resolved" and that
-- is only true within a date:
--
--   1. A reorg whose replacement lands at a LOWER block number does not win. block_number leads the
--      comparison, per the house rule for selecting the latest row, so an orphaned-branch observation
--      can stay as a date's winner. Same-block reorgs resolve correctly on block_version. Changing this
--      needs a house-level decision about the ordering, not a local deviation.
--
--   2. A correction that moves an observation across a UTC midnight leaves the old date's row in place.
--      as_of_date is derived from the winning row's timestamp and the upsert is keyed on it, so a
--      reprocess landing on a different date writes a NEW row and nothing retracts the old one. The
--      backfill reproduces the same state, so the rebuild cannot repair it either. This is faithful to
--      the stated contract -- "the winning observation for that date" is still that row, and
--      position_state still holds it -- but a consumer reading a range sees a value that a later
--      correction moved elsewhere. There is no fix that stays append-only and keyed on the date; the
--      alternative is a delete channel, which this layer deliberately does not have.
--
--   3. There is no cross-table invariant with position_current, and none should be asserted. An earlier
--      revision claimed this table's greatest as_of_date always holds the same row position_current
--      does, and a review disproved it on legal data: position_state documents block_timestamp as NOT a
--      table-wide function of block_number (an event-time source supplies wall-clock), so block 1000 on
--      06-05 and block 1001 on 06-04 make the two disagree. The tables answer different questions and
--      are not reconciled.
--
-- 7-day chunks, not position_state's 1-day: chunk count drives planning cost and per-position fan-out,
-- since a position's series spans every chunk it lives in, and 7 days keeps a chunk a workable size at
-- this table's projected cardinality. Measured at 1,825,000 rows, one position's 365-date history was
-- 1,095 buffers / 4.32 ms at 1-day chunks against 523 / 1.69 ms at 7-day. No standalone as_of_date index
-- -- chunk exclusion serves date-bounded windows, the PK serves one position, the holder index below
-- serves one holder.
--
-- as_of_date is derived in UTC, matching the repo's business-today rule (never CURRENT_DATE), and a
-- CHECK pins it to that derivation so a direct INSERT cannot date an observation to any day it likes.

-- cold chunks, per-chunk compression when it lands, and per-chunk vacuum are what keep it maintainable.
--
-- Every measurement behind the paragraphs above is in the PR description for VEC-636, deliberately not
-- here: they are point-in-time numbers on specific fixtures, and a migration is immutable once applied,
-- so a figure recorded here could never be corrected once it goes stale.
--
-- as_of_date is derived in UTC, matching the repo's business-today rule (never CURRENT_DATE).

-- Fail fast rather than convoy ingestion. Each CREATE TRIGGER below takes SHARE ROW EXCLUSIVE on
-- position_state, held to commit and propagated to its chunks, and the migrator runs this whole file in
-- one transaction -- so without a bound this waits out every in-flight writer while queueing all new
-- INSERTs behind it. Same rationale and value as the current-position caches in #733; re-run in a
-- quieter window. This file must therefore never be marked `-- migrate: no-transaction`: outside a
-- transaction block SET LOCAL only warns and changes nothing, silently restoring both hazards these two
-- statements exist to prevent.
SET LOCAL lock_timeout = '10s';

-- The backfills below must see S3-tiered history: a backfill that reads local chunks only computes
-- "newest per key" over a partial table, and a position whose newest observation has been tiered gets
-- a stale row or none at all. position_state adds a 1-year tiering policy of its own, and it partitions
-- on block_timestamp (on-chain time, not insert time), so a historical backfill writes chunks that are
-- ALREADY older than the policy window -- this table reads tiered history from the first policy run,
-- not in a year's time.
--
-- Set explicitly rather than relied on. Measured default is 'on' for TimescaleDB 2.25.1-pg17 (the CI
-- pin) and 2.27.2-pg18 (the PostgreSQL major prod runs), so on those engines this is a no-op -- it is
-- here so the correctness of the backfill does not depend on a GUC default that has changed before and
-- that a Cloud service can set per-instance. cmd/backfillers/transform-bootstrap treats failing to set
-- it as fatal for the same reason.
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

    -- The same guards position_state carries, because these tables are the surface consumers are told
    -- to read and stl_readwrite can INSERT into them directly, not only through the trigger. Without
    -- them a hand-written INSERT can seat a NaN quantity, a wrong-width position_id that joins to
    -- nothing, or an uppercase holder_id that forks one wallet into two identities -- in the copy,
    -- while the history stays clean. NaN needs both clauses: NaN >= 0 is TRUE in numeric ordering, and
    -- it is `< 'Infinity'` that rejects it.
    CONSTRAINT position_daily_id_len_chk CHECK (octet_length(position_id) = 32),
    CONSTRAINT position_daily_qty_nonneg_chk CHECK (quantity >= 0 AND quantity <> 'NaN'::numeric AND quantity < 'Infinity'::numeric),
    -- Mirrors position_state exactly: a 20-byte address as 40 lowercase hex characters. The unanchored
    -- form also admits a decimal surrogate rendered as text ('12345' is all hex digits), and a cache that
    -- accepted a holder the spine rejects could never be reproduced from the spine.
    CONSTRAINT position_daily_holder_hex_chk CHECK (holder_id ~ '^[0-9a-f]{40}$'),
    -- Mirrors position_state: a key longer than the bridge's PK btree can store resolves to nothing.
    CONSTRAINT position_daily_instrument_key_len_chk CHECK (char_length(instrument_key) <= 2000),
    CONSTRAINT position_daily_coord_nonneg_chk CHECK (block_number >= 0 AND block_version >= 0 AND processing_version >= 0 AND build_id >= 0),
    CONSTRAINT position_daily_chain_pos_chk CHECK (chain_id IS NULL OR chain_id > 0),
    CONSTRAINT position_daily_protocol_pos_chk CHECK (protocol_id IS NULL OR protocol_id > 0),
    CONSTRAINT position_daily_ts_sane_chk CHECK (block_timestamp >= '2009-01-03 00:00:00+00'::timestamptz),
    -- as_of_date is DERIVED from block_timestamp by both writers, so pin the two together rather than
    -- trusting them to agree. Without this a direct INSERT can date an observation to any day it likes,
    -- and on position_daily that also seats a row in the wrong chunk. timezone(text, timestamptz) is
    -- IMMUTABLE, so the expression is CHECK-legal.
    CONSTRAINT position_daily_as_of_date_chk CHECK (as_of_date = (block_timestamp AT TIME ZONE 'utc')::date)
);

-- position_daily is a timeseries table, so it is a hypertable on as_of_date. Converted here while the
-- table is still empty (nothing has been inserted yet), so no data migration is involved.
-- create_default_indexes => FALSE: chunk exclusion on as_of_date does the job a standalone as_of_date
-- index would, and the header explains why that index was unwanted.
--
-- 7-day chunks, not position_state's 1-day. Chunk count drives planning and per-position fan-out, and
-- a position's series spans every chunk it lives in. Measured on identical data (1,825,000 rows, one
-- table 1-day and one 7-day):
--   one position's 365-date history   1-day 1,095 buf / 4.32 ms    7-day   523 buf / 1.69 ms
--   one position's latest day         1-day     3 buf / 1.11 ms    7-day     3 buf / 0.16 ms
--   one holder's full history         1-day 1,095 buf / 3.22 ms    7-day   523 buf / 1.61 ms
-- 7 days also keeps chunks a workable size at the cardinality this is heading for: 200,000 positions
-- x 7 dates x ~190 B is roughly 270 MB per chunk.
--
-- Against a plain table the trade is measured and small, on identical data (730,000 rows, 2,000
-- positions x 365 dates, both with the two indexes below):
--   load 730,000 rows                plain 62.7 s        hypertable 79.3 s   (26% slower writes)
--   heap / index size                plain 133 / 144 MB  hypertable 133 / 111 MB
--   one position's full history      plain 371 buf / 1.05 ms   hyper 471 buf / 1.62 ms
--   one position's latest day        plain   4 buf / 0.045 ms  hyper   3 buf / 0.18 ms
--   one holder's full history        plain 372 buf / 1.23 ms   hyper 471 buf / 1.53 ms
--   7-day window, all positions      plain 16,977 buf / 15.4 ms  hyper 652 buf / 2.74 ms
-- The per-position queries give up a fraction of a millisecond; the window query gains 5.6x. The
-- reason for partitioning is not those numbers though -- it is that at 200,000 positions over two
-- years this table is ~39 GB, where tiering cold chunks to S3, per-chunk compression when it lands,
-- and per-chunk vacuum are the difference between a maintainable table and one that is not.
SELECT create_hypertable('position_daily', 'as_of_date', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE, create_default_indexes => FALSE);

-- Tier cold chunks to S3 after 1 year. add_tiering_policy is a Timescale Cloud/TigerData primitive, so
-- the two absent-capability codes are tolerated and EVERYTHING ELSE IS FATAL — a permission error, a
-- conflicting policy, or a typo must fail the migration rather than ship a 39 GB table with no tiering.
-- WHEN OTHERS would swallow all of those, and its RAISE NOTICE would be invisible: the production
-- migrator installs no pgx notice handler and log_min_messages defaults to warning, so nobody would
-- ever learn tiering had been skipped. Same narrow handler as position_state's.
DO $tier$
BEGIN
    PERFORM add_tiering_policy('position_daily', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function OR feature_not_supported THEN
    RAISE NOTICE 'add_tiering_policy unavailable (%), skipping tiering for position_daily', SQLERRM;
END;
$tier$;

-- NO compression policy, and unlike position_state this deferral is real. position_state DOES ship one
-- (its write path is ON CONFLICT DO NOTHING behind a NOT EXISTS anti-join, so it never rewrites a stored
-- row); this table's trigger is ON CONFLICT DO UPDATE, which must. Measured on the same table, the same
-- chunks and the same rows, which is what isolates the difference:
--   ON CONFLICT DO NOTHING   INSERT 0 0
--   ON CONFLICT DO UPDATE    ERROR: tuple decompression limit exceeded
--                            current limit: 100000, tuples decompressed: 100050
-- A single-row upsert into a compressed chunk is fine (2.25.1-pg17 and 2.27.2-pg18, newer-wins and
-- older-rejected both correct), so it is a volume limit rather than a capability gap -- but a reprocess
-- here rewrites a whole day for every position at once, which is exactly the volume that breaks.
--
-- So this is a straight incompatibility between compression and an in-place upsert, not a cost to
-- accept. VEC-566 carries it, with two exits: record a sanctioned exception to the hypertable rule, or
-- change the write path so this table stops rewriting stored rows. Do NOT cite position_state's
-- compression paragraph as precedent for adding a policy here -- the shapes differ in the one way that
-- matters.

COMMENT ON TABLE position_daily IS '[Hypertable] Partition key: as_of_date. One row per (position, UTC date): the winning observation for that position on that day, reorg and reprocess versions resolved (VEC-409). Every date on which the position was OBSERVED is retained; there is no carry-forward, so a date with no observation has no row and a query for one specific date may correctly return nothing. NO COMPRESSION POLICY, against the default rule: the maintaining trigger upserts in place with ON CONFLICT DO UPDATE, and a reprocess rewriting a whole day for every position exceeds max_tuples_decompressed_per_dml_transaction (measured 100,050 against the 100,000 limit, where the same rows via DO NOTHING are free). VEC-566 carries the exception or the write-path change. S3 tiering at 1 year IS set. Derived cache of the position_state history; rebuildable by re-running the statement between the REBUILD-BEGIN/END position_daily markers in 20260824_120000, which carries the same newer-wins guard as the trigger and is therefore idempotent and safe against a live cache. TRUNCATE is needed only to REMOVE rows and is owner-only (stl_readwrite has no TRUNCATE grant). Consumers should read this table for a date-bounded series, or position_current (VEC-409) for now; nothing enforces that -- stl_readonly can still SELECT position_state.';
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

-- Relationship to the strict append-only rule (see AGENTS.md in this directory). This table sits OUTSIDE
-- it, on the same footing as the transformed layer (20260706_140000_create_transformed_bucket1) and the
-- current-position caches in #733. The rule governs HISTORY -- ingest tables whose rows are
-- observations, where "current" must stay a query so reorgs roll back and replays are order-independent.
-- This is a derived cache of exactly that query: position_state stays append-only and untouched, nothing
-- here is a source of truth, and every row is reproducible by re-running the backfill below. It
-- therefore DOES take ON CONFLICT DO UPDATE and holds the UPDATE grant.
--
-- The distinction from position_current, since #733's wording is "they must never be read as a history":
-- a DATE is not a version. This table cannot answer "as of block N", it never supersedes a version the
-- history holds, and every row is reproducible from position_state. What it does not inherit is the
-- append-only guarantee that a reorg rolls back cleanly -- limit 2 in the header is that case.
--
-- Grants: SELECT, INSERT, UPDATE and NO DELETE. The GRANT below does not achieve that on its own, and
-- withholding a privilege is not the same as revoking it: 20260122_140100 uses ALTER DEFAULT PRIVILEGES
-- to grant SELECT, INSERT, UPDATE, DELETE on every new public table to stl_readwrite, so this table
-- arrives with DELETE already held and the narrowed GRANT is a no-op. Only the explicit REVOKE closes
-- it. (Found on position_current by asserting the ACL from the catalogue rather than trusting the GRANT
-- list; #733's caches have the same latent gap.) TRUNCATE is not in the default grant, so it needs no
-- REVOKE and stays owner-only.
GRANT SELECT ON position_daily TO stl_readonly;
GRANT SELECT, INSERT, UPDATE ON position_daily TO stl_readwrite;
REVOKE DELETE ON position_daily FROM stl_readwrite;

-- Holder access path. The PK serves position_id lookups; enriched views on top of this layer filter by
-- holder, which the PK cannot serve. Measured at 200,000 positions / 2,000,000 rows: one holder's
-- history across instruments went from 46,744 buffers and 32.0 ms (seq scan of a 513 MB table) to
-- 30 buffers and 0.058 ms, for 27 MB of index. as_of_date is the trailing column so the ORDER BY on a
-- holder's series is served by the index too.
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
    -- One upsert per STATEMENT over the transition table, ordered by this table's PK. NEW is not
    -- available in a statement trigger; newrows is.
    --
    -- Lock ORDER is why, and a row trigger cannot deliver it. A row trigger fires in the writer's
    -- insertion order, which the spine pins to (block_timestamp, position_id) to keep ITS chunk locks
    -- aligned with the compression and tiering jobs, while the rebuild below locks this table's PK in
    -- (position_id, as_of_date) order. Those are orthogonal -- position_id is a sha256 -- and on the
    -- sibling cache that cycle was measured at 16/20 rounds with the ingest batch as the victim.
    -- Ordering both writers by the PK is a total order no timestamp can permute; on the sibling that
    -- took it to 0 deadlocks over 120 rounds. It also collapses an N-row batch from N invocations to
    -- one, worth a measured 6.7x on the per-row upsert there. Residual: the total order is per
    -- STATEMENT, so a writer splitting one batch across several statements in a transaction can still
    -- cross, and the runner's 40P01 retry stays necessary.
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

-- Backfill / rebuild. Carries the SAME newer-wins guard as the trigger, so it converges to the same
-- rows whichever runs second. It is NOT a full rebuild and NOT unconditionally live-safe. Re-running it
-- is a FORWARD-ONLY merge: the guard raises a cached row and never lowers or removes one, so it cannot
-- repair a row ahead of history (repairable in-role by a plain UPDATE) or an orphan row whose position
-- has no history (owner-only, since DELETE and TRUNCATE are both revoked). And on the sibling cache,
-- running the equivalent statement against live ingest deadlocked until both writers were ordered by the
-- PK; it wants a quiet window and the 40P01 retry regardless. A true rebuild is TRUNCATE then this. A TRUNCATE is needed only to REMOVE rows (history itself was truncated, which
-- the trigger cannot observe) and must run as the table owner, since stl_readwrite has no TRUNCATE
-- grant. DO NOTHING would be wrong here: it would let a row inserted by the trigger during the rebuild
-- keep an OLDER observation than the one this statement computes, unrepairably.
-- REBUILD-BEGIN position_daily
SET LOCAL lock_timeout = '10s';
SET LOCAL timescaledb.enable_tiered_reads = 'on';
SET LOCAL search_path = public;

-- Both SETs live INSIDE the markers: the table COMMENT points operators at this region, the test
-- extracts exactly it, and SET LOCAL dies with its transaction -- so a region omitting them would drop
-- the tiered-read guarantee and the lock bound. The guard catches the copy-paste path, where psql sends
-- the statements one at a time, both SETs degrade to warnings the driver discards, and lock_timeout
-- stays 0.
DO $rebuild_guard$
BEGIN
    IF current_setting('lock_timeout') <> '10s' THEN
        RAISE EXCEPTION 'run the REBUILD region inside ONE transaction (BEGIN; ... COMMIT;): outside a transaction block both SET LOCALs are inert warnings, so this would run with no tiered-read guarantee and no lock bound';
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
