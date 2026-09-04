-- position_current (VEC-409): one row per position, its latest observation. A derived cache of the
-- position_state history -- it answers "now", never "as of block N". Limits, ordering consequences and
-- the measurements behind the trigger shape are in the table COMMENT and on the PR.

-- Bounds the wait for CREATE TRIGGER's SHARE ROW EXCLUSIVE on position_state, which every ingest
-- INSERT conflicts with. Never mark this file `migrate: no-transaction`: SET LOCAL would be inert.
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

    -- position_state's guards, because stl_readwrite can INSERT here directly, not only through the
    -- trigger. NaN needs both clauses: NaN >= 0 is TRUE in numeric ordering, and `< 'Infinity'`
    -- is what rejects it.
    CONSTRAINT position_current_id_len_chk CHECK (octet_length(position_id) = 32),
    CONSTRAINT position_current_qty_nonneg_chk CHECK (quantity >= 0 AND quantity <> 'NaN'::numeric AND quantity < 'Infinity'::numeric),
    -- A 20-byte address as 40 lowercase hex characters. Unanchored, it also admits a decimal
    -- surrogate rendered as text.
    CONSTRAINT position_current_holder_hex_chk CHECK (holder_id ~ '^[0-9a-f]{40}$'),
    -- instrument_key feeds the position_id hash and is copied into every derived cache. No legitimate
    -- source form approaches this: an address is 40 hex characters, a composite two of those. A
    -- sanity bound on an unbounded text column, not a limit derived from any one consumer.
    CONSTRAINT position_current_instrument_key_len_chk CHECK (char_length(instrument_key) <= 2000),
    CONSTRAINT position_current_coord_nonneg_chk CHECK (block_number >= 0 AND block_version >= 0 AND processing_version >= 0 AND build_id >= 0),
    CONSTRAINT position_current_chain_pos_chk CHECK (chain_id IS NULL OR chain_id > 0),
    CONSTRAINT position_current_protocol_pos_chk CHECK (protocol_id IS NULL OR protocol_id > 0),
    CONSTRAINT position_current_ts_sane_chk CHECK (block_timestamp >= '2009-01-03 00:00:00+00'::timestamptz),
    -- Both writers derive as_of_date from block_timestamp; this pins them together rather than
    -- trusting them to agree. timezone(text, timestamptz) is IMMUTABLE, so it is CHECK-legal.
    CONSTRAINT position_current_as_of_date_chk CHECK (as_of_date = (block_timestamp AT TIME ZONE 'utc')::date)
);

COMMENT ON TABLE position_current IS '[Operational] One row per position: its newest observation by (block_number, block_version, processing_version, block_timestamp) (VEC-409). Derived cache of position_state, maintained by trigger_upsert_position_current. Ordering is block_number FIRST, so a reorg replacement at a LOWER block does not displace the orphan it supersedes. Re-running the REBUILD region in 20260819_150100 is a FORWARD-ONLY merge: a row ahead of history and an orphan row do not converge, and a cache-only UPDATE self-reverts -- correct a wrong value by appending a spine row. Point-in-time questions are answered from position_state.';
COMMENT ON COLUMN position_current.position_id IS 'PK. The bytea(32) native position identity from position_id() (VEC-400).';
COMMENT ON COLUMN position_current.as_of_date IS 'Derived. Date of the LAST OBSERVATION, not a snapshot date: never equality-filter or date-join on it. UTC, never CURRENT_DATE. Chain day for chain projections, sync day where an event-time source stamps synced_at, so a date-keyed join mixes calendars.';
COMMENT ON COLUMN position_current.chain_id IS 'Derived (copy of position_state.chain_id). NULL is a materializer convention, not missing data, so a filtered or grouped total will not reconcile against the unfiltered sum.';
COMMENT ON COLUMN position_current.protocol_id IS 'Derived (copy of position_state.protocol_id). NULL is a materializer convention, not missing data; same reconciliation caveat as chain_id.';
COMMENT ON COLUMN position_current.instrument_key IS 'Derived (copy of position_state.instrument_key). The instrument''s native, globally-unique id.';
COMMENT ON COLUMN position_current.holder_id IS 'Derived (copy of position_state.holder_id). Native on-chain holder, lowercase hex, no 0x.';
COMMENT ON COLUMN position_current.quantity IS 'Derived (copy of position_state.quantity). Native units; scale is source-defined and NOT normalized across protocols, so do not SUM across heterogeneous instruments. The LAST observation, not a live value: a position that stops being observed keeps its quantity indefinitely, and only as_of_date age signals that. A zero is a real observation of an emptied position, not a gap.';
COMMENT ON COLUMN position_current.block_number IS 'Derived. Block of the latest observation; part of the newer-wins comparison.';
COMMENT ON COLUMN position_current.block_version IS 'Derived. Reorg version of that block (0 = original); part of the newer-wins comparison.';
COMMENT ON COLUMN position_current.processing_version IS 'Derived. Correction version of that row (0 = original, N = Nth reprocess); part of the newer-wins comparison.';
COMMENT ON COLUMN position_current.block_timestamp IS 'Derived. On-chain time of the latest observation; the last leg of the newer-wins comparison, so the pick is total when two rows share the other four key columns.';
COMMENT ON COLUMN position_current.projection IS 'Audit. Which projection view wrote the latest observation.';
COMMENT ON COLUMN position_current.build_id IS 'Audit. Which build wrote the latest observation (build_registry.id; 0 = pre-tracking).';

-- Outside the strict append-only rule by recorded exception (AGENTS.md here): a derived cache holding
-- UPDATE and taking ON CONFLICT DO UPDATE. That arm is checked at executor start, so UPDATE here is a
-- precondition for INSERT into position_state -- a narrower materializer role (VEC-562) needs both.

GRANT SELECT ON position_current TO stl_readonly;
GRANT SELECT, INSERT, UPDATE ON position_current TO stl_readwrite;
-- ALTER DEFAULT PRIVILEGES (20260122_140100) already granted DELETE, so the narrowed GRANT above is a
-- no-op and only this REVOKE closes it. The suite asserts the ACL from the catalogue, not the GRANT
-- list, because the two disagree. TRUNCATE is not in the default grant and stays owner-only.
REVOKE DELETE ON position_current FROM stl_readwrite;

CREATE OR REPLACE FUNCTION upsert_position_current() RETURNS trigger
    LANGUAGE plpgsql
    -- Explicit, not FROM CURRENT, which would bake in whatever session applied the migration.
    SET search_path = pg_catalog, public
AS $fn$
BEGIN
    INSERT INTO public.position_current AS cur
        (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
         block_number, block_version, processing_version, block_timestamp, projection, build_id)
        -- One upsert per STATEMENT over the transition table, ordered by position_id: the only total
        -- order a block_timestamp cannot permute, so this cannot cross the rebuild's lock order. NEW
        -- is unavailable in a statement trigger. Deadlock and cost measurements in #644.
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

COMMENT ON FUNCTION upsert_position_current() IS '[Operational] Keeps position_current at the latest observation per position (VEC-409). AFTER INSERT on position_state, once per STATEMENT over its transition table, ordered by position_id so the rebuild cannot cross it.';

-- Precondition for the REBUILD region in 20260819_150100, called from inside that INSERT and
-- standalone. An xid rather than a sentinel: ALTER ROLE can pre-seed a custom GUC, not an xid.
-- VOLATILE because the body assigns an xid. Branch reasoning and measurements in #644.
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

-- KNOWN GAP: TimescaleDB refuses to enable or disable a trigger on a hypertable, so this one stays at
-- ORIGIN and does not fire under session_replication_role = 'replica' (pg_restore
-- --disable-triggers). Recovery is the REBUILD region in 20260819_150100. Measured in #644.

INSERT INTO migrations (filename) VALUES ('20260819_150000_create_position_current.sql') ON CONFLICT (filename) DO NOTHING;
