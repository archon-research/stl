-- position_daily (VEC-636): what a position held on a UTC date. The VIEW `position_daily` is the
-- answer, one row per (position, date); the table beneath it, position_daily_observation, is
-- append-only and holds one row per batch that observed the position that day.

-- Bounds the wait for CREATE TRIGGER's SHARE ROW EXCLUSIVE on position_state, which every ingest
-- INSERT conflicts with. Never mark this file `migrate: no-transaction`: SET LOCAL would be inert.
SET LOCAL lock_timeout = '10s';

CREATE TABLE IF NOT EXISTS position_daily_observation (
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
    created_at         timestamptz NOT NULL DEFAULT now(),
    -- position_state's coordinate with as_of_date ahead of it, so a (position, date) read is a
    -- prefix scan. The two writers copy that coordinate, so this is exactly as unique as the spine.
    CONSTRAINT position_daily_observation_pkey PRIMARY KEY
        (position_id, as_of_date, block_number, block_version, processing_version, block_timestamp),
    -- Pins both writers' date derivation to one expression, so they cannot disagree about the day.
    CONSTRAINT position_daily_observation_as_of_date_chk
        CHECK (as_of_date = (block_timestamp AT TIME ZONE 'utc')::date)
);

COMMENT ON TABLE position_daily_observation IS '[Operational] Append-only observation log behind the position_daily view (VEC-636). One row per (position, UTC date) PER ingest batch that observed the position that day, so as_of_date is NOT unique per position here -- read position_daily, or position_daily_as_of(T), rather than this table. Nothing updates or deletes: a correction is a new row, and CALL rebuild_position_daily() only adds rows the trigger missed. Row count cannot exceed position_state, whose coordinate every row copies; bytes can, since this table is plain with no compression or tiering and the spine has both. Point-in-time questions at block grain are answered from position_state.';
COMMENT ON COLUMN position_daily_observation.position_id IS 'Roles: PK. The bytea(32) native position identity from position_id() (VEC-400).';
COMMENT ON COLUMN position_daily_observation.as_of_date IS 'Roles: PK, Derived. UTC date of block_timestamp, pinned to it by a CHECK. NOT unique per position on this table: as_of_date = D here returns one row per batch that observed the position that day. Filter it on the position_daily view instead.';
COMMENT ON COLUMN position_daily_observation.chain_id IS 'Roles: Derived (copy of position_state.chain_id). NULL is a materializer convention for an off-chain source, not missing data.';
COMMENT ON COLUMN position_daily_observation.protocol_id IS 'Roles: Derived (copy of position_state.protocol_id). NULL only for an off-chain source, which has no protocol row.';
COMMENT ON COLUMN position_daily_observation.instrument_key IS 'Roles: Derived (copy of position_state.instrument_key). The instrument''s native, globally-unique id.';
COMMENT ON COLUMN position_daily_observation.holder_id IS 'Roles: Derived (copy of position_state.holder_id). Native on-chain holder, lowercase hex, no 0x.';
COMMENT ON COLUMN position_daily_observation.quantity IS 'Roles: Derived (copy of position_state.quantity). Native units; scale is source-defined and NOT normalized across projections. A zero is a real closing observation, not an absence.';
COMMENT ON COLUMN position_daily_observation.block_number IS 'Roles: PK, Derived. Block of the observation; the leading leg of the newest-per-day ordering.';
COMMENT ON COLUMN position_daily_observation.block_version IS 'Roles: PK, Derived. Reorg version of that block (0 = original); second leg of the ordering.';
COMMENT ON COLUMN position_daily_observation.processing_version IS 'Roles: PK, Derived. Correction version of that row (0 = original, N = Nth reprocess); third leg of the ordering.';
COMMENT ON COLUMN position_daily_observation.block_timestamp IS 'Roles: PK, Derived. On-chain time of the observation; the last leg of the ordering, so the pick is total.';
COMMENT ON COLUMN position_daily_observation.projection IS 'Roles: Audit. Which projection view wrote the observation. A superuser re-stamp of position_state.projection (see 20260818_130000) does NOT reach this copy, and no writer here can repair it.';
COMMENT ON COLUMN position_daily_observation.deal_type IS 'Roles: Derived (copy of position_state.deal_type). The deal type of this observation.';
COMMENT ON COLUMN position_daily_observation.build_id IS 'Roles: Audit. Which build wrote the observation (build_registry.id; 0 = pre-tracking).';
COMMENT ON COLUMN position_daily_observation.run_id IS 'Roles: Audit (copy of position_state.run_id). Which writer run appended the observation (writer_run.id; NULL means it predates run tracking).';
COMMENT ON COLUMN position_daily_observation.created_at IS 'Roles: Audit. When this row was appended; never rewritten. The as-of axis position_daily_as_of(T) filters on. It is TRANSACTION START time (now()), and a row becomes visible at COMMIT, so a T newer than the start of a still-open writer gains rows later: a pinned T is only stable once older than every writer transaction that was open at T. Processing time, not block time (see block_timestamp).';

-- Trigger-only, like position_current: the SECURITY DEFINER maintainer inserts, the app role reads.
-- ALTER DEFAULT PRIVILEGES (20260122_140100) hands every migrator-owned table full DML, so the REVOKE closes it.
GRANT SELECT ON position_daily_observation TO stl_readonly;
GRANT SELECT ON position_daily_observation TO stl_readwrite;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON position_daily_observation FROM stl_readwrite;

-- Append-only at the owner too, as position_state and sec_node are: the writers below only INSERT,
-- and nothing FKs this table, so no integrity probe needs the owner's UPDATE. Derived from relowner
-- so it lands whatever the role is called; recorded but not enforced where the owner is a superuser.
DO $$
DECLARE
    owner_role text;
    owner_is_super boolean;
BEGIN
    SELECT pg_get_userbyid(c.relowner) INTO owner_role
      FROM pg_class c WHERE c.oid = 'position_daily_observation'::regclass;
    SELECT rolsuper INTO owner_is_super FROM pg_roles WHERE rolname = owner_role;
    EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON position_daily_observation FROM %I', owner_role);
    IF NOT owner_is_super AND has_table_privilege(owner_role, 'position_daily_observation', 'UPDATE') THEN
        RAISE EXCEPTION 'append-only not enforced: owner % still holds UPDATE on position_daily_observation after the revoke', owner_role;
    END IF;
END $$;

-- SECURITY DEFINER so the append runs as the owner: the role appending to position_state holds no
-- write grant here. search_path is then mandatory, so a caller's path cannot bind these names.
CREATE OR REPLACE FUNCTION append_position_daily() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path = pg_catalog, public
AS $fn$
BEGIN
    -- One row per (position, date) per STATEMENT: the batch's newest observation for that day. The
    -- day's overall newest is the newest of some batch, so it is always among the appended rows.
    -- Ordered by this table's key, so this writer and the rebuild insert in one direction and cannot
    -- deadlock against each other; a FOR EACH ROW trigger would follow the writer's insertion order.
    INSERT INTO public.position_daily_observation
        (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
         block_number, block_version, processing_version, block_timestamp, projection, build_id,
         run_id, deal_type)
    SELECT DISTINCT ON (n.position_id, (n.block_timestamp AT TIME ZONE 'utc')::date)
           n.position_id, (n.block_timestamp AT TIME ZONE 'utc')::date, n.chain_id, n.protocol_id,
           n.instrument_key, n.holder_id, n.quantity, n.block_number, n.block_version,
           n.processing_version, n.block_timestamp, n.projection, n.build_id, n.run_id, n.deal_type
    FROM newrows n
    ORDER BY n.position_id, (n.block_timestamp AT TIME ZONE 'utc')::date,
             n.block_number DESC, n.block_version DESC, n.processing_version DESC, n.block_timestamp DESC
    -- Named, so a unique or exclusion constraint added later cannot silently swallow rows here.
    ON CONFLICT ON CONSTRAINT position_daily_observation_pkey DO NOTHING;
    RETURN NULL;
END;
$fn$;

COMMENT ON FUNCTION append_position_daily() IS '[Operational] Appends each batch''s newest observation per (position, UTC date) to position_daily_observation (VEC-636). AFTER INSERT FOR EACH STATEMENT on position_state over the transition table; DO NOTHING on the PK, never UPDATE. SECURITY DEFINER: the appending role holds no write grant on the table.';

-- A NULL bound would make every created_at <= NULL comparison NULL, so the read would return an empty
-- set and a caller with an unset timestamp would read "held nothing" as an answer. Raise instead.
CREATE OR REPLACE FUNCTION position_daily_as_of_bound(seen_before timestamptz) RETURNS timestamptz
    LANGUAGE plpgsql IMMUTABLE
    SET search_path = pg_catalog, public
AS $fn$
BEGIN
    IF seen_before IS NULL THEN
        RAISE EXCEPTION 'position_daily_as_of: the as-of time is required; a NULL bound would return an empty series rather than an answer';
    END IF;
    RETURN seen_before;
END;
$fn$;

COMMENT ON FUNCTION position_daily_as_of_bound(timestamptz) IS '[Operational] Returns its argument, raising on NULL (VEC-636). Guards position_daily_as_of, whose created_at filter would otherwise turn a NULL bound into a silent empty result. IMMUTABLE so a constant bound folds at plan time and the guarded read still inlines.';

-- The read: newest row per (position, date) among those appended by time T. STABLE, invoker rights and
-- no SET clause, so the planner inlines it into the caller's query instead of running it as a scan.
--
-- holder_id joins position_id and as_of_date in the DISTINCT ON key. It does not change the grouping,
-- because position_id is sha256(chain;protocol;instrument;holder) and so determines it. It is there
-- because a qual on a non-key column cannot be pushed below a DISTINCT ON: without it a holder filter
-- became a post-filter over the whole table, measured at 1.9s against 34ms (VEC-636).
CREATE OR REPLACE FUNCTION position_daily_as_of(seen_before timestamptz)
    RETURNS SETOF position_daily_observation
    LANGUAGE sql STABLE
AS $fn$
    SELECT DISTINCT ON (d.position_id, d.as_of_date, d.holder_id) d.*
      FROM public.position_daily_observation d
     WHERE d.created_at <= public.position_daily_as_of_bound(seen_before)
     ORDER BY d.position_id, d.as_of_date, d.holder_id,
              d.block_number DESC, d.block_version DESC, d.processing_version DESC, d.block_timestamp DESC;
$fn$;

COMMENT ON FUNCTION position_daily_as_of(timestamptz) IS '[Operational] position_daily as it read at time T: the newest observation per (position, UTC date) among rows appended by T (VEC-636). Raises on a NULL T. A report that records its own run time can reconstruct what it saw, subject to the window created_at''s COMMENT records: T is only stable once older than every writer transaction open at T. position_daily_as_of(''infinity'') is the current answer, which the position_daily view wraps.';

CREATE OR REPLACE VIEW position_daily AS
    SELECT * FROM public.position_daily_as_of('infinity'::timestamptz);

COMMENT ON VIEW position_daily IS '[Operational] What each position held on each observed UTC date: one row per (position, UTC date), the newest observation for that day (VEC-636). Equal to the newest position_state observation per (position, UTC date). Only OBSERVED dates get a row -- no carry-forward, so a query for one date may correctly return nothing. This is the read; position_daily_observation beneath it is the append-only log and holds a row per ingest batch. Not reproducible across appends; pin a time with position_daily_as_of(T) for that.';

GRANT SELECT ON position_daily TO stl_readonly;
GRANT SELECT ON position_daily TO stl_readwrite;

-- The repair an operator re-runs, as a procedure so its settings cannot be forgotten or stepped over:
-- enable_tiered_reads because newest-per-day over position_state's local chunks alone reads a PARTIAL
-- spine, work_mem because the DISTINCT ON sorts the whole of it. Adds rows only; never rewrites one.
CREATE OR REPLACE PROCEDURE rebuild_position_daily()
    LANGUAGE sql
    SET search_path = pg_catalog, public
    SET timescaledb.enable_tiered_reads = 'on'
    SET lock_timeout = '10s'
    SET work_mem = '64MB'
AS $proc$
    INSERT INTO public.position_daily_observation
        (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
         block_number, block_version, processing_version, block_timestamp, projection, build_id,
         run_id, deal_type)
    SELECT DISTINCT ON (p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date)
           p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date, p.chain_id, p.protocol_id,
           p.instrument_key, p.holder_id, p.quantity, p.block_number, p.block_version,
           p.processing_version, p.block_timestamp, p.projection, p.build_id, p.run_id, p.deal_type
    FROM public.position_state p
    ORDER BY p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date,
             p.block_number DESC, p.block_version DESC, p.processing_version DESC, p.block_timestamp DESC
    ON CONFLICT ON CONSTRAINT position_daily_observation_pkey DO NOTHING;
$proc$;

COMMENT ON PROCEDURE rebuild_position_daily() IS '[Operational] Appends to position_daily_observation every (position, UTC date) winner in position_state that it lacks (VEC-636): CALL rebuild_position_daily(). Insert-only and idempotent; rows already present keep their created_at, so the as-of reading stays honest. What it repairs is the CURRENT reading. What it cannot: the as-of history of a skipped window, since the per-batch rows were never written and the row it adds carries the rebuild''s created_at, so position_daily_as_of(T) for a T inside that window stays wrong; and a row whose spine source was re-stamped in place, which no writer here can reach. Pins enable_tiered_reads so the winner is computed over the whole spine, tiered chunks included. Requires a quiet window on position_state.';

-- Guarded like every other DDL statement here, so a re-run does not fail with "trigger already exists".
DROP TRIGGER IF EXISTS trigger_append_position_daily ON position_state;
CREATE TRIGGER trigger_append_position_daily
    AFTER INSERT ON position_state
    REFERENCING NEW TABLE AS newrows
    FOR EACH STATEMENT
EXECUTE FUNCTION append_position_daily();

-- KNOWN GAP, as on position_current (20260819_150000): TimescaleDB refuses ENABLE ALWAYS on a
-- hypertable trigger, so this stays at ORIGIN and does not fire under session_replication_role =
-- 'replica' (pg_restore --disable-triggers). The rebuild's COMMENT states what that leaves unrepaired.

-- The backfill, both indexes and the ANALYZE are in 20260824_120100, as 20260819_150100 established:
-- the migrator runs a file in one transaction, so here they would run under the lock CREATE TRIGGER
-- takes on position_state -- ACCESS EXCLUSIVE on a re-run, when the DROP above finds a trigger.

INSERT INTO public.migrations (filename) VALUES ('20260824_120000_create_position_daily.sql') ON CONFLICT (filename) DO NOTHING;
