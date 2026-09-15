-- position_daily (VEC-636): append-only, one row per (position, UTC date) per batch that observed it --
-- that batch's newest observation for the day. "What did it hold on D" is the newest row per (position,
-- date), position_daily_latest; "what did we say it held on D at time T" is position_daily_as_of(T).

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
    created_at         timestamptz NOT NULL DEFAULT now(),
    -- The spine's coordinate with as_of_date ahead of it, so a (position, date) read is a prefix scan.
    CONSTRAINT position_daily_pkey PRIMARY KEY
        (position_id, as_of_date, block_number, block_version, processing_version, block_timestamp),
    -- Pins both writers' date derivation to one expression, so they cannot disagree about the day.
    CONSTRAINT position_daily_as_of_date_chk CHECK (as_of_date = (block_timestamp AT TIME ZONE 'utc')::date)
);

COMMENT ON TABLE position_daily IS '[Operational] Append-only. One row per (position, UTC date) per ingest batch that observed the position that day: the batch''s newest observation (VEC-636). The day''s answer is the newest row per (position, date) by (block_number, block_version, processing_version, block_timestamp); read it through position_daily_latest, or position_daily_as_of(T) for the answer as it stood at time T. Only OBSERVED dates get rows -- no carry-forward. Nothing updates or deletes here: a correction is a new row, and CALL rebuild_position_daily() only adds rows the trigger missed. Plain, not a hypertable: rows per (position, date) are bounded by the batches that touched it, never exceeding position_state. Point-in-time questions at block grain are answered from position_state.';
COMMENT ON COLUMN position_daily.position_id IS 'Roles: PK. The bytea(32) native position identity from position_id() (VEC-400).';
COMMENT ON COLUMN position_daily.as_of_date IS 'Roles: PK. UTC date of block_timestamp, pinned to it by a CHECK.';
COMMENT ON COLUMN position_daily.chain_id IS 'Roles: Derived (copy of position_state.chain_id). NULL is a materializer convention for an off-chain source, not missing data.';
COMMENT ON COLUMN position_daily.protocol_id IS 'Roles: Derived (copy of position_state.protocol_id). NULL only for an off-chain source, which has no protocol row.';
COMMENT ON COLUMN position_daily.instrument_key IS 'Roles: Derived (copy of position_state.instrument_key). The instrument''s native, globally-unique id.';
COMMENT ON COLUMN position_daily.holder_id IS 'Roles: Derived (copy of position_state.holder_id). Native on-chain holder, lowercase hex, no 0x.';
COMMENT ON COLUMN position_daily.quantity IS 'Roles: Derived (copy of position_state.quantity). Native units at this observation; a zero is a real closing observation, not an absence.';
COMMENT ON COLUMN position_daily.block_number IS 'Roles: PK, Derived. Block of the observation; the leading leg of the newest-per-day ordering.';
COMMENT ON COLUMN position_daily.block_version IS 'Roles: PK, Derived. Reorg version of that block (0 = original); second leg of the ordering.';
COMMENT ON COLUMN position_daily.processing_version IS 'Roles: PK, Derived. Correction version of that row (0 = original, N = Nth reprocess); third leg of the ordering.';
COMMENT ON COLUMN position_daily.block_timestamp IS 'Roles: PK, Derived. On-chain time of the observation; the last leg of the ordering, so the pick is total.';
COMMENT ON COLUMN position_daily.projection IS 'Roles: Audit. Which projection view wrote the observation.';
COMMENT ON COLUMN position_daily.deal_type IS 'Roles: Derived (copy of position_state.deal_type). The deal type of this observation.';
COMMENT ON COLUMN position_daily.build_id IS 'Roles: Audit. Which build wrote the observation (build_registry.id; 0 = pre-tracking).';
COMMENT ON COLUMN position_daily.run_id IS 'Roles: Audit (copy of position_state.run_id). Which writer run appended the observation (writer_run.id; NULL means it predates run tracking).';
COMMENT ON COLUMN position_daily.created_at IS 'Roles: Audit. When this row was appended; never rewritten. The reproducibility axis: position_daily_as_of(T) answers from rows with created_at <= T. Processing time, not block time (see block_timestamp).';

-- Trigger-only, like position_current: the SECURITY DEFINER maintainer inserts, the app role reads.
-- ALTER DEFAULT PRIVILEGES (20260122_140100) hands every migrator-owned table full DML, so the REVOKE closes it.
GRANT SELECT ON position_daily TO stl_readonly;
GRANT SELECT ON position_daily TO stl_readwrite;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON position_daily FROM stl_readwrite;

-- Append-only at the owner too, as position_state and sec_node are: the writers below only INSERT, and
-- nothing FKs this table, so no integrity probe needs the owner's UPDATE. Derived from relowner so it
-- lands whatever the role is called; recorded but not enforced where the owner is a superuser.
DO $$
DECLARE
    owner_role text;
    owner_is_super boolean;
BEGIN
    SELECT pg_get_userbyid(c.relowner) INTO owner_role FROM pg_class c WHERE c.oid = 'position_daily'::regclass;
    SELECT rolsuper INTO owner_is_super FROM pg_roles WHERE rolname = owner_role;
    EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON position_daily FROM %I', owner_role);
    IF NOT owner_is_super AND has_table_privilege(owner_role, 'position_daily', 'UPDATE') THEN
        RAISE EXCEPTION 'append-only not enforced: owner % still holds UPDATE on position_daily after the revoke', owner_role;
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
    INSERT INTO public.position_daily
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
    ON CONFLICT DO NOTHING;
    RETURN NULL;
END;
$fn$;

COMMENT ON FUNCTION append_position_daily() IS '[Operational] Appends each batch''s newest observation per (position, UTC date) to position_daily (VEC-636). AFTER INSERT FOR EACH STATEMENT on position_state over the transition table; ON CONFLICT DO NOTHING, never UPDATE. SECURITY DEFINER: the appending role holds no write grant on the table.';

-- The read: newest row per (position, date) among those appended by time T. STABLE, invoker rights and
-- no SET clause, so the planner inlines it into the caller's query instead of running it as a scan.
CREATE OR REPLACE FUNCTION position_daily_as_of(seen_before timestamptz)
    RETURNS SETOF position_daily
    LANGUAGE sql STABLE
AS $fn$
    SELECT DISTINCT ON (d.position_id, d.as_of_date) d.*
      FROM public.position_daily d
     WHERE d.created_at <= seen_before
     ORDER BY d.position_id, d.as_of_date,
              d.block_number DESC, d.block_version DESC, d.processing_version DESC, d.block_timestamp DESC;
$fn$;

COMMENT ON FUNCTION position_daily_as_of(timestamptz) IS '[Operational] position_daily as it read at time T: the newest row per (position, UTC date) among rows with created_at <= T (VEC-636). A query pinned to a T it records is reproducible: later appends are invisible to it. position_daily_as_of(''infinity'') is the current answer, which position_daily_latest wraps.';

CREATE OR REPLACE VIEW position_daily_latest AS
    SELECT * FROM public.position_daily_as_of('infinity'::timestamptz);

COMMENT ON VIEW position_daily_latest IS '[Operational] What each position held on each observed UTC date: the newest position_daily row per (position, date) (VEC-636). Equal to the newest position_state observation per (position, UTC date). Not reproducible across appends; pin a time with position_daily_as_of(T) for that.';

GRANT SELECT ON position_daily_latest TO stl_readonly;
GRANT SELECT ON position_daily_latest TO stl_readwrite;

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
    INSERT INTO public.position_daily
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
    ON CONFLICT DO NOTHING;
$proc$;

COMMENT ON PROCEDURE rebuild_position_daily() IS '[Operational] Appends to position_daily every (position, UTC date) winner in position_state that it lacks (VEC-636): CALL rebuild_position_daily(). Insert-only and idempotent; it never rewrites or removes a row, so rows already present keep their created_at and the as-of reading stays honest. Pins enable_tiered_reads so the winner is computed over the whole spine, tiered chunks included. Requires a quiet window on position_state.';

-- Guarded like every other DDL statement here, so a re-run does not fail with "trigger already exists".
DROP TRIGGER IF EXISTS trigger_append_position_daily ON position_state;
CREATE TRIGGER trigger_append_position_daily
    AFTER INSERT ON position_state
    REFERENCING NEW TABLE AS newrows
    FOR EACH STATEMENT
EXECUTE FUNCTION append_position_daily();

-- KNOWN GAP, as on position_current (20260819_150000): TimescaleDB refuses ENABLE ALWAYS on a hypertable
-- trigger, so this stays at ORIGIN and does not fire under session_replication_role = 'replica'
-- (pg_restore --disable-triggers). CALL rebuild_position_daily() appends what the bypass skipped.

-- The backfill, both indexes and the ANALYZE are in 20260824_120100, as 20260819_150100 established:
-- the migrator runs a file in one transaction, so here they would run under the lock CREATE TRIGGER
-- takes on position_state -- ACCESS EXCLUSIVE on a re-run, when the DROP above finds a trigger.

INSERT INTO public.migrations (filename) VALUES ('20260824_120000_create_position_daily.sql') ON CONFLICT (filename) DO NOTHING;
