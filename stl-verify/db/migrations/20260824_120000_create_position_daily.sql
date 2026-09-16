-- position_daily (VEC-636): what a position held on a UTC date. The VIEW `position_daily` is the
-- answer, one row per (position, date). The table beneath it, position_daily_observation, is
-- append-only and crystallized once a day has closed, so it holds one row per (position, date) and
-- one more each time a correction changes a day that was already settled.

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
    is_retracted       boolean,
    -- This table's OWN correction counter, and the reason a retraction does not use
    -- processing_version: that column is the spine's to allocate, and a tombstone placed in it
    -- squats on the coordinate the spine's next correction crystallizes to.
    correction_seq     integer     NOT NULL DEFAULT 0,
    retraction_ticket  text,
    retraction_reason  text,
    -- A day's version is the coordinate of the observation that won it, copied from the spine, and
    -- the winner is the maximum of a set that only grows. correction_seq breaks ties within one
    -- coordinate, so a local correction outranks the row it corrects and nothing else.
    CONSTRAINT position_daily_observation_pkey PRIMARY KEY
        (position_id, as_of_date, block_number, block_version, processing_version, block_timestamp,
         correction_seq),
    -- A retraction carries its attribution and nothing else may, so a hand-written INSERT is held
    -- to the same rule as the procedure.
    CONSTRAINT position_daily_observation_retraction_attribution_chk
        CHECK ((is_retracted IS TRUE) = (retraction_ticket IS NOT NULL)
           AND (retraction_ticket IS NULL) = (retraction_reason IS NULL)
           AND (retraction_ticket IS NULL OR btrim(retraction_ticket) <> '')
           AND (retraction_reason IS NULL OR btrim(retraction_reason) <> '')),
    -- Pins the date derivation to one expression, so no row can land on a day its instant is not on.
    CONSTRAINT position_daily_observation_as_of_date_chk
        CHECK (as_of_date = (block_timestamp AT TIME ZONE 'utc')::date)
);

-- Plain, per AGENTS.md; converted if measurement says to. Crystallizing is what bounds this table --
-- one row per (position, date) rather than one per observation -- and a correction to a settled day
-- is the exception, so there is no append-only tail for compression or tiering to close behind.

COMMENT ON TABLE position_daily_observation IS '[Operational] Append-only log behind the position_daily view (VEC-636). Written by CALL crystallize_position_daily(): once a UTC day has closed, that day''s winning position_state observation is written here, so the normal shape is ONE row per (position, date). A correction or late observation that changes a settled day appends one more row carrying its own spine coordinate; nothing is ever updated or deleted, so every answer the table has given stays readable through position_daily_as_of(T). as_of_date is therefore not unique per position here -- read position_daily instead. A key that should never have existed is withdrawn the same way, by appending a row with is_retracted = TRUE, which the reads treat as absent (ADR-0006 §3, ARCT-470). The current UTC day is not crystallized; position_current answers "now". Point-in-time questions at block grain are answered from position_state.';
COMMENT ON COLUMN position_daily_observation.position_id IS 'Roles: PK. The bytea(32) native position identity from position_id() (VEC-400).';
COMMENT ON COLUMN position_daily_observation.as_of_date IS 'Roles: PK, Derived. UTC date of block_timestamp, pinned to it by a CHECK. NOT unique per position on this table: a day that has been corrected carries one row per answer it has had. Filter it on the position_daily view instead.';
COMMENT ON COLUMN position_daily_observation.chain_id IS 'Roles: Derived (copy of position_state.chain_id). NULL is a materializer convention for an off-chain source, not missing data.';
COMMENT ON COLUMN position_daily_observation.protocol_id IS 'Roles: Derived (copy of position_state.protocol_id). NULL only for an off-chain source, which has no protocol row.';
COMMENT ON COLUMN position_daily_observation.instrument_key IS 'Roles: Derived (copy of position_state.instrument_key). The instrument''s native, globally-unique id.';
COMMENT ON COLUMN position_daily_observation.holder_id IS 'Roles: Derived (copy of position_state.holder_id). Native on-chain holder, lowercase hex, no 0x.';
COMMENT ON COLUMN position_daily_observation.quantity IS 'Roles: Derived (copy of position_state.quantity). Native units; scale is source-defined and NOT normalized across projections. A zero is a real closing observation, not an absence.';
COMMENT ON COLUMN position_daily_observation.block_number IS 'Roles: PK, Derived. Block of the winning observation; the leading leg of the day''s ordering.';
COMMENT ON COLUMN position_daily_observation.block_version IS 'Roles: PK, Derived. Reorg version of that block (0 = original); second leg of the ordering.';
COMMENT ON COLUMN position_daily_observation.processing_version IS 'Roles: PK, Derived. The SOURCE''s correction version of that observation (0 = original, N = Nth reprocess); third leg. It versions one observation and never the day, so it is never read on its own here.';
COMMENT ON COLUMN position_daily_observation.block_timestamp IS 'Roles: PK, Derived. On-chain time of the winning observation; the last leg, so the pick is total.';
COMMENT ON COLUMN position_daily_observation.projection IS 'Roles: Audit. Which projection view wrote the observation. A superuser re-stamp of position_state.projection (see 20260818_130000) does NOT reach this copy, and no writer here can repair it.';
COMMENT ON COLUMN position_daily_observation.deal_type IS 'Roles: Derived (copy of position_state.deal_type). The deal type of the winning observation.';
COMMENT ON COLUMN position_daily_observation.build_id IS 'Roles: Audit. Which build wrote the observation (build_registry.id; 0 = pre-tracking).';
COMMENT ON COLUMN position_daily_observation.run_id IS 'Roles: Audit (copy of position_state.run_id). Which writer run appended the observation (writer_run.id; NULL means it predates run tracking).';
COMMENT ON COLUMN position_daily_observation.created_at IS 'Roles: Audit. When this row was crystallized; never rewritten. The as-of axis position_daily_as_of(T) filters on. It is TRANSACTION START time (now()) and a row becomes visible at COMMIT, so a T newer than the start of a still-running crystallization gains rows afterwards: a pinned T is stable once older than every run open at T. Processing time, not block time (see block_timestamp).';
COMMENT ON COLUMN position_daily_observation.is_retracted IS 'Roles: Audit. Retraction marker (ADR-0006 §3, ARCT-470): TRUE means this (position, date) key should never have existed, so position_daily and position_daily_as_of(T) treat the day as ABSENT rather than falling back to an older row. NULL and FALSE are live. Day-level: it withdraws the (position, date), and is NOT a copy of any spine retraction, which withdraws one observation and leaves the day to its next live one. Written only by a correction run, as a new row at the retracted row''s FULL spine coordinate -- processing_version included -- and correction_seq + 1; the crystallizer never sets it. A later spine observation at a higher coordinate revives the key, so a mis-keyed projection must be fixed upstream too. Raw reads of this table still return retracted rows, which is what keeps an earlier position_daily_as_of(T) reproducible.';
COMMENT ON COLUMN position_daily_observation.retraction_ticket IS 'Roles: Audit. The ticket a retraction was written under; NULL on every row that is not a retraction. Required by retract_position_daily, so no key is withdrawn without a record of who decided to.';
COMMENT ON COLUMN position_daily_observation.retraction_reason IS 'Roles: Audit. Why a retraction was written; NULL on every row that is not a retraction.';
COMMENT ON COLUMN position_daily_observation.correction_seq IS 'Roles: PK. This table''s own correction counter within one spine coordinate; 0 is every crystallized row. It exists because processing_version here is a COPY of the source''s count and is the spine''s to allocate: a retraction written at processing_version + 1 would occupy the primary key the spine''s own next correction crystallizes to, and the writer''s ON CONFLICT DO NOTHING would then drop that correction silently. Last leg of the ordering, so it breaks ties within a coordinate and never outranks a genuinely newer observation.';

-- The app role reads; only the owner writes, which is the crystallizer's caller. ALTER DEFAULT
-- PRIVILEGES (20260122_140100) hands every migrator-owned table full DML, so the REVOKE closes it.
GRANT SELECT ON position_daily_observation TO stl_readonly;
GRANT SELECT ON position_daily_observation TO stl_readwrite;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON position_daily_observation FROM stl_readwrite;

-- Append-only at the owner too, as position_state and sec_node are: the crystallizer only INSERTs,
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

-- The writer. It never inserts the observation that just arrived: it recomputes each settled day's
-- WINNER over the whole spine and offers that, so a late arrival that loses writes nothing and a
-- re-run writes nothing. That is what makes it idempotent and safe to overlap its own schedule.
--
-- settle_after buys quiet, not correctness: a day crystallized early is repaired by the next run
-- appending its new winner. Pins enable_tiered_reads because newest-per-day over the spine's local
-- chunks alone reads a PARTIAL history, and work_mem because the DISTINCT ON sorts it.
CREATE OR REPLACE PROCEDURE crystallize_position_daily(
    settle_after interval DEFAULT '1 hour',
    INOUT appended bigint DEFAULT NULL)
    LANGUAGE plpgsql
    SET search_path = pg_catalog, public
    SET timescaledb.enable_tiered_reads = 'on'
    SET lock_timeout = '10s'
    SET work_mem = '64MB'
AS $proc$
BEGIN
    INSERT INTO public.position_daily_observation
        (position_id, as_of_date, chain_id, protocol_id, instrument_key, holder_id, quantity,
         block_number, block_version, processing_version, block_timestamp, projection, build_id,
         run_id, deal_type)
    SELECT DISTINCT ON (p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date)
           p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date, p.chain_id, p.protocol_id,
           p.instrument_key, p.holder_id, p.quantity, p.block_number, p.block_version,
           p.processing_version, p.block_timestamp, p.projection, p.build_id, p.run_id, p.deal_type
    FROM public.position_state p
    -- Settled days only: the current UTC day is still gaining observations, and crystallizing it
    -- would append a row every time its answer moved during the day.
    WHERE (p.block_timestamp AT TIME ZONE 'utc')::date
          <= ((now() - settle_after) AT TIME ZONE 'utc')::date - 1
    ORDER BY p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date,
             p.block_number DESC, p.block_version DESC, p.processing_version DESC, p.block_timestamp DESC
    -- Named, so a unique or exclusion constraint added later cannot silently swallow rows here.
    ON CONFLICT ON CONSTRAINT position_daily_observation_pkey DO NOTHING;

    -- What this run actually wrote, so a scheduled caller can report progress instead of
    -- reporting that it ran. Zero is the normal result: nothing changed.
    GET DIAGNOSTICS appended = ROW_COUNT;
END;
$proc$;

COMMENT ON PROCEDURE crystallize_position_daily(interval, bigint) IS '[Operational] Writes each settled UTC day''s winning position_state observation into position_daily_observation (VEC-636): CALL crystallize_position_daily(). Returns the number of rows it wrote, normally zero. Insert-only and idempotent -- it offers the recomputed winner per (position, date), and conflicts do nothing, so a re-run and a late observation that loses both write nothing while a genuine change appends exactly one row. settle_after (default 1 hour) holds back days that have just closed; it reduces churn rather than buying correctness, since a later correction is picked up by the next run. What it cannot repair: a row whose spine source was re-stamped in place, and the as-of history of a window it never saw. Never writes is_retracted or correction_seq, and must never COPY is_retracted from the spine: a retracted spine row means that OBSERVATION never should have existed, so the day falls back to its next live observation, while a retracted row here means the whole (position, date) never should have existed. When position_state gains the column this procedure gains a filter, not a copied column. Pins enable_tiered_reads so the winner is computed over the whole spine, tiered chunks included.';

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

-- The read: the winning row per (position, date) among those crystallized by time T. STABLE, invoker
-- rights and no SET clause, so the planner inlines it into the caller's query rather than scanning.
--
-- holder_id joins position_id and as_of_date in the DISTINCT ON key. It does not change the grouping,
-- because position_id is sha256(chain;protocol;instrument;holder) and so determines it. It is there
-- because a qual on a non-key column cannot be pushed below a DISTINCT ON: without it a holder filter
-- became a post-filter over the whole table, measured at 1.9s against 34ms (VEC-636).
--
-- The retraction filter sits OUTSIDE the DISTINCT ON, so a retracted key is absent rather than
-- falling back to the older row it retracts. Inside, it would pick the newest LIVE row and answer
-- with a reading the correction withdrew.
CREATE OR REPLACE FUNCTION position_daily_as_of(seen_before timestamptz)
    RETURNS SETOF position_daily_observation
    LANGUAGE sql STABLE
AS $fn$
    SELECT w.* FROM (
        SELECT DISTINCT ON (d.position_id, d.as_of_date, d.holder_id) d.*
          FROM public.position_daily_observation d
         WHERE d.created_at <= public.position_daily_as_of_bound(seen_before)
         ORDER BY d.position_id, d.as_of_date, d.holder_id,
                  d.block_number DESC, d.block_version DESC, d.processing_version DESC,
                  d.block_timestamp DESC, d.correction_seq DESC
    ) w
    WHERE w.is_retracted IS NOT TRUE;
$fn$;

COMMENT ON FUNCTION position_daily_as_of(timestamptz) IS '[Operational] position_daily as it read at time T: the winning row per (position, UTC date) among those crystallized by T (VEC-636). Raises on a NULL T. A report that records its own run time can reconstruct what it saw, subject to the window created_at''s COMMENT records: T is stable once older than every crystallization open at T. A key whose winning row is retracted is absent, not replaced by the row it retracted. position_daily_as_of(''infinity'') is the current answer, which the position_daily view wraps.';

CREATE OR REPLACE VIEW position_daily AS
    SELECT * FROM public.position_daily_as_of('infinity'::timestamptz);

COMMENT ON VIEW position_daily IS '[Operational] What each position held on each observed UTC date: one row per (position, UTC date), that day''s winning observation (VEC-636). Equal to the newest position_state observation per (position, UTC date) across settled days, except where a retraction withdraws the key. Only OBSERVED dates get a row -- no carry-forward, so a query for one date may correctly return nothing, and the current UTC day is absent until it is crystallized. A retracted key is absent entirely. Not reproducible across corrections; pin a time with position_daily_as_of(T) for that.';

GRANT SELECT ON position_daily TO stl_readonly;
GRANT SELECT ON position_daily TO stl_readwrite;

INSERT INTO public.migrations (filename) VALUES ('20260824_120000_create_position_daily.sql') ON CONFLICT (filename) DO NOTHING;
