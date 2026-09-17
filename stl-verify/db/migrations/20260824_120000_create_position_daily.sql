-- position_daily (VEC-636): what a position held on a UTC date, read straight from position_state.
-- Nothing is copied: the spine is chunked by day and ordered by the version tuple, and its own
-- created_at answers "as of T". Materialization waits on a measured slow read.

-- A NULL bound makes every created_at <= NULL comparison NULL, so the read would return an empty set
-- that a caller with an unset timestamp reads as "held nothing". Raise instead.
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

COMMENT ON FUNCTION position_daily_as_of_bound(timestamptz) IS '[Operational] Returns its argument, raising on NULL (VEC-636). Guards the position_daily reads, whose created_at filter would otherwise turn a NULL bound into a silent empty result. IMMUTABLE so a constant bound folds at plan time and the guarded read still inlines.';

-- The same guard for the date, IMMUTABLE so a constant date folds and the window excludes chunks at plan time.
CREATE OR REPLACE FUNCTION position_daily_date_required(d date) RETURNS date
    LANGUAGE plpgsql IMMUTABLE
    SET search_path = pg_catalog, public
AS $fn$
BEGIN
    IF d IS NULL THEN
        RAISE EXCEPTION 'position_daily_on: the date is required; a NULL date would return an empty book rather than an answer';
    END IF;
    RETURN d;
END;
$fn$;

COMMENT ON FUNCTION position_daily_date_required(date) IS '[Operational] Returns its argument, raising on NULL (VEC-636). Guards position_daily_on, whose block_timestamp window would otherwise be NULL and return nothing.';

-- One date. The window is on block_timestamp itself, the partition column, so it opens one chunk; a
-- predicate on (block_timestamp AT TIME ZONE 'utc')::date opens every chunk. No SET clause, so it inlines.
CREATE OR REPLACE FUNCTION position_daily_on(d date, seen_before timestamptz DEFAULT 'infinity')
    RETURNS TABLE (
        position_id        bytea,
        as_of_date         date,
        chain_id           integer,
        protocol_id        bigint,
        instrument_key     text,
        holder_id          text,
        quantity           numeric,
        block_number       bigint,
        block_version      integer,
        processing_version integer,
        block_timestamp    timestamptz,
        projection         text,
        build_id           integer,
        created_at         timestamptz,
        deal_type          text,
        run_id             bigint)
    LANGUAGE sql STABLE
AS $fn$
    SELECT DISTINCT ON (p.position_id)
           p.position_id, d, p.chain_id, p.protocol_id, p.instrument_key, p.holder_id, p.quantity,
           p.block_number, p.block_version, p.processing_version, p.block_timestamp, p.projection,
           p.build_id, p.created_at, p.deal_type, p.run_id
      FROM public.position_state p
     WHERE p.block_timestamp >= timezone('utc', public.position_daily_date_required(d)::timestamp)
       AND p.block_timestamp <  timezone('utc', (public.position_daily_date_required(d) + 1)::timestamp)
       AND p.created_at <= public.position_daily_as_of_bound(seen_before)
     ORDER BY p.position_id, p.block_number DESC, p.block_version DESC, p.processing_version DESC,
              p.block_timestamp DESC;
$fn$;

COMMENT ON FUNCTION position_daily_on(date, timestamptz) IS '[Operational] The whole book on one UTC date (VEC-636): per position, the winning position_state observation on that date among rows with created_at <= seen_before (default: every row). Ordering is block_number, block_version, processing_version, block_timestamp, all descending. Reads one spine chunk when the date is a literal or the plan is custom; a prepared generic plan reads every chunk, so a caller binding d should SET plan_cache_mode = force_custom_plan. Only positions observed on that date appear, with no carry-forward, and the current UTC date is partial until it closes. Raises on a NULL date or bound. created_at is transaction start, so a T is stable once older than every spine write open at T. Tiered chunks are read only when the caller sets timescaledb.enable_tiered_reads = on.';

-- Every date. holder_id joins the DISTINCT ON key without changing the grouping (position_id is a hash
-- over it) so a holder filter is pushed below the DISTINCT ON rather than applied after it.
CREATE OR REPLACE FUNCTION position_daily_as_of(seen_before timestamptz)
    RETURNS TABLE (
        position_id        bytea,
        as_of_date         date,
        chain_id           integer,
        protocol_id        bigint,
        instrument_key     text,
        holder_id          text,
        quantity           numeric,
        block_number       bigint,
        block_version      integer,
        processing_version integer,
        block_timestamp    timestamptz,
        projection         text,
        build_id           integer,
        created_at         timestamptz,
        deal_type          text,
        run_id             bigint)
    LANGUAGE sql STABLE
AS $fn$
    SELECT DISTINCT ON (p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date, p.holder_id)
           p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date, p.chain_id, p.protocol_id,
           p.instrument_key, p.holder_id, p.quantity, p.block_number, p.block_version,
           p.processing_version, p.block_timestamp, p.projection, p.build_id, p.created_at,
           p.deal_type, p.run_id
      FROM public.position_state p
     WHERE p.created_at <= public.position_daily_as_of_bound(seen_before)
     ORDER BY p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date, p.holder_id,
              p.block_number DESC, p.block_version DESC, p.processing_version DESC, p.block_timestamp DESC;
$fn$;

COMMENT ON FUNCTION position_daily_as_of(timestamptz) IS '[Operational] position_daily as it read at time T (VEC-636): one row per (position, UTC date), the winning position_state observation among rows with created_at <= T. Raises on a NULL T. Scans every spine chunk, so a filter on as_of_date does not exclude chunks; read one date through position_daily_on. Same ordering, created_at window and tiered-read caveat as position_daily_on.';

CREATE OR REPLACE VIEW position_daily AS
    SELECT * FROM public.position_daily_as_of('infinity'::timestamptz);

COMMENT ON VIEW position_daily IS '[Operational] What each position held on each observed UTC date (VEC-636): one row per (position, UTC date), that date''s newest position_state observation. A query over the spine, not a copy. Only observed dates get a row, and the current UTC date is partial until it closes. For one date use position_daily_on(d), which reads one chunk; this view reads them all. Pin a time with position_daily_as_of(T).';

GRANT SELECT ON position_daily TO stl_readonly;
GRANT SELECT ON position_daily TO stl_readwrite;
GRANT EXECUTE ON FUNCTION position_daily_on(date, timestamptz), position_daily_as_of(timestamptz) TO stl_readonly, stl_readwrite;

INSERT INTO public.migrations (filename) VALUES ('20260824_120000_create_position_daily.sql') ON CONFLICT (filename) DO NOTHING;
