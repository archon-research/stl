-- position_daily (VEC-636): what a position held on a UTC date, read straight from position_state.
-- Nothing is copied: the spine is chunked by day and ordered by the version tuple, and its own
-- created_at answers "as of T".

-- A NULL bound makes every created_at <= NULL comparison NULL, so the read would return an empty set
-- that a caller with an unset timestamp reads as "held nothing". Raise instead.
CREATE OR REPLACE FUNCTION position_daily_as_of_bound(seen_before timestamptz) RETURNS timestamptz
    LANGUAGE plpgsql IMMUTABLE
    SET search_path = pg_catalog, public
AS $fn$
BEGIN
    IF seen_before IS NULL THEN
        RAISE EXCEPTION 'position_daily: the as-of time is required; a NULL bound would return an empty series rather than an answer';
    END IF;
    RETURN seen_before;
END;
$fn$;

COMMENT ON FUNCTION position_daily_as_of_bound(timestamptz) IS '[Operational] Returns its argument, raising on NULL (VEC-636). Guards the position_daily reads, whose created_at filter would otherwise turn a NULL bound into a silent empty result. IMMUTABLE so a constant bound folds at plan time and the guarded read still inlines.';

-- The same guard for the date, IMMUTABLE so a constant date folds at plan time.
CREATE OR REPLACE FUNCTION position_daily_date_required(d date) RETURNS date
    LANGUAGE plpgsql IMMUTABLE
    SET search_path = pg_catalog, public
AS $fn$
BEGIN
    IF d IS NULL THEN
        RAISE EXCEPTION 'position_daily: the date is required; a NULL date would return an empty book rather than an answer';
    END IF;
    RETURN d;
END;
$fn$;

COMMENT ON FUNCTION position_daily_date_required(date) IS '[Operational] Returns its argument, raising on NULL (VEC-636). Guards position_daily_between, whose block_timestamp window would otherwise be NULL and return nothing.';

-- A date range, d_to inclusive. The window is on block_timestamp itself, the partition column, so it opens
-- one chunk per day; a predicate on (block_timestamp AT TIME ZONE 'utc')::date opens every chunk.
CREATE OR REPLACE FUNCTION position_daily_between(d_from date, d_to date, seen_before timestamptz DEFAULT 'infinity')
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
    SELECT DISTINCT ON (p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date)
           p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date, p.chain_id, p.protocol_id,
           p.instrument_key, p.holder_id, p.quantity, p.block_number, p.block_version,
           p.processing_version, p.block_timestamp, p.projection, p.build_id, p.created_at,
           p.deal_type, p.run_id
      FROM public.position_state p
     WHERE p.block_timestamp >= timezone('utc', public.position_daily_date_required(d_from)::timestamp)
       AND p.block_timestamp <  timezone('utc', (public.position_daily_date_required(d_to) + 1)::timestamp)
       AND p.created_at <= public.position_daily_as_of_bound(seen_before)
     ORDER BY p.position_id, (p.block_timestamp AT TIME ZONE 'utc')::date,
              p.block_number DESC, p.block_version DESC, p.processing_version DESC, p.block_timestamp DESC;
$fn$;

COMMENT ON FUNCTION position_daily_between(date, date, timestamptz) IS '[Operational] What each position held on each UTC date from d_from to d_to inclusive (VEC-636): one row per (position, UTC date), that date''s winning position_state observation among rows with created_at <= seen_before (default: every row). Ordering is block_number, block_version, processing_version, block_timestamp, all descending. Reads one spine chunk per day in the range. Only observed dates get a row, with no carry-forward, and the current UTC date is partial until it closes. Raises on a NULL date or bound. created_at is transaction start, so a T is stable once older than every spine write open at T. Per db/migrations/AGENTS.md, a caller''s window is validated and interpolated as a literal, never bound: under a generic plan a bound date can read every chunk. A date in a tiered chunk returns empty, not an error, when timescaledb.enable_tiered_reads is off. A holder''s series is fastest as holder -> position_ids from position_current -> position_daily_between(...) WHERE position_id = ANY(...), which uses the compressed chunks'' position_id index; a holder_id filter reads every chunk in the range.';

-- One date: the one-day case of position_daily_between.
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
    SELECT * FROM public.position_daily_between(d, d, seen_before);
$fn$;

COMMENT ON FUNCTION position_daily_on(date, timestamptz) IS '[Operational] The whole book on one UTC date (VEC-636): position_daily_between(d, d, seen_before). Reads one spine chunk. The ordering, created_at window, literal-window rule and tiered-read behaviour are those of position_daily_between.';

GRANT EXECUTE ON FUNCTION position_daily_between(date, date, timestamptz), position_daily_on(date, timestamptz) TO stl_readonly, stl_readwrite;

INSERT INTO public.migrations (filename) VALUES ('20260824_120000_create_position_daily.sql') ON CONFLICT (filename) DO NOTHING;
