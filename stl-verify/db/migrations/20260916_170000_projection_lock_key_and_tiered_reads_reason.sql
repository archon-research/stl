-- VEC-810: derive the position-projection advisory lock key in one place.
-- VEC-799: record why materialize_position_projection overrides enable_tiered_reads.

-- VEC-810. The spine built the key inline and every wrapper wrote the finished string out by hand
-- before its pre-checks. They agree today only because format('%I.%I', ...) needs no quoting for the
-- current names. Nothing compares the two, so a change to either side is silent.
-- No SET search_path: a SQL function carrying one cannot be inlined, and every object here is
-- already schema-qualified. STABLE, not IMMUTABLE -- it reads the catalogue.
CREATE OR REPLACE FUNCTION position_projection_lock_key(p_view regclass)
    RETURNS bigint
    LANGUAGE sql
    STABLE
    PARALLEL SAFE AS $fn$
SELECT pg_catalog.hashtextextended(
           'materialize_position_projection.' || pg_catalog.format('%I.%I', nsp.nspname, cls.relname), 0)
  FROM pg_catalog.pg_class cls
  JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
 WHERE cls.oid = p_view
$fn$;

COMMENT ON FUNCTION position_projection_lock_key(regclass) IS '[Operational] VEC-810: the single definition of the advisory lock key that serialises runs of one position projection. materialize_position_projection() takes it, and every wrapper takes the same key before its own pre-checks so that the pre-check and the append are one unit. Returns NULL for an oid that names no relation, which pg_advisory_xact_lock rejects rather than silently locking nothing. A wrapper that writes the key out by hand instead of calling this is a review finding: the two agree only while format(''%I.%I'') needs no quoting for that view''s name, and nothing compares them.';

CREATE OR REPLACE FUNCTION materialize_position_projection(p_view regclass, p_build_id integer DEFAULT 0,
                                                           p_run_id bigint DEFAULT NULL,
                                                           p_window interval DEFAULT NULL)
    RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    SET timescaledb.enable_tiered_reads = 'on'
    AS $fn$
DECLARE n bigint; bad text; bad_qty text; bad_dt text; v_qualname text; v_since timestamptz; v_emitted bigint;
        v_inverted integer := 0; v_refused integer := 0;
BEGIN
    IF p_view IS NULL THEN
        RAISE EXCEPTION 'materialize_position_projection: p_view must not be NULL';
    END IF;
    -- NULL is unbounded. A bounded run cannot discover history outside its window (see the function
    -- COMMENT), so bootstrap and recovery pass NULL.
    IF p_window IS NOT NULL THEN
        -- Judged on the instant, not the interval: a calendar interval can normalise positive yet bound
        -- into the future, and an infinite one would sweep everything while stamping a non-NULL window.
        v_since := now() - p_window;
        IF NOT isfinite(v_since) OR v_since >= now() THEN
            RAISE EXCEPTION 'materialize_position_projection: p_window must be a finite positive interval, got %', p_window;
        END IF;
    END IF;

    IF p_build_id IS NULL THEN
        RAISE EXCEPTION 'materialize_position_projection: p_build_id must not be NULL (omit it to take the default)';
    END IF;

    SELECT format('%I.%I', nsp.nspname, cls.relname) INTO v_qualname
      FROM pg_catalog.pg_class cls JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
     WHERE cls.oid = p_view;
    IF v_qualname IS NULL THEN
        RAISE EXCEPTION 'materialize_position_projection: p_view (oid %) does not name an existing relation', p_view::oid;
    END IF;

    EXECUTE format('LOCK TABLE %s IN ACCESS SHARE MODE', v_qualname);
    SELECT format('%I.%I', nsp.nspname, cls.relname) INTO v_qualname
      FROM pg_catalog.pg_class cls JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
     WHERE cls.oid = p_view;
    IF v_qualname IS NULL THEN
        RAISE EXCEPTION 'materialize_position_projection: p_view (oid %) was dropped while being locked', p_view::oid;
    END IF;
    -- VEC-810: one definition of the key, so a wrapper taking it by hand cannot drift from this.
    PERFORM pg_advisory_xact_lock(public.position_projection_lock_key(p_view));

    SELECT string_agg(e.col || ' (' || COALESCE('is ' || format_type(a.atttypid, a.atttypmod), 'MISSING') || ')', ', ')
      INTO bad
    FROM (VALUES ('chain_id','integer'),('protocol_id','bigint'),('instrument_key','text'),('holder_id','text'),
                 ('quantity','numeric'),('block_number','bigint'),
                 ('block_version','integer'),('processing_version','integer'),('block_timestamp','timestamp with time zone'),
                 ('deal_type','text')
         ) AS e(col, typ)
    LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = p_view AND a.attname = e.col AND a.attnum > 0 AND NOT a.attisdropped
    WHERE a.attname IS NULL OR format_type(a.atttypid, NULL::integer) <> e.typ;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % violates the position_state column contract: %', p_view, bad;
    END IF;

    SELECT string_agg(format('%s is %s', e.col, format_type(a.atttypid, a.atttypmod)), ', ')
      INTO bad
    FROM (VALUES ('quantity'), ('block_timestamp')) AS e(col)
    JOIN pg_catalog.pg_attribute a ON a.attrelid = p_view AND a.attname = e.col
         AND a.attnum > 0 AND NOT a.attisdropped
    WHERE a.atttypmod <> -1
      AND ((e.col = 'quantity'        AND ((a.atttypmod - 4) & 65535) < 18)
        OR (e.col = 'block_timestamp' AND a.atttypmod < 6));
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % declares a lossy type for a value column (it would silently round or truncate; widen the view''s cast): %', p_view, bad;
    END IF;

    DROP TABLE IF EXISTS pg_temp._mpp_src;
    DROP TABLE IF EXISTS pg_temp._mpp_new;
    DROP TABLE IF EXISTS pg_temp._mpp_drift;
    DROP TABLE IF EXISTS pg_temp._mpp_refused;
    -- The bound is a literal, never a bind parameter (AGENTS.md, "A time window on a hypertable is a SQL
    -- literal"). It applies to the view's output: above a DISTINCT ON it filters rows without pruning chunks.
    EXECUTE format($q$
        CREATE TEMP TABLE _mpp_src ON COMMIT DROP AS
        SELECT public.position_id(chain_id, protocol_id, instrument_key, holder_id) AS position_id,
               chain_id, protocol_id, instrument_key, holder_id, quantity,
               block_number, block_version, processing_version, block_timestamp, deal_type
        FROM %s %s
    $q$, v_qualname,
         CASE WHEN p_window IS NULL THEN ''
              ELSE format('WHERE block_timestamp > %L::timestamptz', v_since) END);
    ANALYZE pg_temp._mpp_src;

    SELECT string_agg(msg, ', ') INTO bad FROM (
        SELECT format('%s=NULL at bn=%s bv=%s pv=%s ik=%s',
                      c.col,
                      COALESCE(s.block_number::text, 'NULL'),
                      COALESCE(s.block_version::text, 'NULL'),
                      COALESCE(s.processing_version::text, 'NULL'),
                      COALESCE(s.instrument_key, 'NULL')) AS msg
        FROM pg_temp._mpp_src s
        CROSS JOIN LATERAL (VALUES
            ('instrument_key',     s.instrument_key IS NULL),
            ('holder_id',          s.holder_id IS NULL),
            ('quantity',           s.quantity IS NULL),
            ('block_number',       s.block_number IS NULL),
            ('block_version',      s.block_version IS NULL),
            ('processing_version', s.processing_version IS NULL),
            ('block_timestamp',    s.block_timestamp IS NULL)
        ) AS c(col, is_null)
        WHERE c.is_null
        ORDER BY s.block_number, s.block_version, s.processing_version, s.instrument_key, c.col
        LIMIT 5) z;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % emits NULL in a NOT NULL position_state column (a nullable source must COALESCE): %', p_view, bad;
    END IF;

    SELECT string_agg(msg, '; ') INTO bad FROM (
        SELECT format('pos=%s bn=%s bv=%s pv=%s x%s', encode(position_id, 'hex'),
                      block_number, block_version, processing_version, count(*)) AS msg
        FROM pg_temp._mpp_src
        GROUP BY position_id, block_number, block_version, processing_version
        HAVING count(*) > 1
        ORDER BY position_id, block_number, block_version, processing_version
        LIMIT 5) z;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % double-emits a logical observation key (position_id,block_number,block_version,processing_version): %', p_view, bad;
    END IF;

    -- A negative or non-finite quantity is a view bug, named here before closure could drop it as a
    -- non-observation and before the table CHECK could reject it with only a constraint name.
    SELECT string_agg(format('qty=%s at bn=%s bv=%s pv=%s ik=%s', s.quantity, s.block_number, s.block_version, s.processing_version, s.instrument_key), '; ')
      INTO bad
    FROM (SELECT * FROM pg_temp._mpp_src
           WHERE quantity < 0 OR quantity = 'NaN'::numeric OR quantity >= 'Infinity'::numeric
           ORDER BY block_number, block_version, processing_version, instrument_key LIMIT 5) s;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % emits a negative or non-finite quantity: %', p_view, bad;
    END IF;

    -- Off-chain rows (chain_id NULL) carry block_number = floor(epoch of block_timestamp): the
    -- observation key needs distinct blocks per snapshot, and deriving it makes the value checkable.
    SELECT string_agg(format('bn=%s ts=%s', s.block_number, s.block_timestamp), '; ') INTO bad FROM (
        SELECT block_number, block_timestamp FROM pg_temp._mpp_src
         WHERE chain_id IS NULL
           AND block_number <> floor(extract(epoch FROM block_timestamp))::bigint
         ORDER BY block_timestamp LIMIT 5) s;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % emits off-chain rows (chain_id NULL) whose block_number is not floor(epoch seconds of block_timestamp): %', p_view, bad;
    END IF;

    -- Closure, applied once here rather than per view: keep every positive row, the first zero after a
    -- positive (the close) and a zero whose predecessor is a sibling version of the same block. Leading
    -- zeros and repeated zeros are not observations. Judged against stored history too, not the batch alone.
    DELETE FROM pg_temp._mpp_src s USING (
        SELECT m.ctid AS rid, m.quantity,
               coalesce(lag(m.quantity)     OVER w, h.prev_qty) AS prev_qty,
               coalesce(lag(m.block_number) OVER w, h.prev_bn)  AS prev_bn,
               coalesce(bool_or(m.quantity > 0) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), false)
                 OR coalesce(h.opened_before, false) AS opened_before
        FROM pg_temp._mpp_src m
        -- The stored row at or before the position's first batch row in full key order, and whether it had
        -- opened by then. A first row that is itself stored is a re-emit, suppressed on insert, so the STORED
        -- value is what its siblings are judged against; a lone close would otherwise read as a leading zero.
        LEFT JOIN (
            SELECT b.position_id,
                   (SELECT p.quantity FROM public.position_state p
                     WHERE p.position_id = b.position_id
                       AND (p.block_number, p.block_version, p.processing_version) <= (b.bn, b.bv, b.pv)
                     ORDER BY p.block_number DESC, p.block_version DESC, p.processing_version DESC LIMIT 1) AS prev_qty,
                   (SELECT p.block_number FROM public.position_state p
                     WHERE p.position_id = b.position_id
                       AND (p.block_number, p.block_version, p.processing_version) <= (b.bn, b.bv, b.pv)
                     ORDER BY p.block_number DESC, p.block_version DESC, p.processing_version DESC LIMIT 1) AS prev_bn,
                   EXISTS (SELECT 1 FROM public.position_state p
                            WHERE p.position_id = b.position_id AND p.quantity > 0
                              AND (p.block_number, p.block_version, p.processing_version) <= (b.bn, b.bv, b.pv)) AS opened_before
            FROM (SELECT DISTINCT ON (position_id) position_id,
                         block_number AS bn, block_version AS bv, processing_version AS pv
                  FROM pg_temp._mpp_src
                  ORDER BY position_id, block_number, block_version, processing_version) b) h
          ON h.position_id = m.position_id
        WINDOW w AS (PARTITION BY m.position_id ORDER BY m.block_number, m.block_version, m.processing_version)) k
    WHERE s.ctid = k.rid
      -- coalesce: with no predecessor anywhere prev_qty is NULL and a NULL predicate would spare the row
      AND NOT coalesce(k.quantity > 0 OR k.prev_qty > 0 OR (k.opened_before AND k.prev_bn = s.block_number), false);
    ANALYZE pg_temp._mpp_src;
    SELECT count(*) INTO v_emitted FROM pg_temp._mpp_src;

    -- One pass over the stored keys this batch re-emits. Stored rows are kept and nothing is applied:
    -- a real correction bumps block_version/processing_version. Every drift is recorded, so the fork
    -- between view and spine is a queryable row rather than a log line, and the run continues.
    CREATE TEMP TABLE _mpp_drift ON COMMIT DROP AS
        SELECT s.position_id, s.block_number, s.block_version, s.processing_version, s.instrument_key, s.holder_id,
               p.block_timestamp <> s.block_timestamp        AS ts_drift,
               p.quantity IS DISTINCT FROM s.quantity        AS qty_drift,
               p.deal_type IS DISTINCT FROM s.deal_type      AS dt_drift,
               p.block_timestamp AS stored_ts,  s.block_timestamp AS emitted_ts,
               p.quantity        AS stored_qty, s.quantity        AS emitted_qty,
               p.deal_type       AS stored_dt,  s.deal_type       AS emitted_dt
        FROM pg_temp._mpp_src s
        JOIN public.position_state p ON p.position_id = s.position_id AND p.block_number = s.block_number
             AND p.block_version = s.block_version AND p.processing_version = s.processing_version
        WHERE p.block_timestamp <> s.block_timestamp
           OR p.quantity IS DISTINCT FROM s.quantity
           OR p.deal_type IS DISTINCT FROM s.deal_type;
    INSERT INTO public.position_projection_refusal
        (projection, position_id, block_number, block_version, processing_version, reason, detail, build_id, run_id)
    SELECT v_qualname, position_id, block_number, block_version, processing_version, r.reason,
           format('ik=%s holder=%s stored ts=%s qty=%s dt=%s; emitted ts=%s qty=%s dt=%s', instrument_key, holder_id,
                  stored_ts, stored_qty, coalesce(stored_dt, 'NULL'), emitted_ts, emitted_qty, coalesce(emitted_dt, 'NULL')),
           p_build_id, p_run_id
    FROM pg_temp._mpp_drift d
    CROSS JOIN LATERAL (SELECT 'deal_type_drift' AS reason WHERE d.dt_drift
                        UNION ALL SELECT 'observation_drift' WHERE d.ts_drift OR d.qty_drift) r
    ON CONFLICT DO NOTHING;
    SELECT string_agg(msg, '; ') FILTER (WHERE ts_drift),
           string_agg(msg, '; ') FILTER (WHERE qty_drift),
           string_agg(msg || format(' stored=%s emitted=%s', coalesce(stored_dt, 'NULL'), coalesce(emitted_dt, 'NULL')), '; ')
               FILTER (WHERE dt_drift)
      INTO bad, bad_qty, bad_dt
    FROM (SELECT format('pos=%s bn=%s bv=%s pv=%s', encode(position_id, 'hex'), block_number, block_version, processing_version) AS msg,
                 ts_drift, qty_drift, dt_drift, stored_dt, emitted_dt
          FROM pg_temp._mpp_drift
          ORDER BY position_id, block_number, block_version, processing_version
          LIMIT 5) z;
    IF bad IS NOT NULL THEN
        RAISE WARNING 'projection % re-emits stored observations with a changed block_timestamp; stored rows kept and recorded in position_projection_refusal (a real correction must bump block_version/processing_version): %', p_view, bad;
    END IF;
    IF bad_qty IS NOT NULL THEN
        RAISE WARNING 'projection % re-emits stored observations with a changed quantity; stored rows kept and recorded in position_projection_refusal (append-only: a real correction must bump block_version/processing_version): %', p_view, bad_qty;
    END IF;
    IF bad_dt IS NOT NULL THEN
        RAISE WARNING 'projection % re-emits stored observations with a different deal_type; stored rows kept and recorded in position_projection_refusal, since the insert is suppressed on the stored key and UPDATE is revoked (a real correction must bump block_version/processing_version): %', p_view, bad_dt;
    END IF;

    -- Within one position a higher block cannot carry an earlier instant, or the caches (ordered by
    -- block, dated by timestamp) disagree. Adjacent pairs suffice: within the batch by window, against
    -- history by one indexed probe below and above each new row, so cost is bounded by the batch.
    CREATE TEMP TABLE _mpp_new ON COMMIT DROP AS
        SELECT s.position_id, s.block_number, s.block_timestamp FROM pg_temp._mpp_src s
        WHERE NOT EXISTS (SELECT 1 FROM public.position_state p
                           WHERE p.position_id = s.position_id AND p.block_number = s.block_number
                             AND p.block_version = s.block_version AND p.processing_version = s.processing_version);
    ANALYZE pg_temp._mpp_new;
    -- The offending POSITIONS, not the first five: their new observations are withheld this run and
    -- recorded, and every other position lands. Aborting here refused the whole projection forever.
    CREATE TEMP TABLE _mpp_refused ON COMMIT DROP AS
        SELECT DISTINCT ON (w.position_id) w.position_id,
               format('bn=%s@%s vs bn=%s@%s', w.block_number, w.block_timestamp, o.block_number, o.block_timestamp) AS detail
        FROM (SELECT position_id, block_number, block_timestamp,
                     lag(block_number)  OVER win AS prev_bn, lag(block_timestamp)  OVER win AS prev_ts,
                     lead(block_number) OVER win AS next_bn, lead(block_timestamp) OVER win AS next_ts
              FROM pg_temp._mpp_new
              WINDOW win AS (PARTITION BY position_id ORDER BY block_number, block_timestamp)) w
        CROSS JOIN LATERAL (
            SELECT w.prev_bn AS block_number, w.prev_ts AS block_timestamp WHERE w.prev_bn IS NOT NULL
            UNION ALL
            SELECT w.next_bn, w.next_ts WHERE w.next_bn IS NOT NULL
            UNION ALL
            (SELECT p.block_number, p.block_timestamp FROM public.position_state p
              WHERE p.position_id = w.position_id AND p.block_number < w.block_number
              ORDER BY p.block_number DESC, p.block_timestamp DESC LIMIT 1)
            UNION ALL
            (SELECT p.block_number, p.block_timestamp FROM public.position_state p
              WHERE p.position_id = w.position_id AND p.block_number > w.block_number
              ORDER BY p.block_number ASC, p.block_timestamp ASC LIMIT 1)
        ) o
        WHERE (w.block_number > o.block_number AND w.block_timestamp < o.block_timestamp)
           OR (w.block_number < o.block_number AND w.block_timestamp > o.block_timestamp)
        ORDER BY w.position_id, w.block_number;
    INSERT INTO public.position_projection_refusal
        (projection, position_id, block_number, block_version, processing_version, reason, detail, build_id, run_id)
    SELECT v_qualname, s.position_id, s.block_number, s.block_version, s.processing_version, 'block_time_inverts_height',
           format('ik=%s holder=%s %s', s.instrument_key, s.holder_id, r.detail), p_build_id, p_run_id
    FROM pg_temp._mpp_src s
    JOIN pg_temp._mpp_refused r USING (position_id)
    WHERE NOT EXISTS (SELECT 1 FROM public.position_state p
                       WHERE p.position_id = s.position_id AND p.block_number = s.block_number
                         AND p.block_version = s.block_version AND p.processing_version = s.processing_version)
    ON CONFLICT DO NOTHING;
    SELECT count(*) INTO v_inverted FROM pg_temp._mpp_refused;
    -- positions_refused counts every class the run withheld, not just the inversions: a projection
    -- re-emitting stored keys with changed values reported zero, so the per-run signal and the alert
    -- that reads it stayed silent while the view and the spine disagreed indefinitely.
    SELECT count(*) INTO v_refused
      FROM (SELECT position_id FROM pg_temp._mpp_refused
            UNION
            SELECT position_id FROM pg_temp._mpp_drift) z;
    IF v_inverted > 0 THEN
        SELECT string_agg(format('pos=%s %s', encode(position_id, 'hex'), detail), '; ') INTO bad
          FROM (SELECT * FROM pg_temp._mpp_refused ORDER BY position_id LIMIT 5) z;
        RAISE WARNING 'projection % emits a higher block with an earlier block_timestamp for % position(s); their new observations are withheld this run and recorded in position_projection_refusal, the rest of the batch continues (first 5): %', p_view, v_inverted, bad;
        DELETE FROM pg_temp._mpp_src s USING pg_temp._mpp_refused r WHERE s.position_id = r.position_id;
    END IF;

    SELECT format('position %s owned by %s', encode(p.position_id, 'hex'), p.projection) INTO bad
    FROM (SELECT DISTINCT position_id FROM pg_temp._mpp_src) s
    JOIN public.position_state p
      ON p.position_id = s.position_id AND p.projection <> v_qualname
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % emits position_ids owned by another projection (cross-view disjointness violated): %', p_view, bad;
    END IF;

    INSERT INTO public.position_state
        (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
         block_number, block_version, processing_version, block_timestamp, projection, build_id,
         run_id, deal_type)
    SELECT s.position_id, s.chain_id, s.protocol_id, s.instrument_key, s.holder_id, s.quantity,
           s.block_number, s.block_version, s.processing_version, s.block_timestamp, v_qualname,
           p_build_id, p_run_id, s.deal_type
    FROM pg_temp._mpp_src s
    WHERE NOT EXISTS (
        SELECT 1 FROM public.position_state p
        WHERE p.position_id = s.position_id AND p.block_number = s.block_number
          AND p.block_version = s.block_version AND p.processing_version = s.processing_version)
    ORDER BY s.block_timestamp, s.position_id, s.block_number, s.block_version, s.processing_version
    ON CONFLICT (position_id, block_number, block_version, processing_version, block_timestamp) DO NOTHING;
    GET DIAGNOSTICS n = ROW_COUNT;

    INSERT INTO public.position_projection_run
        (projection, build_id, run_id, block_timestamp, rows_emitted, rows_appended, positions_refused, window_interval)
    SELECT v_qualname, p_build_id, p_run_id, max(block_timestamp), v_emitted, n, v_refused, p_window FROM pg_temp._mpp_src;

    DROP TABLE pg_temp._mpp_src;
    DROP TABLE pg_temp._mpp_new;
    DROP TABLE pg_temp._mpp_drift;
    DROP TABLE pg_temp._mpp_refused;

    RETURN n;
END $fn$;
-- VEC-799. The override is on the function since #943; review of #624/#626 asked for the reason.
-- Newest-per-key over position_state against local chunks alone reads a partial spine once chunks
-- tier, and the sources begin tiering 2027-02-03 and 2027-02-27. A time predicate on the source is
-- what legitimately removes the tiered tail from scope; this override is not a substitute for one.
COMMENT ON FUNCTION materialize_position_projection(regclass, integer, bigint, interval) IS '[Operational] VEC-402..407 shared materializer: evaluate a per-protocol projection view ONCE into a temp table, validate it against the position_state column contract (each RAISE in the body names its own check), then apply closure and APPEND the new observations, recording the completed run with its counts in position_projection_run, all in one transaction. A view bug (NULLs, a wrong type, a double-emitted key, a negative quantity, the off-chain block_number rule) aborts the run BEFORE closure can drop the offending row; a position_id owned by another projection aborts it too, but after closure, because the check reads what the run would actually append. A data conflict aborts nothing: a position whose new observations invert block against instant is withheld this run, and a stored key re-emitted with a different value keeps the stored row, each recorded in position_projection_refusal and warned. deal_type is copied through; the FK to ref_deal_type constrains the value. position_id is recomputed via position_id(); serialized per view by an advisory lock whose key comes from position_projection_lock_key(p_view), the one definition wrappers share (VEC-810). Idempotent for a fixed source; run out of band. p_build_id and p_run_id are stamped on every row it appends, on the refusals and on the run record (NULL run means pre-tracking). Returns rows INSERTED. p_window (VEC-566) bounds the batch to view rows with block_timestamp > now() - p_window, the instant interpolated as a literal; NULL is unbounded. The bound is applied to the view''s output: every projection dedupes with a DISTINCT ON whose key excludes block_timestamp, and PostgreSQL does not push a qualifier below such a DISTINCT, so against those views it filters rows without pruning chunks. A bounded run keeps closure correct (the LAG falls back to history probed from position_state) but cannot discover positions whose observations all fall outside the window, so bootstrap and any recovery after an outage longer than the window must pass NULL. The window is stamped on the run record as window_interval. TIERED READS (VEC-799): this function pins timescaledb.enable_tiered_reads = on, overriding a database-level off, because its newest-per-key reads over position_state would otherwise answer from local chunks alone and see a partial spine once chunks tier to S3. Measured cost of the pin today is 0.3%.';

INSERT INTO migrations (filename) VALUES ('20260916_170000_projection_lock_key_and_tiered_reads_reason.sql') ON CONFLICT (filename) DO NOTHING;
