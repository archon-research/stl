-- VEC-401: record the deal type on the observation, since position_state cannot derive it: the Morpho
-- loan leg nets supply against borrow as abs(supply - borrow), which destroys the LOAN/BORROW sign.
-- Sorts before every projection and cache so they can reference the column without declaring it.
ALTER TABLE position_state ADD COLUMN IF NOT EXISTS deal_type text;

-- Guarded because ADD CONSTRAINT has no IF NOT EXISTS and the rest of this file is re-runnable.
DO $fk$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
                    WHERE conrelid = 'public.position_state'::regclass
                      AND conname = 'position_state_deal_type_fkey') THEN
        ALTER TABLE position_state ADD CONSTRAINT position_state_deal_type_fkey
            FOREIGN KEY (deal_type) REFERENCES ref_deal_type (deal_type);
    END IF;
END
$fk$;

COMMENT ON COLUMN position_state.deal_type IS 'Derived, nullable. Deal type of THIS observation (LOAN / BORROW / COLLATERAL), stamped by the projection: the Morpho market loan leg nets supply against borrow, so quantity carries the magnitude and this column carries the direction. NULL where the projection emits none. Roles: FK->ref_deal_type.deal_type.';

CREATE TABLE IF NOT EXISTS position_projection_run (
    projection      text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    build_id        integer     NOT NULL,
    block_timestamp timestamptz,
    CONSTRAINT position_projection_run_pkey PRIMARY KEY (projection, created_at)
);

COMMENT ON TABLE position_projection_run IS '[Operational] One row per COMPLETED materialize_position_projection() run, written in the run''s own transaction. A position whose latest observation trails its projection''s latest row here was swept and not re-observed. Plain table: volume is one row per run per projection, so no compression or tiering.';
COMMENT ON COLUMN position_projection_run.projection IS 'Roles: PK. The projection view''s canonical name, as stamped on position_state.projection.';
COMMENT ON COLUMN position_projection_run.created_at IS 'Roles: PK. When the run completed (clock time, so two runs in one transaction are two rows). UTC.';
COMMENT ON COLUMN position_projection_run.build_id IS 'Roles: Audit. build_registry.id of the run (0 = pre-tracking).';
COMMENT ON COLUMN position_projection_run.block_timestamp IS 'Roles: Derived. Latest block_timestamp the projection emitted in this run; NULL when it emitted nothing, which is still a completed sweep. Comparable across on-chain and off-chain observations, unlike block_number.';

GRANT SELECT ON position_projection_run TO stl_readonly;
GRANT SELECT, INSERT ON position_projection_run TO stl_readwrite;
REVOKE UPDATE, DELETE ON position_projection_run FROM stl_readwrite;

COMMENT ON COLUMN position_state.block_number IS 'Roles: PK. Block height of the observation for an on-chain projection (chain_id set). For an OFF-CHAIN observation (chain_id NULL, a custody snapshot) it is floor(epoch seconds of block_timestamp), enforced by the materializer, and not a block on any chain.';

-- Body from 20260818_130000, extended: deal_type is a required contract column, closure is applied
-- here for every projection, the run is recorded, off-chain rows carry their instant as block_number,
-- and block_timestamp must be monotonic in block_number per position. Each RAISE names its own check.
CREATE OR REPLACE FUNCTION materialize_position_projection(p_view regclass, p_build_id integer DEFAULT 0)
    RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    SET timescaledb.enable_tiered_reads = 'on'
    AS $fn$
DECLARE n bigint; bad text; bad_qty text; bad_dt text; v_qualname text;
BEGIN
    IF p_view IS NULL THEN
        RAISE EXCEPTION 'materialize_position_projection: p_view must not be NULL';
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
    PERFORM pg_advisory_xact_lock(hashtextextended('materialize_position_projection.' || v_qualname, 0));

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
    EXECUTE format($q$
        CREATE TEMP TABLE _mpp_src ON COMMIT DROP AS
        SELECT public.position_id(chain_id, protocol_id, instrument_key, holder_id) AS position_id,
               chain_id, protocol_id, instrument_key, holder_id, quantity,
               block_number, block_version, processing_version, block_timestamp, deal_type
        FROM %s
    $q$, p_view);
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

    -- Closure, applied once here rather than per view: keep every positive row, the first zero after a
    -- positive (the close) and a zero whose predecessor is a sibling version of the same block (a reorg
    -- or reprocess of the close). Leading zeros and repeated zeros at later blocks are not observations.
    DELETE FROM pg_temp._mpp_src s USING (
        SELECT ctid AS rid, quantity,
               lag(quantity)     OVER w AS prev_qty,
               lag(block_number) OVER w AS prev_bn,
               coalesce(bool_or(quantity > 0) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), false) AS opened_before
        FROM pg_temp._mpp_src
        WINDOW w AS (PARTITION BY position_id ORDER BY block_number, block_version, processing_version)) k
    WHERE s.ctid = k.rid
      -- coalesce: on a position's first row prev_qty is NULL and a NULL predicate would spare the row
      AND NOT coalesce(k.quantity > 0 OR k.prev_qty > 0 OR (k.opened_before AND k.prev_bn = s.block_number), false);
    ANALYZE pg_temp._mpp_src;

    -- One pass over the stored keys this batch re-emits. Timestamp and quantity drift are kept-stored
    -- and warned; deal_type drift cannot be applied (no UPDATE channel), so it raises after both warnings.
    SELECT string_agg(msg, '; ') FILTER (WHERE ts_drift),
           string_agg(msg, '; ') FILTER (WHERE qty_drift),
           string_agg(msg || format(' stored=%s emitted=%s', coalesce(stored_dt, 'NULL'), coalesce(emitted_dt, 'NULL')), '; ')
               FILTER (WHERE dt_drift)
      INTO bad, bad_qty, bad_dt
    FROM (
        SELECT format('pos=%s bn=%s bv=%s pv=%s', encode(s.position_id, 'hex'),
                      s.block_number, s.block_version, s.processing_version) AS msg,
               p.block_timestamp <> s.block_timestamp        AS ts_drift,
               p.quantity IS DISTINCT FROM s.quantity        AS qty_drift,
               p.deal_type IS DISTINCT FROM s.deal_type      AS dt_drift,
               p.deal_type AS stored_dt, s.deal_type AS emitted_dt
        FROM pg_temp._mpp_src s
        JOIN public.position_state p ON p.position_id = s.position_id AND p.block_number = s.block_number
             AND p.block_version = s.block_version AND p.processing_version = s.processing_version
        WHERE p.block_timestamp <> s.block_timestamp
           OR p.quantity IS DISTINCT FROM s.quantity
           OR p.deal_type IS DISTINCT FROM s.deal_type
        ORDER BY s.position_id, s.block_number, s.block_version, s.processing_version
        LIMIT 5) z;
    IF bad IS NOT NULL THEN
        RAISE WARNING 'projection % re-emits stored observations with a changed block_timestamp; stored rows kept (a real correction must bump block_version/processing_version): %', p_view, bad;
    END IF;
    IF bad_qty IS NOT NULL THEN
        RAISE WARNING 'projection % re-emits stored observations with a changed quantity; stored rows kept (append-only: a real correction must bump block_version/processing_version): %', p_view, bad_qty;
    END IF;
    IF bad_dt IS NOT NULL THEN
        RAISE EXCEPTION 'projection % re-emits stored observations with a different deal_type, which this function CANNOT apply: the insert is suppressed on the stored key and UPDATE is revoked; a real correction must bump block_version/processing_version: %', p_view, bad_dt;
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

    -- Within one position a higher block cannot carry an earlier instant, or the caches (ordered by
    -- block, dated by timestamp) disagree. Adjacent pairs suffice: within the batch by window, against
    -- history by one indexed probe below and above each new row, so cost is bounded by the batch.
    CREATE TEMP TABLE _mpp_new ON COMMIT DROP AS
        SELECT s.position_id, s.block_number, s.block_timestamp FROM pg_temp._mpp_src s
        WHERE NOT EXISTS (SELECT 1 FROM public.position_state p
                           WHERE p.position_id = s.position_id AND p.block_number = s.block_number
                             AND p.block_version = s.block_version AND p.processing_version = s.processing_version);
    ANALYZE pg_temp._mpp_new;
    SELECT string_agg(msg, '; ') INTO bad FROM (
        SELECT format('pos=%s bn=%s@%s vs bn=%s@%s', encode(w.position_id, 'hex'),
                      w.block_number, w.block_timestamp, o.block_number, o.block_timestamp) AS msg
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
        ORDER BY w.position_id, w.block_number
        LIMIT 5) z;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % emits a higher block with an earlier block_timestamp for one position; the caches order by block and date by timestamp, so they would disagree: %', p_view, bad;
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
         deal_type)
    SELECT s.position_id, s.chain_id, s.protocol_id, s.instrument_key, s.holder_id, s.quantity,
           s.block_number, s.block_version, s.processing_version, s.block_timestamp, v_qualname,
           p_build_id, s.deal_type
    FROM pg_temp._mpp_src s
    WHERE NOT EXISTS (
        SELECT 1 FROM public.position_state p
        WHERE p.position_id = s.position_id AND p.block_number = s.block_number
          AND p.block_version = s.block_version AND p.processing_version = s.processing_version)
    ORDER BY s.block_timestamp, s.position_id, s.block_number, s.block_version, s.processing_version
    ON CONFLICT (position_id, block_number, block_version, processing_version, block_timestamp) DO NOTHING;
    GET DIAGNOSTICS n = ROW_COUNT;

    INSERT INTO public.position_projection_run (projection, build_id, block_timestamp)
    SELECT v_qualname, p_build_id, max(block_timestamp) FROM pg_temp._mpp_src;

    DROP TABLE pg_temp._mpp_src;
    DROP TABLE pg_temp._mpp_new;

    RETURN n;
END $fn$;

COMMENT ON FUNCTION materialize_position_projection(regclass, integer) IS '[Operational] VEC-402..407 shared materializer: evaluate a per-protocol projection view ONCE into a temp table, validate it against the position_state column contract (each RAISE in the body names its own check), then apply closure (a position''s leading zeros and repeated zeros are not observations; the first zero after a positive and its same-block siblings are), APPEND the new observations and record the completed run in position_projection_run, all in one transaction. deal_type is copied through; the FK to ref_deal_type constrains the value. position_id is recomputed via position_id(); serialized per view by an advisory lock on the view''s canonical name. Idempotent; run out of band. Returns rows INSERTED.';

-- position_classification is retired: the classification lands on the observation, where a position
-- that flips LOAN/BORROW can be represented; a mutable per-position copy cannot, and nothing wrote it.
DROP TABLE IF EXISTS position_classification;

INSERT INTO migrations (filename) VALUES ('20260818_140000_add_position_state_deal_type.sql') ON CONFLICT (filename) DO NOTHING;
