-- VEC-401: record the deal type on the observation, since it cannot be derived from position_state.
-- The Morpho market loan leg nets supply against borrow as abs(supply - borrow), so the sign -- and
-- with it LOAN vs BORROW -- is destroyed at projection time and no query recovers it.

-- Nullable, and NOT in the materializer's required contract: the vault and Sky legs are constant per
-- instrument and the collateral leg is implied by instrument_key, so only the market loan leg has
-- anything to say. Added now because a later backfill would need a superuser.

-- No FK to deal_type_ref: nothing FKs position_state, and an FK would hit the RI-probe privilege trap
-- 20260714_160000 fixed for the reference tables, since the owner's UPDATE is revoked here.
ALTER TABLE position_state ADD COLUMN IF NOT EXISTS deal_type_code text;

COMMENT ON COLUMN position_state.deal_type_code IS 'Derived, nullable. Deal type of THIS observation (LOAN / BORROW / COLLATERAL), stamped by the projection because it is not recoverable from the stored row: the Morpho market loan leg nets supply against borrow, so quantity carries the magnitude and this column carries the direction. NULL where the projection emits none -- the vault and Sky legs are constant per instrument and the collateral leg is implied by instrument_key, so a reader derives those. Not FK-constrained to deal_type_ref: nothing FKs position_state, and an FK there would need the owner UPDATE this table revokes.';

-- Body copied from 20260818_130000 with one change: deal_type_code is resolved per view and carried
-- into the snapshot and the append. Comments are not duplicated -- that migration is immutable, so it
-- stays the reference for why each check exists, and a copy here would drift from it.
CREATE OR REPLACE FUNCTION materialize_position_projection(p_view regclass, p_build_id integer DEFAULT 0)
    RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    SET timescaledb.enable_tiered_reads = 'on'
    AS $fn$
DECLARE n bigint; bad text; bad_qty text; v_qualname text; v_deal_type text;
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
                 ('block_version','integer'),('processing_version','integer'),('block_timestamp','timestamp with time zone')
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

    SELECT CASE WHEN EXISTS (
               SELECT 1 FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = p_view AND a.attname = 'deal_type_code'
                  AND a.attnum > 0 AND NOT a.attisdropped
                  AND format_type(a.atttypid, NULL::integer) = 'text')
           THEN 'deal_type_code' ELSE 'NULL::text' END
      INTO v_deal_type;

    DROP TABLE IF EXISTS pg_temp._mpp_src;
    EXECUTE format($q$
        CREATE TEMP TABLE _mpp_src ON COMMIT DROP AS
        SELECT public.position_id(chain_id, protocol_id, instrument_key, holder_id) AS position_id,
               chain_id, protocol_id, instrument_key, holder_id, quantity,
               block_number, block_version, processing_version, block_timestamp,
               %2$s AS deal_type_code
        FROM %1$s
    $q$, p_view, v_deal_type);
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

    SELECT string_agg(msg, '; ') FILTER (WHERE ts_drift),
           string_agg(msg, '; ') FILTER (WHERE qty_drift)
      INTO bad, bad_qty
    FROM (
        SELECT format('pos=%s bn=%s bv=%s pv=%s', encode(s.position_id, 'hex'),
                      s.block_number, s.block_version, s.processing_version) AS msg,
               p.block_timestamp <> s.block_timestamp        AS ts_drift,
               p.quantity IS DISTINCT FROM s.quantity        AS qty_drift
        FROM pg_temp._mpp_src s
        JOIN public.position_state p ON p.position_id = s.position_id AND p.block_number = s.block_number
             AND p.block_version = s.block_version AND p.processing_version = s.processing_version
        WHERE p.block_timestamp <> s.block_timestamp
           OR p.quantity IS DISTINCT FROM s.quantity
        ORDER BY s.position_id, s.block_number, s.block_version, s.processing_version
        LIMIT 5) z;
    IF bad IS NOT NULL THEN
        RAISE WARNING 'projection % re-emits stored observations with a changed block_timestamp; stored rows kept (a real correction must bump block_version/processing_version): %', p_view, bad;
    END IF;
    IF bad_qty IS NOT NULL THEN
        RAISE WARNING 'projection % re-emits stored observations with a changed quantity; stored rows kept (append-only: a real correction must bump block_version/processing_version): %', p_view, bad_qty;
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
         deal_type_code)
    SELECT s.position_id, s.chain_id, s.protocol_id, s.instrument_key, s.holder_id, s.quantity,
           s.block_number, s.block_version, s.processing_version, s.block_timestamp, v_qualname,
           p_build_id, s.deal_type_code
    FROM pg_temp._mpp_src s
    WHERE NOT EXISTS (
        SELECT 1 FROM public.position_state p
        WHERE p.position_id = s.position_id AND p.block_number = s.block_number
          AND p.block_version = s.block_version AND p.processing_version = s.processing_version)
    ORDER BY s.block_timestamp, s.position_id, s.block_number, s.block_version, s.processing_version
    ON CONFLICT (position_id, block_number, block_version, processing_version, block_timestamp) DO NOTHING;
    GET DIAGNOSTICS n = ROW_COUNT;

    DROP TABLE pg_temp._mpp_src;

    RETURN n;
END $fn$;

COMMENT ON FUNCTION materialize_position_projection(regclass, integer) IS '[Operational] VEC-402..407 shared materializer: validate a per-protocol projection view against the position_state column contract, fail hard on contract/type drift, double-emitted keys, or cross-view ownership violations; keep-stored-and-warn on a re-emitted key whose block_timestamp or quantity drifted, then -- evaluating the projection ONCE into a temp table every check reads -- APPEND the new observations. deal_type_code is copied when the view emits it as text and left NULL otherwise: OPTIONAL, not part of the required contract, so a projection omitting it still works. position_id is recomputed via position_id(); serialized per view by an advisory lock on the view''s canonical name. Idempotent; run out of band. Returns rows INSERTED.';

INSERT INTO migrations (filename) VALUES ('20260904_120000_add_position_state_deal_type_code.sql') ON CONFLICT (filename) DO NOTHING;
