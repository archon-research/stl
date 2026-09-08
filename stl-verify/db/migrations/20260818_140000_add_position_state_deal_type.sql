-- VEC-401: record the deal type on the observation, since it cannot be derived from position_state.
-- Sorts directly after the spine (20260818_130000) and before every projection and cache, which is what
-- lets those files reference deal_type without each declaring the column.
-- Named deal_type, matching ref_deal_type.deal_type and its unsuffixed sibling `direction`. No
-- canonical column in the register carries a _code suffix, so this does not introduce the first.
-- The Morpho market loan leg nets supply against borrow as abs(supply - borrow), so the sign -- and
-- with it LOAN vs BORROW -- is destroyed at projection time and no query recovers it.

-- Nullable, and NOT in the materializer's required contract: the vault and Sky legs are constant per
-- instrument and the collateral leg is implied by instrument_key, so only the market loan leg has
-- anything to say. Added now because a later backfill would need a superuser.

-- FK'd to ref_deal_type, like position_classification.deal_type already is. An earlier revision
-- argued the RI probe would trip on this table's revoked UPDATE; that was wrong -- the probe needs
-- UPDATE on the PARENT, which 20260714_160000 restored.
ALTER TABLE position_state ADD COLUMN IF NOT EXISTS deal_type text;

-- Guarded because ADD CONSTRAINT has no IF NOT EXISTS and the rest of this file is re-runnable.
-- Measured on pg18.6/ts2.29.2: enforced from stl_readwrite and after compress_chunk, and drop_chunks
-- still works.
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

COMMENT ON COLUMN position_state.deal_type IS 'Derived, nullable. Deal type of THIS observation (LOAN / BORROW / COLLATERAL), stamped by the projection because it is not recoverable from the stored row: the Morpho market loan leg nets supply against borrow, so quantity carries the magnitude and this column carries the direction. NULL where the projection emits none -- the vault and Sky legs are constant per instrument and the collateral leg is implied by instrument_key, so a reader derives those. Roles: FK->ref_deal_type.deal_type, which is what constrains the value for EVERY writer, including a direct INSERT the materializer never sees.';

-- One row per completed materializer run per projection. Turns "this row is old" into a checkable
-- fact: a state-reading projection re-observes every position it can see on each run, so a position
-- whose latest observation trails the projection's latest completed run was swept and not seen.
-- Plain table: one row per run per projection is rows-per-hour, not time-series volume.
CREATE TABLE IF NOT EXISTS position_projection_run (
    projection    text        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    build_id      integer     NOT NULL,
    block_number  bigint,
    CONSTRAINT position_projection_run_pkey PRIMARY KEY (projection, created_at),
    CONSTRAINT position_projection_run_build_nonneg_chk CHECK (build_id >= 0),
    CONSTRAINT position_projection_run_block_nonneg_chk CHECK (block_number IS NULL OR block_number >= 0)
);

COMMENT ON TABLE position_projection_run IS '[Operational] One row per COMPLETED materialize_position_projection() run, written in the same transaction as the run''s inserts, so a failed run leaves no row. Freshness of a position: its projection''s latest row here is the point up to which it was swept; a position whose position_current.block_number trails that row''s block_number was re-swept and not re-observed. Append-only; no compression or tiering because volume is one row per run per projection.';
COMMENT ON COLUMN position_projection_run.projection IS 'Roles: PK. The projection view''s canonical name, as stamped on position_state.projection.';
COMMENT ON COLUMN position_projection_run.created_at IS 'Roles: PK. When the run completed. UTC.';
COMMENT ON COLUMN position_projection_run.build_id IS 'Roles: Audit. build_registry.id of the run (0 = pre-tracking).';
COMMENT ON COLUMN position_projection_run.block_number IS 'Derived. Highest block_number the projection emitted in this run; NULL when it emitted nothing, which is still a completed sweep.';

GRANT SELECT ON position_projection_run TO stl_readonly;
GRANT SELECT, INSERT ON position_projection_run TO stl_readwrite;
REVOKE UPDATE, DELETE ON position_projection_run FROM stl_readwrite;

-- Off-chain observations (custody snapshots) have no block. They carry chain_id NULL, protocol_id NULL
-- (a protocol row needs a chain address), and block_number = the snapshot instant in epoch seconds --
-- derived, so the materializer can check it, and distinct per snapshot, which the observation key needs.
COMMENT ON COLUMN position_state.block_number IS 'Block of the observation for an on-chain projection (chain_id set). For an OFF-CHAIN observation (chain_id NULL, a custody snapshot) it is floor(epoch seconds of block_timestamp): derived, distinct per snapshot, enforced by the materializer, and not a block on any chain. A reader asking "as of block N" reads chain_id-NULL rows by block_timestamp. chain_id is hashed into position_id, so one position is off-chain or on-chain for life.';

-- Body copied from 20260818_130000 with one change: deal_type is resolved per view and carried
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
    FROM (VALUES ('quantity'), ('block_timestamp'), ('deal_type')) AS e(col)
    JOIN pg_catalog.pg_attribute a ON a.attrelid = p_view AND a.attname = e.col
         AND a.attnum > 0 AND NOT a.attisdropped
    JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
    WHERE a.atttypmod <> -1
      AND ((e.col = 'quantity'        AND ((a.atttypmod - 4) & 65535) < 18)
        OR (e.col = 'block_timestamp' AND a.atttypmod < 6)
        -- The FK cannot catch this one: a narrow declared type truncates on cast and the truncation
        -- can land on ANOTHER valid code, so CUSTODY_COLLATERAL as varchar(7) stores CUSTODY.
        OR (e.col = 'deal_type'  AND t.typcategory = 'S' AND (a.atttypmod - 4) < 63));
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % declares a lossy type for a value column (it would silently round or truncate; widen the view''s cast): %', p_view, bad;
    END IF;

    -- OPTIONAL: absent stores NULL, present is cast and copied. No type branch -- whatever a wrong
    -- type casts to is not a ref_deal_type code, so the FK rejects it.
    v_deal_type := CASE WHEN EXISTS (
                       SELECT 1 FROM pg_catalog.pg_attribute a
                        WHERE a.attrelid = p_view AND a.attname = 'deal_type'
                          AND a.attnum > 0 AND NOT a.attisdropped)
                   THEN 'deal_type::text' ELSE 'NULL::text' END;

    DROP TABLE IF EXISTS pg_temp._mpp_src;
    DROP TABLE IF EXISTS pg_temp._mpp_new;
    EXECUTE format($q$
        CREATE TEMP TABLE _mpp_src ON COMMIT DROP AS
        SELECT public.position_id(chain_id, protocol_id, instrument_key, holder_id) AS position_id,
               chain_id, protocol_id, instrument_key, holder_id, quantity,
               block_number, block_version, processing_version, block_timestamp,
               %2$s AS deal_type
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
    SELECT string_agg(msg, '; ') INTO bad FROM (
        SELECT format('pos=%s bn=%s bv=%s pv=%s stored=%s emitted=%s', encode(s.position_id, 'hex'),
                      s.block_number, s.block_version, s.processing_version,
                      coalesce(p.deal_type, 'NULL'), coalesce(s.deal_type, 'NULL')) AS msg
        FROM pg_temp._mpp_src s
        JOIN public.position_state p ON p.position_id = s.position_id AND p.block_number = s.block_number
             AND p.block_version = s.block_version AND p.processing_version = s.processing_version
        WHERE p.deal_type IS DISTINCT FROM s.deal_type
        ORDER BY s.position_id, s.block_number, s.block_version, s.processing_version
        LIMIT 5) z;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % re-emits stored observations with a different deal_type, which this function CANNOT apply: the insert is suppressed on the stored key and UPDATE is revoked, so a silent no-op would leave the direction wrong forever. Append a higher processing_version instead: %', p_view, bad;
    END IF;

    IF bad_qty IS NOT NULL THEN
        RAISE WARNING 'projection % re-emits stored observations with a changed quantity; stored rows kept (append-only: a real correction must bump block_version/processing_version): %', p_view, bad_qty;
    END IF;

    -- Off-chain rows (chain_id NULL) must carry block_number = floor(epoch of block_timestamp). The
    -- observation key is (position, block, versions), so snapshots need distinct blocks; deriving the
    -- value from the instant makes it deterministic and checkable rather than a convention.
    SELECT string_agg(format('bn=%s ts=%s', s.block_number, s.block_timestamp), '; ') INTO bad FROM (
        SELECT block_number, block_timestamp FROM pg_temp._mpp_src
         WHERE chain_id IS NULL
           AND block_number <> floor(extract(epoch FROM block_timestamp))::bigint
         ORDER BY block_timestamp LIMIT 5) s;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % emits off-chain rows (chain_id NULL) whose block_number is not floor(epoch seconds of block_timestamp): %', p_view, bad;
    END IF;

    -- Within one position a higher block cannot carry an EARLIER instant. The caches order by block_number
    -- and read the date from block_timestamp, so a violation makes position_current and position_daily
    -- disagree about the newest observation. Governs rows that will be INSERTED, checked against each
    -- other and the stored history; a re-emit of a stored key is the drift path above, not this one.
    CREATE TEMP TABLE _mpp_new ON COMMIT DROP AS
        SELECT s.position_id, s.block_number, s.block_timestamp FROM pg_temp._mpp_src s
        WHERE NOT EXISTS (SELECT 1 FROM public.position_state p
                           WHERE p.position_id = s.position_id AND p.block_number = s.block_number
                             AND p.block_version = s.block_version AND p.processing_version = s.processing_version);
    SELECT string_agg(msg, '; ') INTO bad FROM (
        SELECT format('pos=%s bn=%s@%s vs bn=%s@%s', encode(b.position_id, 'hex'),
                      b.block_number, b.block_timestamp, a.block_number, a.block_timestamp) AS msg
        FROM pg_temp._mpp_new b
        JOIN (SELECT position_id, block_number, block_timestamp FROM pg_temp._mpp_new
              UNION ALL
              SELECT p.position_id, p.block_number, p.block_timestamp FROM public.position_state p
               WHERE p.position_id IN (SELECT DISTINCT position_id FROM pg_temp._mpp_new)) a
          ON a.position_id = b.position_id
         AND ((b.block_number > a.block_number AND b.block_timestamp < a.block_timestamp)
           OR (b.block_number < a.block_number AND b.block_timestamp > a.block_timestamp))
        ORDER BY b.position_id, b.block_number
        LIMIT 5) z;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % emits a higher block with an earlier block_timestamp for one position; the caches order by block and date by timestamp, so they would disagree about the newest observation: %', p_view, bad;
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

    INSERT INTO public.position_projection_run (projection, build_id, block_number)
    SELECT v_qualname, p_build_id, max(block_number) FROM pg_temp._mpp_src;

    DROP TABLE pg_temp._mpp_src;
    DROP TABLE pg_temp._mpp_new;

    RETURN n;
END $fn$;

COMMENT ON FUNCTION materialize_position_projection(regclass, integer) IS '[Operational] VEC-402..407 shared materializer: validate a per-protocol projection view against the position_state column contract, fail hard on contract/type drift, double-emitted keys, a higher block carrying an earlier block_timestamp within one position, an off-chain row (chain_id NULL) whose block_number is not its instant in epoch seconds, or cross-view ownership violations; keep-stored-and-warn on a re-emitted key whose block_timestamp or quantity drifted, then -- evaluating the projection ONCE into a temp table every check reads -- APPEND the new observations. deal_type is OPTIONAL, not part of the required contract, so a projection omitting it still works and stores NULL; a projection emitting it as any string type has the value copied, and one emitting a lossily-narrow string type, or a value that changes a STORED observation''s deal type, is REJECTED. Everything else about the value is the table''s FK to ref_deal_type, not this function''s job. position_id is recomputed via position_id(); serialized per view by an advisory lock on the view''s canonical name. Records the completed run in position_projection_run, in the same transaction. Idempotent; run out of band. Returns rows INSERTED.';

-- position_classification is retired. It was a classification engine over the spine, but the engine
-- is the projection's CASE expression and its result now lands on the observation, where a position
-- that flips LOAN/BORROW can be represented; a second, MUTABLE copy per position cannot, and nothing
-- ever wrote it. direction derives from ref_deal_type; collateral_status was unused. Project lead decision.
DROP TABLE IF EXISTS position_classification;

INSERT INTO migrations (filename) VALUES ('20260818_140000_add_position_state_deal_type.sql') ON CONFLICT (filename) DO NOTHING;
