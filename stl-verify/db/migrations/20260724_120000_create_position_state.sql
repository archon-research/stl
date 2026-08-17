-- VEC-402..409: position_state — the shared spine every per-protocol position materializer writes into.
--
-- One row per (native position identity, observation). Position identity is NATIVE ONLY (VEC-400):
--   position_id = position_id(chain_id, protocol_id, instrument_key, holder_id)
-- No mapped / interpreted value enters the id. instrument_key and holder_id are the source's own
-- native ids (contract address / market id / registry:ilk / provider:package, and the on-chain holder
-- address or prime vault address), never one of our classifications. Classifications (deal_type,
-- direction) live in position_classification (VEC-401), keyed by position_id; they are attributes, not
-- part of identity.
--
-- quantity is a single canonical amount: the holder's balance in the instrument's own native units.
-- Per-protocol amount breakdowns (Morpho supply/borrow/collateral shares, Aave scaled balances, ...)
-- stay in the raw source; this spine carries the one number every consumer needs plus the resolution
-- keys. The per-protocol materializers (VEC-402..408) fan a raw row out by NATIVE INSTRUMENT only
-- (e.g. a Morpho market row -> its loan-token position and its collateral-token position), never by a
-- house leg/deal_type classifier.
--
-- Observation axis: (block_number, block_version, processing_version), all NOT NULL and part of the
-- PK. processing_version defaults 0 for sources that don't carry one (Morpho). The current state per
-- position is position_current (VEC-409, DISTINCT ON position_id) — built once all materializers land.
--
-- Scope: block-observed sources only. A snapshot-keyed source (e.g. Anchorage's
-- anchorage_package_snapshot, keyed by snapshot_time with no block axis) is NOT supported by this
-- spine as written, and must not be forced onto the block axis by a hack: a constant block_number
-- collapses every snapshot of a position onto one PK row and the upsert destroys custody history,
-- and encoding snapshot_time into block_number corrupts the column's semantics. Supporting such a
-- source requires an explicit, deliberate schema change first (a snapshot/observation-time
-- discriminator in the PK), decided when the first snapshot-keyed materializer is built — not a
-- mapping improvised at that materializer's call site.
--
-- Hypertable partitioned on block_timestamp (mirrors the transformed bucket1 derived tables): a
-- curated/derived spine of one row per (position, observation) that grows without bound as blocks
-- accrue. 1-day chunks; compression after 2 days (segmentby position_id, so a position's observations
-- compress together, ordered block DESC for latest-first reads); S3 tiering after 1 year. Writes are
-- the out-of-band full-projection upsert in the materializer helper below (the transform _bootstrap
-- shape); a trigger-fed incremental _run path is the follow-up for when a scheduled runner and real
-- volume make the full upsert's decompression cost matter.

CREATE TABLE IF NOT EXISTS position_state (
    position_id        bytea       NOT NULL,
    chain_id           integer,                       -- native; nullable per position_id's structural-field convention
    protocol_id        bigint,                        -- native; nullable per convention
    instrument_key     text        NOT NULL,          -- native instrument id (VEC-412 bridge key); resolves security via security_instrument_bridge_current
    holder_id          text        NOT NULL,          -- native on-chain holder (wallet / prime vault address, lowercase hex, no 0x); resolves entity downstream (VEC-417)
    quantity           numeric     NOT NULL,          -- holder's balance in the instrument's native units
    block_number       bigint      NOT NULL,
    block_version      integer     NOT NULL DEFAULT 0,
    processing_version integer     NOT NULL DEFAULT 0,
    block_timestamp    timestamptz NOT NULL,          -- on-chain observation time
    created_at         timestamptz NOT NULL DEFAULT now(),
    -- block_timestamp is in the PK because it is the hypertable partition column (Timescale requires the
    -- partition column in every unique constraint). It is functionally determined by block_number, so the
    -- 5-column key is unique over the same observations as (position_id, block_number, block_version,
    -- processing_version) — the upsert arbiter in the helper below matches it exactly.
    CONSTRAINT position_state_pkey PRIMARY KEY (position_id, block_number, block_version, processing_version, block_timestamp),
    -- position_id is sha256() output: enforce the 32-byte width (bytea is unlength-modified), matching
    -- position_classification (Simon review on #572).
    CONSTRAINT position_state_id_len_chk CHECK (octet_length(position_id) = 32),
    -- quantity is a non-negative native magnitude; direction (BORROW/short vs supply/long) lives in
    -- position_classification, never as a sign here. Every materializer emits abs()/balance/debt >= 0
    -- (verified: 0 negative rows across morpho/vault/sky on prod). Fail hard if a bad source amount
    -- would write a negative "exposure" that exposure queries (quantity <> 0) would silently surface.
    -- Exclude the non-finite numerics explicitly: in Postgres both 'NaN' and 'Infinity' sort above every
    -- finite numeric, so they clear quantity >= 0 and the quantity > 0 classification filter, then poison
    -- every downstream SUM(quantity). '< Infinity' rejects both +Infinity and NaN; '<> NaN' is kept for
    -- readability. (-Infinity and negatives are rejected by quantity >= 0.)
    CONSTRAINT position_state_qty_nonneg_chk
        CHECK (quantity >= 0 AND quantity <> 'NaN'::numeric AND quantity < 'Infinity'::numeric)
);

COMMENT ON TABLE position_state IS '[Hypertable] Shared spine for materialized positions (VEC-402..408), partitioned on block_timestamp. One row per (native position_id, observation): the native resolution keys (instrument_key -> security via the bridge; holder_id -> entity via VEC-417) plus a single canonical quantity. Identity is native-only (VEC-400); classifications live in position_classification. Current state per position is position_current (VEC-409).';
COMMENT ON COLUMN position_state.position_id IS 'PK. bytea(32) native identity from position_id() (VEC-400): hash(chain_id, protocol_id, instrument_key, holder_id). No mapped value in the hash.';
COMMENT ON COLUMN position_state.chain_id IS 'Native chain id. Nullable per the position_id structural-field convention; each materializer uses a fixed NULL-ness convention.';
COMMENT ON COLUMN position_state.protocol_id IS 'Native protocol id. Nullable per convention.';
COMMENT ON COLUMN position_state.instrument_key IS 'Native, globally-unique instrument id (the security_instrument_bridge key). Resolves to a security via security_instrument_bridge_current -> security_master_current (VEC-420).';
COMMENT ON COLUMN position_state.holder_id IS 'Native on-chain holder id (wallet or prime vault address, lowercase hex without 0x). Resolves to an entity downstream (VEC-417); not resolved here so the id stays computable without a master lookup.';
COMMENT ON COLUMN position_state.quantity IS 'Holder balance in the instrument''s own native units. Scale is source-defined and NOT normalized across protocols (e.g. Morpho shares are raw integers at the loan token''s decimals; Sky/prime flow amounts are decimal-normalized), so each materializer''s projection documents its unit/scale, and a consumer must not SUM across heterogeneous instruments without normalizing. Per-protocol amount breakdowns stay in the raw source.';
COMMENT ON COLUMN position_state.block_number IS 'PK. Block height of the observation.';
COMMENT ON COLUMN position_state.block_version IS 'PK. Reorg version of the block.';
COMMENT ON COLUMN position_state.processing_version IS 'PK. Pipeline processing version; 0 for sources without one (e.g. Morpho).';
COMMENT ON COLUMN position_state.block_timestamp IS 'Partition. On-chain observation time (UTC); the hypertable partition column, and part of the PK.';
COMMENT ON COLUMN position_state.created_at IS 'Audit. Row insert time.';

-- Hypertable on block_timestamp, 1-day chunks (matches the transformed bucket1 position tables).
SELECT create_hypertable('position_state', 'block_timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
-- Compress closed chunks: segment by position_id so a position's observations compress together and
-- latest-first (block DESC) reads stay cheap; compress chunks older than 2 days.
ALTER TABLE position_state SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'position_id',
    timescaledb.compress_orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);
SELECT add_compression_policy('position_state', INTERVAL '2 days', if_not_exists => TRUE);
-- Tier cold chunks to S3 after 1 year. add_tiering_policy is a Timescale Cloud/TigerData primitive;
-- guard so the migration still applies where it is unavailable: the function is absent on plain
-- TimescaleDB (undefined_function), and a Cloud service without tiered storage enabled raises a
-- feature_not_supported (0A000). Catch both, or the error escapes and rolls back the whole migration.
DO $$ BEGIN
    PERFORM add_tiering_policy('position_state', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function OR feature_not_supported THEN
    RAISE NOTICE 'add_tiering_policy unavailable (%), skipping tiering for position_state', SQLERRM;
END $$;

-- No dedicated current-state index. On UNCOMPRESSED chunks, latest-observation lookups (the per-position
-- WHERE position_id = $1 ORDER BY ... LIMIT 1, and VEC-409's global DISTINCT ON) are served by an
-- Index Scan Backward over each chunk's PK btree (not Index Only — quantity/instrument_key/holder_id are
-- not in the PK), merged with no Sort, provided the query spells its ORDER BY fully descending
-- (position_id DESC, block_number DESC, block_version DESC, processing_version DESC). NOTE (VEC-409):
-- once chunks compress (2-day policy) the PK btree is gone from them, and the GLOBAL latest-per-position
-- DISTINCT ON degrades to a per-chunk Seq Scan + Sort over columnstore data; only the per-position
-- (segmentby position_id) lookup stays cheap. So position_current needs a different shape for compressed
-- data — a maintained uncompressed current-state table, or the incremental path (VEC-566). No reverse-
-- lookup (instrument_key / holder_id) indexes here: they have no consumer yet (VEC-417/420 unbuilt),
-- cost an index write per row, and exist only on uncompressed chunks — add them in the first consumer PR.

GRANT SELECT ON position_state TO stl_readonly;
-- Append-only: corrections arrive as new (block_version, processing_version) rows, so no DELETE/TRUNCATE.
-- The roles migration's ALTER DEFAULT PRIVILEGES grants stl_readwrite full DML on every stl_migrator
-- table, so the narrowed GRANT below does not remove DELETE by itself — the explicit REVOKE does. (A
-- dedicated narrow materializer role, VEC-562, is the broader least-privilege fix and pins search_path.)
GRANT SELECT, INSERT, UPDATE ON position_state TO stl_readwrite;
REVOKE DELETE, TRUNCATE ON position_state FROM stl_readwrite;

-- Shared materializer body for every per-protocol projection (VEC-402..408). Each projection view
-- (position_morpho_market, position_morpho_vault, position_sky_prime_debt, ...) holds its own bespoke
-- projection logic but emits the identical position_state COLUMN CONTRACT — (chain_id integer,
-- protocol_id bigint, instrument_key text, holder_id text, quantity numeric, deal_type_code text,
-- block_number bigint, block_version integer, processing_version integer, block_timestamp timestamptz).
-- position_id is NOT part of the contract: the helper recomputes it from the identity fields via the
-- canonical IMMUTABLE position_id(), so a view cannot mint an id that stores but joins to nothing.
-- This function is the identical plumbing shared by all of them (CLAUDE.md: consolidate duplicated
-- code) — it upserts the observations into the spine and the current (latest NON-ZERO) deal-type into
-- position_classification (VEC-401). A closed position keeps the deal_type of its last real observation,
-- not the ambiguous direction of a closing zero-row.
--
-- The contract is ENFORCED, not trusted: before touching data the helper (1) validates p_view emits
-- every contract column at its exact type (catches silent assignment-casts, e.g. a float8 quantity
-- corrupting precision), (2) fails hard if the view double-emits a logical observation key
-- (position_id, block_number, block_version, processing_version) rather than relying on SQLSTATE 21000,
-- and (3) fails hard if the view re-emits an existing observation with a CHANGED block_timestamp — that
-- would insert a duplicate under the block_timestamp-in-PK arbiter; a genuine correction must bump
-- block_version. Given (2)/(3), block_timestamp for a logical key is invariant, so the upsert updates
-- only quantity.
--
-- Classification recency: the current deal-type is written only from an observation at least as recent
-- (by block_number) as the position's already-stored latest, so a windowed backfill run does NOT
-- regress a newer live classification. A position whose entire canonical history is zero produces NO
-- classification row (there is no non-zero observation to classify) — this is legal-but-unclassified;
-- consumers and VEC-409 must LEFT JOIN position_classification, never inner-join.
--
-- collateral_status is not written here (stays NULL). When a materializer needs it (Anchorage CUSTODY
-- vs CUSTODY_COLLATERAL, VEC-408), add it as one more nullable column to THIS guarded upsert, not a
-- separate write — a separate write leaves a stale status attached across a reclassification.
--
-- p_view is a regclass, so it must name an existing relation — no SQL injection via the dynamic FROM.
-- search_path is intentionally NOT pinned on the function: pinning breaks the per-schema integration
-- test harness (sibling table migrations don't pin either). The out-of-band runner must connect with a
-- safe search_path; the dedicated stl_materialize role (VEC-562) pins it (ALTER ROLE ... SET search_path).
-- Idempotent; run out of band. Returns the number of position_state rows actually inserted or changed.
CREATE OR REPLACE FUNCTION materialize_position_projection(p_view regclass, p_reason text)
    RETURNS bigint
    LANGUAGE plpgsql AS $fn$
DECLARE n bigint; bad text;
BEGIN
    -- Serialize concurrent runs of the SAME projection (cron overlap, scheduled + manual backfill).
    -- Name-hash key (not p_view::oid): stable across a DROP+CREATE of the view, and mappable in pg_locks
    -- during incidents; matches the transformed _run_* / trigger lock sites.
    PERFORM pg_advisory_xact_lock(hashtextextended('materialize_position_projection.' || p_view::text, 0));

    -- (1) Enforce the column contract (name + exact type) before trusting the view.
    SELECT string_agg(e.col || ' (' || COALESCE('is ' || format_type(a.atttypid, a.atttypmod), 'MISSING') || ')', ', ')
      INTO bad
    FROM (VALUES ('chain_id','integer'),('protocol_id','bigint'),('instrument_key','text'),('holder_id','text'),
                 ('quantity','numeric'),('deal_type_code','text'),('block_number','bigint'),
                 ('block_version','integer'),('processing_version','integer'),('block_timestamp','timestamp with time zone')
         ) AS e(col, typ)
    LEFT JOIN pg_attribute a ON a.attrelid = p_view AND a.attname = e.col AND a.attnum > 0 AND NOT a.attisdropped
    WHERE a.attname IS NULL OR format_type(a.atttypid, a.atttypmod) <> e.typ;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % violates the position_state column contract: %', p_view, bad;
    END IF;

    -- (2) Fail hard on a double-emitted logical observation key (do not rely on 21000).
    EXECUTE format($chk$
        SELECT string_agg(msg, '; ') FROM (
            SELECT format('bn=%%s bv=%%s pv=%%s x%%s', block_number, block_version, processing_version, count(*)) msg
            FROM (SELECT position_id(chain_id, protocol_id, instrument_key, holder_id) AS pid,
                         block_number, block_version, processing_version FROM %1$s) s
            GROUP BY pid, block_number, block_version, processing_version
            HAVING count(*) > 1 LIMIT 5
        ) z
    $chk$, p_view) INTO bad;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % double-emits a logical observation key (position_id,block_number,block_version,processing_version): %', p_view, bad;
    END IF;

    -- (3) Fail hard if a re-emitted observation carries a changed block_timestamp (would duplicate under the arbiter).
    EXECUTE format($chk$
        SELECT string_agg(msg, '; ') FROM (
            SELECT format('bn=%%s bv=%%s pv=%%s', s.block_number, s.block_version, s.processing_version) msg
            FROM (SELECT position_id(chain_id, protocol_id, instrument_key, holder_id) AS pid,
                         block_number, block_version, processing_version, block_timestamp FROM %1$s) s
            JOIN position_state p ON p.position_id = s.pid AND p.block_number = s.block_number
                 AND p.block_version = s.block_version AND p.processing_version = s.processing_version
            WHERE p.block_timestamp <> s.block_timestamp LIMIT 5
        ) z
    $chk$, p_view) INTO bad;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % re-emits observations with a changed block_timestamp (a real correction must bump block_version): %', p_view, bad;
    END IF;

    -- One evaluation feeds both writes (src AS MATERIALIZED, single snapshot via data-modifying CTEs).
    EXECUTE format($q$
        WITH src AS MATERIALIZED (
            SELECT position_id(chain_id, protocol_id, instrument_key, holder_id) AS position_id,
                   chain_id, protocol_id, instrument_key, holder_id, quantity, deal_type_code,
                   block_number, block_version, processing_version, block_timestamp
            FROM %1$s
        ),
        ins AS (
            INSERT INTO position_state
                (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
                 block_number, block_version, processing_version, block_timestamp)
            SELECT position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
                   block_number, block_version, processing_version, block_timestamp
            FROM src
            -- Insert in PK order so overlapping runs of different views acquire row locks in the same
            -- order and cannot deadlock (migrations AGENTS.md sorted-key-order rule).
            ORDER BY position_id, block_number, block_version, processing_version, block_timestamp
            ON CONFLICT (position_id, block_number, block_version, processing_version, block_timestamp) DO UPDATE
                -- quantity is the only mutable non-key column; block_timestamp is invariant for a logical
                -- key (enforced by pre-flight (3)), and the identity columns are fixed by position_id.
                SET quantity = EXCLUDED.quantity
                WHERE position_state.quantity IS DISTINCT FROM EXCLUDED.quantity
            RETURNING 1
        ),
        canonical AS (
            SELECT DISTINCT ON (position_id, block_number)
                   position_id, block_number, quantity, deal_type_code
            FROM src
            ORDER BY position_id, block_number, block_version DESC, processing_version DESC
        ),
        latest AS (
            SELECT DISTINCT ON (position_id)
                   position_id, deal_type_code, block_number
            FROM canonical
            WHERE quantity > 0
            ORDER BY position_id, block_number DESC
        ),
        -- Pre-run latest block per position (same snapshot, so it excludes this run's ins). Used to
        -- refuse a classification write from an older-block (backfill/window) run.
        stored_max AS (
            SELECT position_id, max(block_number) AS max_blk
            FROM position_state
            WHERE position_id IN (SELECT position_id FROM latest)
            GROUP BY position_id
        ),
        cls AS (
            INSERT INTO position_classification (position_id, deal_type_code, direction, change_reason)
            -- LEFT JOIN + raw deal_type_code: an unseeded / typo'd code must hit the deal_type_code FK
            -- (23503) and fail the run, not be silently dropped by an inner join (CLAUDE.md: fail hard).
            SELECT l.position_id, l.deal_type_code, d.direction, %2$L
            FROM latest l
            LEFT JOIN stored_max s ON s.position_id = l.position_id
            LEFT JOIN ref_deal_type d ON d.deal_type = l.deal_type_code
            -- Recency guard: only (re)classify from an observation at least as recent as the stored
            -- latest, so a windowed backfill run cannot regress a newer live classification.
            WHERE l.block_number >= COALESCE(s.max_blk, -1)
            ORDER BY l.position_id
            ON CONFLICT (position_id) DO UPDATE
                SET deal_type_code = EXCLUDED.deal_type_code,
                    direction      = EXCLUDED.direction,
                    change_reason  = EXCLUDED.change_reason,
                    -- re-stamp valid_from + change_reason only when the classification actually changes.
                    valid_from     = (now() AT TIME ZONE 'utc')::date
                WHERE position_classification.deal_type_code IS DISTINCT FROM EXCLUDED.deal_type_code
                   OR position_classification.direction      IS DISTINCT FROM EXCLUDED.direction
        )
        SELECT count(*) FROM ins
    $q$, p_view, p_reason) INTO n;

    RETURN n;
END $fn$;

COMMENT ON FUNCTION materialize_position_projection(regclass, text) IS '[Operational] VEC-402..408 shared materializer: validate a per-protocol projection view against the position_state column contract, fail hard on contract/type drift, double-emitted keys, or a changed block_timestamp, then in one snapshot upsert its observations into position_state and its current latest-non-zero deal-type into position_classification (recency-guarded so a backfill window cannot regress a newer classification; an all-zero position stays unclassified). position_id is recomputed via position_id(); runs serialized per view by a name-hash advisory lock; guarded upserts skip no-op reruns. Idempotent; run out of band. Returns position_state rows inserted or changed.';

INSERT INTO migrations (filename) VALUES ('20260724_120000_create_position_state.sql') ON CONFLICT (filename) DO NOTHING;
