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
-- keys. The per-protocol materializers (VEC-402..407) fan a raw row out by NATIVE INSTRUMENT only
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
-- accrue. 1-day chunks. Writes are the out-of-band full-projection upsert in the materializer helper
-- below, which re-projects and re-upserts the whole history every run.
--
-- Compression is DEFERRED to VEC-566, not set here, and this is deliberate: the full-projection upsert
-- re-touches every chunk, and ON CONFLICT decompresses each compressed chunk it checks before the
-- no-op guard runs — at scale that exceeds max_tuples_decompressed_per_dml_transaction and the run
-- aborts (SQLSTATE 53400, verified on 2.29.1). Compression only becomes safe once the write path is
-- incremental (only touches recent, uncompressed chunks), so the compression policy ships WITH the
-- trigger-fed _run/_bootstrap write path in VEC-566, not before it. The hypertable + PK are set now
-- (cheap while the table is empty; the PK-ends-in-block_timestamp shape can't be changed cheaply later).
-- (S3 tiering below is set now but has the same latent full-upsert interaction — it just can't bite
-- for ~1 year and is Cloud-only, so unverified; revisit alongside VEC-566.)

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
    -- partition column in every unique constraint). It is invariant PER LOGICAL KEY (position_id,
    -- block_number, block_version, processing_version) — enforced by the helper's pre-flight check (3) —
    -- so the 5-column key is unique over the same observations as the 4-column key, and the upsert arbiter
    -- below matches it exactly. It is NOT a table-wide function of block_number: block_timestamp is each
    -- source's observation time, and an event-time source (Sky prime_debt uses synced_at) can legitimately
    -- give two positions at the same block different timestamps.
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
        CHECK (quantity >= 0 AND quantity <> 'NaN'::numeric AND quantity < 'Infinity'::numeric),
    -- holder_id must be lowercase hex, no 0x — both documented holder forms (wallet, prime vault
    -- address) are. position_id() cannot normalise (the convention is the materializer's contract), and
    -- this table is the single chokepoint every materializer writes through: one materializer emitting
    -- '0xAbC…' while another emits 'abc…' would fork one wallet into two position_ids, and a
    -- non-conforming holder silently fails the VEC-417 lowercase-hex holder join (entity_ref_codes
    -- CHECK-enforces the same pattern on the other side). instrument_key stays unchecked — heterogeneous
    -- native forms (registry:ilk, provider:package) are legitimate there.
    CONSTRAINT position_state_holder_hex_chk CHECK (holder_id ~ '^[0-9a-f]+$'),
    -- Observation coordinates are non-negative by definition (genesis is block 0; versions count from 0).
    -- quantity had a CHECK but the coordinates did not, so a source bug emitting a negative height was
    -- stored silently and — because the recency guard orders on these columns — became the position's
    -- "oldest" observation and skewed classification recency. Reject at the chokepoint instead.
    CONSTRAINT position_state_coord_nonneg_chk
        CHECK (block_number >= 0 AND block_version >= 0 AND processing_version >= 0),
    -- chain_id / protocol_id are nullable structural fields (see position_key), but when present they are
    -- registry ids and strictly positive; 0 or negative is an upstream default/corruption, and because the
    -- value feeds the position_id hash a wrong-but-accepted id forks the position permanently.
    CONSTRAINT position_state_chain_pos_chk    CHECK (chain_id IS NULL OR chain_id > 0),
    CONSTRAINT position_state_protocol_pos_chk CHECK (protocol_id IS NULL OR protocol_id > 0),
    -- block_timestamp is the partition column: a corrupted epoch-zero/1970 value (e.g. a hex-parse bug in
    -- a loader) would silently create a 1970 chunk and poison time-ordered reads. No blockchain predates
    -- Bitcoin's genesis (2009-01-03), so anything earlier is corruption, not data. Deliberately no upper
    -- bound: now()-relative CHECKs are non-immutable and would reject legitimate clock-skewed live blocks.
    CONSTRAINT position_state_ts_sane_chk CHECK (block_timestamp >= '2009-01-03 00:00:00+00'::timestamptz)
);

COMMENT ON TABLE position_state IS '[Hypertable] Shared spine for materialized positions (VEC-402..407), partitioned on block_timestamp. One row per (native position_id, observation): the native resolution keys (instrument_key -> security via the bridge; holder_id -> entity via VEC-417) plus a single canonical quantity. Identity is native-only (VEC-400); classifications live in position_classification. Current state per position is position_current (VEC-409).';
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
-- create_default_indexes => FALSE: the PK does NOT lead with block_timestamp, so Timescale's default
-- would silently add a block_timestamp-only index on every chunk that no documented access path uses
-- (all lead with position_id) — an extra index write per upsert. Reverse-lookup indexes are added by
-- whoever first needs them.
SELECT create_hypertable('position_state', 'block_timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE, create_default_indexes => FALSE);
-- Compression is intentionally NOT enabled here — it ships with the incremental write path in VEC-566
-- (see the table header for why the full-projection upsert and compression are incompatible).
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
-- (position_id DESC, block_number DESC, block_version DESC, processing_version DESC). NOTE for when
-- compression lands (VEC-566): a compressed chunk loses its PK btree, so the GLOBAL latest-per-position
-- DISTINCT ON will degrade to a per-chunk Seq Scan + Sort over columnstore data (only the per-position
-- segmentby lookup stays cheap), and position_current will need a different shape then — a maintained
-- uncompressed current-state table, or the incremental path. That does not bite today (no compression
-- yet). No reverse-lookup (instrument_key / holder_id) indexes here: no consumer yet (VEC-417/420
-- unbuilt) and they cost an index write per row — add them in the first consumer PR.

GRANT SELECT ON position_state TO stl_readonly;
-- Append-only: corrections arrive as new (block_version, processing_version) rows, so no DELETE/TRUNCATE.
-- The roles migration's ALTER DEFAULT PRIVILEGES grants stl_readwrite full DML on every stl_migrator
-- table, so narrowed GRANTs below do not remove anything by themselves — the explicit REVOKEs do. (A
-- dedicated narrow materializer role, VEC-562, is the broader least-privilege fix and pins search_path.)
--
-- UPDATE is COLUMN-SCOPED to quantity: the materializer's ON CONFLICT touches only quantity, and a
-- table-wide UPDATE would let the app role rewrite identity/coordinate columns (holder_id, block_number,
-- ...) on any historical row — "append-only" must hold against silent history rewrites, not just DELETE.
GRANT SELECT, INSERT ON position_state TO stl_readwrite;
REVOKE UPDATE, DELETE, TRUNCATE ON position_state FROM stl_readwrite;
GRANT UPDATE (quantity) ON position_state TO stl_readwrite;

-- Same narrowing for position_classification (created by an applied migration, so re-granted here, in the
-- migration that defines its only sanctioned writer). The cls upsert below SETs exactly deal_type_code,
-- direction, change_reason, valid_from. collateral_status is deliberately NOT grantable: nothing writes it
-- yet (see the collateral_status note below), and an out-of-band write would go stale across the next
-- reclassification with nothing to clear it — the future materializer PR that handles it adds the column
-- grant together with the upsert logic that keeps it consistent. position_id (identity) stays unwritable.
GRANT SELECT, INSERT ON position_classification TO stl_readwrite;   -- the upsert's INSERT arm (idempotent re-grant)
GRANT SELECT ON ref_deal_type TO stl_readwrite;                      -- the direction lookup join (idempotent re-grant)
REVOKE UPDATE, DELETE, TRUNCATE ON position_classification FROM stl_readwrite;
GRANT UPDATE (deal_type_code, direction, change_reason, valid_from) ON position_classification TO stl_readwrite;

-- Shared materializer body for every per-protocol projection (VEC-402..407). Each projection view
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
-- block_version. The checks and both writes read ONE materialised snapshot of the projection (a temp
-- table), so block_timestamp for a logical key is invariant within the run and the upsert updates only
-- quantity. Cross-run safety rests on two invariants: the advisory lock serialises same-view runs, and
-- different projection views emit DISJOINT position_id sets by native-key construction (distinct
-- protocol / instrument / holder), so no concurrent run touches the position_ids this one checks.
--
-- Classification recency: the current deal-type is written only from an observation at least as recent
-- (by block_number) as the position's already-stored latest, so a windowed backfill run does NOT
-- regress a newer live classification. A position whose entire canonical history is zero produces NO
-- classification row (there is no non-zero observation to classify) — this is legal-but-unclassified;
-- consumers and VEC-409 must LEFT JOIN position_classification, never inner-join.
--
-- collateral_status is not written here (stays NULL). If a future BLOCK-OBSERVED materializer in this
-- spine's scope needs it, add it as one more nullable column to THIS guarded upsert (not a separate
-- write, which leaves a stale status attached across a reclassification). Anchorage custody (CUSTODY vs
-- CUSTODY_COLLATERAL) is snapshot-keyed and out of scope (see the Scope block), so it is NOT planned
-- into this upsert.
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
    -- p_reason stamps change_reason provenance on every classification write; a NULL/blank would erase
    -- provenance (and overwrite a prior reason with NULL on the update path). Fail fast on the argument.
    IF p_reason IS NULL OR btrim(p_reason) = '' THEN
        RAISE EXCEPTION 'materialize_position_projection(%): p_reason must be a non-empty change_reason', p_view;
    END IF;

    -- Serialize concurrent runs of the SAME projection (cron overlap, scheduled + manual backfill).
    -- Key on the view's CANONICAL schema-qualified name from the catalog, not p_view::text: the regclass
    -- text renders schema-qualified only when the relation is not visible unqualified, so it would hash
    -- differently for the pinned runner (explicit search_path) vs a plain psql session — same view, two
    -- keys, no mutual exclusion. quote_qualified_ident(nspname, relname) is search_path-independent.
    -- Contract: materialize AT MOST ONE view per transaction (or, if batching several, acquire them in
    -- canonical-name order) — the xact lock is held to commit, so two callers locking different views in
    -- different orders would deadlock (40P01) and lose the whole batch.
    PERFORM pg_advisory_xact_lock(hashtextextended(
        (SELECT format('materialize_position_projection.%I.%I', nsp.nspname, cls.relname)
           FROM pg_class cls JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
          WHERE cls.oid = p_view), 0));

    -- (1) Enforce the column contract (name + BASE type) before trusting the view. Compare on the base
    -- type with typmod stripped (format_type(atttypid, NULL)) so a losslessly assignment-compatible
    -- source column such as numeric(30,18) passes instead of failing an exact-string match against 'numeric'.
    SELECT string_agg(e.col || ' (' || COALESCE('is ' || format_type(a.atttypid, a.atttypmod), 'MISSING') || ')', ', ')
      INTO bad
    FROM (VALUES ('chain_id','integer'),('protocol_id','bigint'),('instrument_key','text'),('holder_id','text'),
                 ('quantity','numeric'),('deal_type_code','text'),('block_number','bigint'),
                 ('block_version','integer'),('processing_version','integer'),('block_timestamp','timestamp with time zone')
         ) AS e(col, typ)
    LEFT JOIN pg_attribute a ON a.attrelid = p_view AND a.attname = e.col AND a.attnum > 0 AND NOT a.attisdropped
    WHERE a.attname IS NULL OR format_type(a.atttypid, NULL::integer) <> e.typ;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % violates the position_state column contract: %', p_view, bad;
    END IF;

    -- Evaluate the projection EXACTLY ONCE into a temp table. The projection is the dominant cost (full
    -- raw scan + joins + LAG + per-row sha256); the pre-flight checks and both writes all read this
    -- materialized snapshot, so the view is never rescanned (was 3x). position_id() is recomputed here,
    -- keeping identity off the view's contract.
    -- pg_temp-qualified so an unqualified _mpp_src on the caller's search_path can never resolve to a
    -- permanent relation of the same name and be dropped; the cleanup still catches a leftover temp
    -- table from an earlier same-transaction call.
    DROP TABLE IF EXISTS pg_temp._mpp_src;
    EXECUTE format($q$
        CREATE TEMP TABLE _mpp_src ON COMMIT DROP AS
        SELECT position_id(chain_id, protocol_id, instrument_key, holder_id) AS position_id,
               chain_id, protocol_id, instrument_key, holder_id, quantity, deal_type_code,
               block_number, block_version, processing_version, block_timestamp
        FROM %1$s
    $q$, p_view);
    ANALYZE _mpp_src;

    -- (2) Fail hard on a double-emitted logical observation key (do not rely on 21000). Reads the temp
    -- snapshot, so no view rescan.
    SELECT string_agg(msg, '; ') INTO bad FROM (
        SELECT format('bn=%s bv=%s pv=%s x%s', block_number, block_version, processing_version, count(*)) AS msg
        FROM _mpp_src
        GROUP BY position_id, block_number, block_version, processing_version
        HAVING count(*) > 1 LIMIT 5) z;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % double-emits a logical observation key (position_id,block_number,block_version,processing_version): %', p_view, bad;
    END IF;

    -- (3) Fail hard if a re-emitted observation carries a changed block_timestamp (would duplicate under
    -- the arbiter). Same snapshot as the upsert; the per-view advisory lock serialises same-view runs and
    -- the header's disjointness contract keeps different views off each other's position_ids, so a
    -- concurrent run cannot slip a colliding row between this check and the upsert.
    SELECT string_agg(msg, '; ') INTO bad FROM (
        SELECT format('bn=%s bv=%s pv=%s', s.block_number, s.block_version, s.processing_version) AS msg
        FROM _mpp_src s
        JOIN position_state p ON p.position_id = s.position_id AND p.block_number = s.block_number
             AND p.block_version = s.block_version AND p.processing_version = s.processing_version
        WHERE p.block_timestamp <> s.block_timestamp LIMIT 5) z;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % re-emits observations with a changed block_timestamp (a real correction must bump block_version): %', p_view, bad;
    END IF;

    -- (4) Fail hard on a NULL deal_type_code for a NON-ZERO observation: a non-zero position must carry a
    -- classification code. Surface the projection defect loudly (naming the observation) rather than let it
    -- hit the NOT NULL classification insert as a raw 23502, or silently leave the position unclassified.
    SELECT string_agg(msg, '; ') INTO bad FROM (
        SELECT format('bn=%s bv=%s pv=%s', block_number, block_version, processing_version) AS msg
        FROM _mpp_src WHERE quantity > 0 AND deal_type_code IS NULL LIMIT 5) z;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % emits a NULL deal_type_code on a non-zero observation (every non-zero observation must carry a classification code): %', p_view, bad;
    END IF;

    -- Upsert the observations into the spine and the current latest-non-zero deal-type into
    -- position_classification. Static SQL over the temp snapshot (no view rescan); the data-modifying
    -- cls CTE runs to completion even though the top-level query reads only ins.
    WITH ins AS (
        INSERT INTO position_state
            (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
             block_number, block_version, processing_version, block_timestamp)
        SELECT position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
               block_number, block_version, processing_version, block_timestamp
        FROM _mpp_src
        -- Insert in PK order for B-tree bulk-load locality (sequential leaf writes). NOTE: this is NOT a
        -- cross-view lock-ordering guard — the header's view-disjointness contract makes two views' row
        -- sets disjoint, so concurrent runs never contend on the same position_state row; ordering here is
        -- a load-locality benefit, not a deadlock protection.
        ORDER BY position_id, block_number, block_version, processing_version, block_timestamp
        ON CONFLICT (position_id, block_number, block_version, processing_version, block_timestamp) DO UPDATE
            -- quantity is the only mutable non-key column; block_timestamp is invariant for a logical key
            -- (enforced by pre-flight (3)), and the identity columns are fixed by position_id.
            SET quantity = EXCLUDED.quantity
            WHERE position_state.quantity IS DISTINCT FROM EXCLUDED.quantity
        RETURNING 1
    ),
    canonical AS (
        SELECT DISTINCT ON (position_id, block_number)
               position_id, block_number, block_version, processing_version, quantity, deal_type_code
        FROM _mpp_src
        ORDER BY position_id, block_number, block_version DESC, processing_version DESC
    ),
    latest AS (
        SELECT DISTINCT ON (position_id)
               position_id, deal_type_code, block_number, block_version, processing_version
        FROM canonical
        -- Non-zero observations only. deal_type_code is guaranteed non-null here by pre-flight check (4),
        -- so no NULL code can reach the NOT NULL classification insert.
        WHERE quantity > 0
        ORDER BY position_id, block_number DESC, block_version DESC, processing_version DESC
    ),
    -- Recency high-water per position: the latest CLASSIFIABLE (non-zero) observation across the MERGE of
    -- already-stored rows AND this run's observations, canonicalised per (position, block) with the run
    -- winning ties (it may correct or zero a stored row). Two things make this correct where a plain
    -- max(block_number) over raw stored rows was not:
    --   (a) canonicalising the merge per block means a reorged-out or run-zeroed block drops out of the
    --       high-water (fixes the version-blind wedge — a stale non-zero row no longer pins it);
    --   (b) folding _mpp_src into the input lets the guard see the run's OWN zeroing, which a same-
    --       statement scan of position_state alone cannot (data-modifying CTEs share the pre-statement
    --       snapshot) — fixes the self-snapshot suppression.
    merged_canon AS (
        SELECT DISTINCT ON (position_id, block_number)
               position_id, block_number, block_version, processing_version, quantity
        FROM (
            SELECT position_id, block_number, block_version, processing_version, quantity, 0 AS src
              FROM position_state WHERE position_id IN (SELECT position_id FROM latest)
            UNION ALL
            SELECT position_id, block_number, block_version, processing_version, quantity, 1 AS src
              FROM _mpp_src WHERE position_id IN (SELECT position_id FROM latest)
        ) m
        ORDER BY position_id, block_number, block_version DESC, processing_version DESC, src DESC
    ),
    hwm AS (
        SELECT DISTINCT ON (position_id)
               position_id, block_number AS max_bn, block_version AS max_bv, processing_version AS max_pv
        FROM merged_canon
        WHERE quantity > 0
        ORDER BY position_id, block_number DESC, block_version DESC, processing_version DESC
    ),
    cls AS (
        INSERT INTO position_classification (position_id, deal_type_code, direction, change_reason)
        -- LEFT JOIN + raw deal_type_code: an unseeded / typo'd code must hit the deal_type_code FK
        -- (23503) and fail the run, not be silently dropped by an inner join (CLAUDE.md: fail hard).
        SELECT l.position_id, l.deal_type_code, d.direction, p_reason
        FROM latest l
        LEFT JOIN hwm s ON s.position_id = l.position_id
        LEFT JOIN ref_deal_type d ON d.deal_type = l.deal_type_code
        -- Recency guard: (re)classify only when this run's latest classifiable observation is at least as
        -- recent as the merged high-water, on the full (block_number, block_version, processing_version)
        -- tuple. So an older version/height re-emission cannot regress a newer live classification, while a
        -- reorg/zeroing that moves the canonical latest DOWN still reclassifies (hwm dropped that block).
        WHERE (l.block_number, l.block_version, l.processing_version)
              >= (COALESCE(s.max_bn, -1), COALESCE(s.max_bv, -1), COALESCE(s.max_pv, -1))
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
    SELECT count(*) INTO n FROM ins;

    DROP TABLE pg_temp._mpp_src;

    RETURN n;
END $fn$;

COMMENT ON FUNCTION materialize_position_projection(regclass, text) IS '[Operational] VEC-402..407 shared materializer: validate a per-protocol projection view against the position_state column contract, fail hard on contract/type drift, double-emitted keys, or a changed block_timestamp, then (evaluating the projection ONCE into a temp table that every check and both writes read) upsert its observations into position_state and its current latest-non-zero deal-type into position_classification (recency-guarded against the stored latest NON-ZERO block, so a backfill window cannot regress a newer classification and a closed position can still be reclassified; an all-zero position stays unclassified). position_id is recomputed via position_id(); runs serialized per view by an advisory lock keyed on the view''s canonical qualified name; guarded upserts skip no-op reruns. Idempotent; run out of band. Returns position_state rows inserted or changed.';

INSERT INTO migrations (filename) VALUES ('20260818_130000_create_position_state.sql') ON CONFLICT (filename) DO NOTHING;
