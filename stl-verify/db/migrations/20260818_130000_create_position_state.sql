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
    -- The writing projection's canonical qualified name, stamped by the materializer (not part of the
    -- view contract). Enforces the cross-view disjointness contract as data: the materializer raises if a
    -- view emits a position_id whose stored rows carry a different projection, so two views can no longer
    -- silently interleave the same position (review finding on the disjointness leg of the contract).
    projection         text        NOT NULL,
    created_at         timestamptz NOT NULL DEFAULT now(),
    -- block_timestamp is in the PK because it is the hypertable partition column (Timescale requires the
    -- partition column in every unique constraint). It is invariant PER LOGICAL KEY (position_id,
    -- block_number, block_version, processing_version) — the helper inserts a logical key once and a
    -- drifted re-emission is kept-stored-and-warned, never applied — so the 5-column key is unique over
    -- the same observations as the 4-column key. It is NOT a table-wide function of block_number: block_timestamp is each
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
COMMENT ON COLUMN position_state.projection IS 'Audit + ownership. Canonical qualified name of the projection view that wrote the row, stamped by materialize_position_projection. One view owns a position_id: the materializer raises when a view emits a position whose stored rows carry a different projection.';
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
-- position_state is STRICTLY APPEND-ONLY (team policy: append-only is the default; #737). There is no
-- update channel at all: the insert arm is ON CONFLICT DO NOTHING, a re-observation that disagrees with a
-- stored row is kept-stored-and-warned, and a real correction arrives as a new block_version /
-- processing_version row from the source. The owner-side REVOKE makes a stray fix-migration fail loudly
-- (nothing FKs position_state, so the ref-table FK/KEY SHARE caveat does not apply here); a deliberate
-- fix re-grants explicitly first. Enforced test-side by TestConvertedTablesAreAppendOnly.
GRANT SELECT, INSERT ON position_state TO stl_readwrite;
-- Guarded by role existence, mirroring security_master (20260713_140000): the roles are created by the
-- infra bootstrap, not by migrations, so the per-database test harness (which migrates as its own
-- bootstrap superuser) has neither role. Revoking the OWNER's UPDATE is safe here only because nothing
-- FKs position_state — a future FK against it would hit the RI-probe privilege trap that
-- 20260714_160000 fixed for the reference tables (restore owner UPDATE + a BEFORE UPDATE OR DELETE
-- trigger instead).
DO $$
DECLARE role text;
BEGIN
    FOREACH role IN ARRAY ARRAY['stl_readwrite','stl_migrator'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role) THEN
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON position_state FROM %I', role);
        END IF;
    END LOOP;
END $$;

-- position_classification is the RECORDED EXCEPTION to the append-only default (#737): a one-row-per-
-- position current-state table whose documented purpose is the guarded in-place reclassification, so its
-- UPDATE channel is sanctioned and recorded in db/migrations/AGENTS.md citing VEC-401/VEC-402. The channel
-- stays column-scoped to exactly the columns the cls upsert SETs (deal_type_code, direction,
-- change_reason, valid_from). collateral_status is deliberately NOT grantable: nothing writes it yet (see
-- the collateral_status note below) — the future materializer PR that handles it adds the column grant
-- together with the consistency logic. position_id (identity) stays unwritable.
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
-- code) — it APPENDS the new observations into the spine and upserts the current (latest NON-ZERO,
-- merged-canonical) deal-type into position_classification (VEC-401). A closed position keeps the
-- deal_type of its last real observation, not the ambiguous direction of a closing zero-row.
--
-- The contract is ENFORCED, not trusted: before touching data the helper (1) validates p_view emits
-- every contract column at its exact type (catches silent assignment-casts, e.g. a float8 quantity
-- corrupting precision), (2) fails hard if the view double-emits a logical observation key
-- (position_id, block_number, block_version, processing_version) rather than relying on SQLSTATE 21000,
-- (3) KEEPS-STORED-AND-WARNS when the view re-emits a stored key with a changed block_timestamp or
-- quantity (raising would wedge at-least-once wall-clock sources forever; rewriting is the update
-- channel the append-only default removes — a genuine correction bumps block_version or
-- processing_version, and event-time views must dedupe each key to a STABLE pick such as
-- MIN(synced_at)), and (5) fails hard when the view emits a position_id whose stored rows were written
-- by a DIFFERENT projection — disjointness is enforced through the stamped `projection` column, no
-- longer trusted by construction. The checks and both writes read ONE materialised snapshot of the
-- projection (a temp table). Cross-run safety: the advisory lock serialises same-view runs, and check
-- (5) keeps different views off each other's position_ids.
--
-- Classification recency: the classifying row is the highest-block CANDIDATE — a run-canonical non-zero
-- row that is also the merged-canonical row at its own block (filter-then-top, so a legitimate
-- lower-block correction is not suppressed by a stale higher-block re-emission) — and it is written only
-- when NO merged-canonical non-zero observation exists above its block, so a windowed backfill cannot
-- regress a newer live classification. A position whose entire canonical history is zero produces NO
-- classification row when it was never classified. KNOWN, DOCUMENTED BEHAVIOR (review finding): a
-- classification can OUTLIVE its canonical basis — if a position is classified and a later reorg zeroes
-- every canonical observation, the classification row remains (indistinguishable from the closed-position
-- case, and there is no delete channel under the append-only default). VEC-409 consumers must treat
-- classification as advisory alongside position_current's quantity (a zero latest quantity means closed
-- OR reorged-out) — never as proof of a live canonical basis.
--
-- collateral_status is not written here (stays NULL). If a future BLOCK-OBSERVED materializer in this
-- spine's scope needs it, add it as one more nullable column to THIS guarded upsert (not a separate
-- write, which leaves a stale status attached across a reclassification). Anchorage custody (CUSTODY vs
-- CUSTODY_COLLATERAL) is snapshot-keyed and out of scope (see the Scope block), so it is NOT planned
-- into this upsert.
--
-- p_view is a regclass, so it must name an existing relation — no SQL injection via the dynamic FROM.
-- Idempotent; run out of band. Returns the number of position_state rows INSERTED (append-only: nothing
-- is ever changed in place, so a rerun over unchanged data returns 0).
CREATE OR REPLACE FUNCTION materialize_position_projection(p_view regclass, p_reason text)
    RETURNS bigint
    LANGUAGE plpgsql
    -- Pin name resolution to the creating session's search_path (review finding: pg_temp is searched
    -- FIRST for unqualified relation names, so a session pre-creating pg_temp.position_state would have
    -- every spine write silently absorbed into the shadow). FROM CURRENT is compatible with both the
    -- per-database migrator harness and the per-schema testutil harness, unlike a hardcoded pin; the
    -- temp snapshot stays reachable because its references are pg_temp-qualified explicitly.
    SET search_path FROM CURRENT
    AS $fn$
DECLARE n bigint; bad text; v_qualname text;
BEGIN
    -- p_reason stamps change_reason provenance on every classification write; a NULL/blank would erase
    -- provenance (and overwrite a prior reason with NULL on the update path). Fail fast on the argument.
    IF p_reason IS NULL OR btrim(p_reason) = '' THEN
        RAISE EXCEPTION 'materialize_position_projection(%): p_reason must be a non-empty change_reason', p_view;
    END IF;
    -- NULL p_view would silently SKIP the advisory lock (hashtextextended is STRICT, so PERFORM of a NULL
    -- lock key is a no-op) and then fail confusingly in the contract check. Fail honestly instead.
    IF p_view IS NULL THEN
        RAISE EXCEPTION 'materialize_position_projection: p_view must not be NULL';
    END IF;

    -- Serialize concurrent runs of the SAME projection (cron overlap, scheduled + manual backfill).
    -- Key on the view's CANONICAL schema-qualified name from the catalog, not p_view::text: the regclass
    -- text renders schema-qualified only when the relation is not visible unqualified, so it would hash
    -- differently for the pinned runner (explicit search_path) vs a plain psql session — same view, two
    -- keys, no mutual exclusion. quote_qualified_ident(nspname, relname) is search_path-independent.
    -- Contract: materialize AT MOST ONE view per transaction (or, if batching several, acquire them in
    -- canonical-name order) — the xact lock is held to commit, so two callers locking different views in
    -- different orders would deadlock (40P01) and lose the whole batch.
    SELECT format('%I.%I', nsp.nspname, cls.relname) INTO v_qualname
      FROM pg_class cls JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
     WHERE cls.oid = p_view;
    PERFORM pg_advisory_xact_lock(hashtextextended('materialize_position_projection.' || v_qualname, 0));

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
    ANALYZE pg_temp._mpp_src;

    -- (2) Fail hard on a double-emitted logical observation key (do not rely on 21000). Reads the temp
    -- snapshot, so no view rescan.
    SELECT string_agg(msg, '; ') INTO bad FROM (
        SELECT format('bn=%s bv=%s pv=%s x%s', block_number, block_version, processing_version, count(*)) AS msg
        FROM pg_temp._mpp_src
        GROUP BY position_id, block_number, block_version, processing_version
        HAVING count(*) > 1 LIMIT 5) z;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % double-emits a logical observation key (position_id,block_number,block_version,processing_version): %', p_view, bad;
    END IF;

    -- (3) KEPT-STORED, WARNED — never raised, never rewritten. A re-emitted logical key that disagrees
    -- with its stored row (changed block_timestamp, or changed quantity) keeps the stored row and logs a
    -- WARNING naming the keys. Raising here wedges at-least-once wall-clock sources forever (Sky's
    -- synced_at moves on an SQS retry, no version bump will ever come, and the app role can neither
    -- UPDATE nor DELETE to repair) — and rewriting is the update channel the append-only default (#737)
    -- removes. Contract for event-time views: dedupe each logical key to a STABLE pick (e.g.
    -- MIN(synced_at)) so drift stays the rare exception; a REAL correction arrives as a new
    -- block_version / processing_version row from the source. Recovery from a persistent warn is fixing
    -- the view's pick — nothing wrong is ever stored.
    SELECT string_agg(msg, '; ') INTO bad FROM (
        SELECT format('bn=%s bv=%s pv=%s', s.block_number, s.block_version, s.processing_version) AS msg
        FROM pg_temp._mpp_src s
        JOIN public.position_state p ON p.position_id = s.position_id AND p.block_number = s.block_number
             AND p.block_version = s.block_version AND p.processing_version = s.processing_version
        WHERE p.block_timestamp <> s.block_timestamp LIMIT 5) z;
    IF bad IS NOT NULL THEN
        RAISE WARNING 'projection % re-emits stored observations with a changed block_timestamp; stored rows kept (a real correction must bump block_version/processing_version): %', p_view, bad;
    END IF;
    SELECT string_agg(msg, '; ') INTO bad FROM (
        SELECT format('bn=%s bv=%s pv=%s', s.block_number, s.block_version, s.processing_version) AS msg
        FROM pg_temp._mpp_src s
        JOIN public.position_state p ON p.position_id = s.position_id AND p.block_number = s.block_number
             AND p.block_version = s.block_version AND p.processing_version = s.processing_version
        WHERE p.block_timestamp = s.block_timestamp AND p.quantity IS DISTINCT FROM s.quantity LIMIT 5) z;
    IF bad IS NOT NULL THEN
        RAISE WARNING 'projection % re-emits stored observations with a changed quantity; stored rows kept (append-only: a real correction must bump block_version/processing_version): %', p_view, bad;
    END IF;

    -- (4) Fail hard on a NULL deal_type_code for a NON-ZERO observation: a non-zero position must carry a
    -- classification code. Surface the projection defect loudly (naming the observation) rather than let it
    -- hit the NOT NULL classification insert as a raw 23502, or silently leave the position unclassified.
    SELECT string_agg(msg, '; ') INTO bad FROM (
        SELECT format('bn=%s bv=%s pv=%s', block_number, block_version, processing_version) AS msg
        FROM pg_temp._mpp_src WHERE quantity > 0 AND deal_type_code IS NULL LIMIT 5) z;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % emits a NULL deal_type_code on a non-zero observation (every non-zero observation must carry a classification code): %', p_view, bad;
    END IF;

    -- (5) Cross-view disjointness ENFORCED as data, not trusted: one projection owns a position_id. Every
    -- insert below stamps the writing view's canonical name, and this check probes ONE stored row per run
    -- position (the PK-first row — sound inductively, because this check runs before every insert and all
    -- of a position's rows are stamped identically). A different owner means two views are interleaving
    -- the same position — the overlap the per-view advisory lock cannot serialize (different views take
    -- different locks) — so fail loudly naming both views.
    SELECT format('position owned by %s', own.projection) INTO bad
    FROM (SELECT DISTINCT position_id FROM pg_temp._mpp_src) s
    JOIN LATERAL (
        SELECT p.projection FROM public.position_state p
        WHERE p.position_id = s.position_id
        ORDER BY p.position_id, p.block_number, p.block_version, p.processing_version, p.block_timestamp
        LIMIT 1) own ON own.projection <> v_qualname
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % emits position_ids owned by another projection (cross-view disjointness violated): %', p_view, bad;
    END IF;

    -- Insert NEW logical keys and classify, in one statement over the temp snapshot. position_state is
    -- strictly append-only: already-stored keys were handled above (identical rows are no-ops; drifted
    -- re-emissions are kept-stored and warned), so the insert filters to unseen keys; ON CONFLICT DO
    -- NOTHING is only a belt against a racing same-key insert (same-view runs are serialized; cross-view
    -- keys are disjoint by check (5)). The data-modifying cls CTE runs to completion even though the
    -- top-level query reads only ins.
    WITH ins AS (
        INSERT INTO public.position_state
            (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
             block_number, block_version, processing_version, block_timestamp, projection)
        SELECT s.position_id, s.chain_id, s.protocol_id, s.instrument_key, s.holder_id, s.quantity,
               s.block_number, s.block_version, s.processing_version, s.block_timestamp, v_qualname
        FROM pg_temp._mpp_src s
        WHERE NOT EXISTS (
            SELECT 1 FROM public.position_state p
            WHERE p.position_id = s.position_id AND p.block_number = s.block_number
              AND p.block_version = s.block_version AND p.processing_version = s.processing_version)
        -- Insert in PK order for B-tree bulk-load locality (sequential leaf writes); not a lock-ordering
        -- concern (check (5) makes cross-view row sets disjoint).
        ORDER BY s.position_id, s.block_number, s.block_version, s.processing_version, s.block_timestamp
        ON CONFLICT (position_id, block_number, block_version, processing_version, block_timestamp) DO NOTHING
        RETURNING 1
    ),
    -- Per-block canonical view of the RUN's rows, with EFFECTIVE values: where a logical key is already
    -- stored, the stored quantity is authoritative (kept-stored above), so the guard reasons about what
    -- the table actually holds, never about a drifted re-emission that was not applied. The run's
    -- deal_type_code is kept — codes live only in the run and feed the classification.
    canonical AS (
        SELECT DISTINCT ON (s.position_id, s.block_number)
               s.position_id, s.block_number, s.block_version, s.processing_version,
               COALESCE(p.quantity, s.quantity) AS quantity, s.deal_type_code
        FROM pg_temp._mpp_src s
        LEFT JOIN public.position_state p ON p.position_id = s.position_id AND p.block_number = s.block_number
             AND p.block_version = s.block_version AND p.processing_version = s.processing_version
        ORDER BY s.position_id, s.block_number, s.block_version DESC, s.processing_version DESC
    ),
    -- Candidate classifying rows: FILTER-THEN-TOP (review finding: top-1-then-filter permanently
    -- suppressed a legitimate lower-block correction arriving alongside a stale higher-block row, with
    -- every rerun repeating the skip). A run-canonical row qualifies only if it is ALSO the
    -- merged-canonical row at its own block: no stored row at that block carries a strictly higher
    -- (block_version, processing_version). A stored row at the SAME version is the same observation
    -- (kept-stored) and still qualifies.
    cand AS (
        SELECT c.*
        FROM canonical c
        WHERE c.quantity > 0
          AND NOT EXISTS (
            SELECT 1 FROM public.position_state p
            WHERE p.position_id = c.position_id AND p.block_number = c.block_number
              AND (p.block_version, p.processing_version) > (c.block_version, c.processing_version))
    ),
    -- The classifying row per position: the candidate at the highest block.
    latest AS (
        SELECT DISTINCT ON (position_id)
               position_id, deal_type_code, block_number, block_version, processing_version
        FROM cand
        ORDER BY position_id, block_number DESC
    ),
    -- Recency: classify only when NO merged-canonical non-zero observation exists ABOVE the classifying
    -- block. Bounded per the review's equivalence argument: a canonical non-zero BELOW l.block_number can
    -- never outrank l, so both branches join to `latest` and read only block_number >= l.block_number —
    -- the stored branch becomes a per-position PK range scan instead of a full-history external sort, and
    -- the run branch reuses `canonical` (per-block canonicalization is idempotent). The former
    -- three-component tuple guard provably collapsed to this block equality: given the cand filter, l is
    -- always a non-zero merged-canonical row, so the high-water always exists and the version legs could
    -- never decide. NOTE: the run branch is what lets the guard see the run's OWN zeroing at a NEW
    -- version (a same-statement scan of position_state cannot — data-modifying CTEs share the
    -- pre-statement snapshot), and per-block canonicalisation is what drops a reorged-out block from the
    -- high-water.
    merged_ge AS (
        SELECT DISTINCT ON (m.position_id, m.block_number)
               m.position_id, m.block_number, m.quantity
        FROM (
            SELECT p.position_id, p.block_number, p.block_version, p.processing_version, p.quantity, 0 AS src
              FROM public.position_state p JOIN latest l ON l.position_id = p.position_id
             WHERE p.block_number >= l.block_number
            UNION ALL
            SELECT c.position_id, c.block_number, c.block_version, c.processing_version, c.quantity, 1 AS src
              FROM canonical c JOIN latest l ON l.position_id = c.position_id
             WHERE c.block_number >= l.block_number
        ) m
        ORDER BY m.position_id, m.block_number, m.block_version DESC, m.processing_version DESC, m.src DESC
    ),
    hwm AS (
        SELECT position_id, max(block_number) AS max_bn
        FROM merged_ge
        WHERE quantity > 0
        GROUP BY position_id
    ),
    cls AS (
        INSERT INTO public.position_classification (position_id, deal_type_code, direction, change_reason)
        -- LEFT JOIN + raw deal_type_code: an unseeded / typo'd code must hit the deal_type_code FK
        -- (23503) and fail the run, not be silently dropped by an inner join (CLAUDE.md: fail hard).
        SELECT l.position_id, l.deal_type_code, d.direction, p_reason
        FROM latest l
        JOIN hwm s ON s.position_id = l.position_id
        LEFT JOIN public.ref_deal_type d ON d.deal_type = l.deal_type_code
        WHERE l.block_number = s.max_bn
        ORDER BY l.position_id
        ON CONFLICT (position_id) DO UPDATE
            SET deal_type_code = EXCLUDED.deal_type_code,
                direction      = EXCLUDED.direction,
                change_reason  = EXCLUDED.change_reason,
                -- re-stamp valid_from + change_reason only when the classification actually changes.
                valid_from     = (now() AT TIME ZONE 'utc')::date
            WHERE public.position_classification.deal_type_code IS DISTINCT FROM EXCLUDED.deal_type_code
               OR public.position_classification.direction      IS DISTINCT FROM EXCLUDED.direction
    )
    SELECT count(*) INTO n FROM ins;

    DROP TABLE pg_temp._mpp_src;

    RETURN n;
END $fn$;

COMMENT ON FUNCTION materialize_position_projection(regclass, text) IS '[Operational] VEC-402..407 shared materializer: validate a per-protocol projection view against the position_state column contract, fail hard on contract/type drift, double-emitted keys, or cross-view ownership violations (the stamped projection column); keep-stored-and-warn on a re-emitted key whose block_timestamp or quantity drifted (append-only: a real correction bumps block_version/processing_version), then — evaluating the projection ONCE into a temp table every check and both writes read — APPEND the new observations into position_state and upsert the current deal-type into position_classification. The classifying row is the highest-block run row that is also merged-canonical at its block (filter-then-top), written only when no merged-canonical non-zero observation exists above it; both scans are bounded to blocks at or above the classifying block. A classification can outlive a fully-reorged-out basis (documented; no delete channel). position_id is recomputed via position_id(); runs serialized per view by an advisory lock on the view''s canonical name. Idempotent; run out of band. Returns rows INSERTED.';

INSERT INTO migrations (filename) VALUES ('20260818_130000_create_position_state.sql') ON CONFLICT (filename) DO NOTHING;
