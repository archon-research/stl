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

-- ============================================================================
-- Classification: append-only history + derived current cache
-- ============================================================================
-- Shape follows the current-position tables in #733 (VEC-577): the history table is append-only and
-- never edited, and a small plain <name>_current table holds the newest row per key, kept fresh by an
-- AFTER INSERT trigger and rebuildable from history at any time. Applied here, that means:
--
--   position_classification          one row per DECISION, append-only, never edited
--   position_classification_current  one row per position, the derived cache consumers read
--
-- Why append: a classification is an observation about a position at a point in the pipeline, and
-- db/migrations/AGENTS.md names classification specifically as something that must be an appended
-- observation carrying the version tuple, read latest-first, "never a column". Editing one row in place
-- destroyed the previous answer on every write: a run that classified BORROW, was corrected by a reorg
-- to LOAN, then reprocessed to COLLATERAL left only COLLATERAL, so a risk result computed against the
-- BORROW row could not be explained afterwards — the tracing ADR-0002 exists to guarantee. Appending
-- also removes the recency guard entirely: an out-of-order backfill cannot regress the current answer
-- because nothing is overwritten, and the guard had a defect in each of its three revisions.
--
-- The existing one-row-per-position table already has exactly the cache's shape (PK on position_id,
-- deal_type FK, direction CHECK, id-width CHECK), so it is RENAMED rather than rebuilt. Safe to rename
-- now: nothing reads it yet — no view, no join, no application code — and the rename follows the
-- precedent in 20260722_120000. Its creating migration (20260713_150000) stays untouched and applied.
ALTER TABLE position_classification RENAME TO position_classification_current;
ALTER TABLE position_classification_current
    RENAME CONSTRAINT position_classification_pkey TO position_classification_current_pkey;

-- The cache carries the winning decision's version and the provenance of the observation it classified,
-- so a reader can tell WHICH decision it reflects and go straight to that history row.
ALTER TABLE position_classification_current
    ADD COLUMN IF NOT EXISTS classification_version   integer,
    ADD COLUMN IF NOT EXISTS as_of_block              bigint,
    ADD COLUMN IF NOT EXISTS as_of_block_version      integer,
    ADD COLUMN IF NOT EXISTS as_of_processing_version integer;

COMMENT ON TABLE position_classification_current IS '[Operational] Newest classification per position_id. Derived cache of the position_classification history; rebuildable from it at any time (TRUNCATE and re-run the backfill in 20260818_130000). Consumers read this table, never the history.';
COMMENT ON COLUMN position_classification_current.classification_version IS 'Derived (copy of position_classification.classification_version). The winning decision; the whole of the newer-wins comparison, because a decision''s BASIS can move to an earlier block after a reorg while the decision sequence only ever increases.';
COMMENT ON COLUMN position_classification_current.as_of_block IS 'Derived. block_number of the observation the winning decision classified.';
COMMENT ON COLUMN position_classification_current.as_of_block_version IS 'Derived. Reorg version of that observation.';
COMMENT ON COLUMN position_classification_current.as_of_processing_version IS 'Derived. Processing version of that observation.';

-- Per #733, the cache is mutable by design: it is a derived copy, so overwriting it loses nothing that
-- history does not still hold. Recorded as a derived-cache exception in db/migrations/AGENTS.md citing
-- VEC-402 / #625, per #737 (append-only is the default; any update channel is recorded).
GRANT SELECT ON position_classification_current TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON position_classification_current TO stl_readwrite;

-- ---------------------------------------------------------------- the history

CREATE TABLE IF NOT EXISTS position_classification (
    position_id              bytea       NOT NULL,
    -- Per-position decision sequence, allocated by the writer as max+1. It is the ordering key for
    -- "current", not (as_of_block, ...): a reorg that zeroes the latest observation moves the
    -- classifying basis DOWN to an earlier block, so an observation-coordinate comparison would refuse
    -- the correction (reproduced: claiming block 100 after block 200 is a strictly lower tuple).
    classification_version   integer     NOT NULL,
    deal_type_code           text        NOT NULL,
    direction                text,
    -- Carried from the pre-rename table for parity; still unwritten (VEC-408 owns its writer). As a
    -- decision attribute it belongs on the decision, so a future collateral change appends a version
    -- rather than editing one.
    collateral_status        text,
    -- Coordinates of the observation this decision classified, in position_state.
    as_of_block              bigint      NOT NULL,
    as_of_block_version      integer     NOT NULL,
    as_of_processing_version integer     NOT NULL,
    -- Same UTC default as every other normalized enrichment table (20260721_120000); the materializer
    -- supplies it explicitly, so the default only covers direct inserts.
    valid_from               date        NOT NULL DEFAULT (now() AT TIME ZONE 'utc')::date,
    change_reason            text        NOT NULL,
    created_at               timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT position_classification_pkey PRIMARY KEY (position_id, classification_version),
    CONSTRAINT position_classification_deal_type_fkey
        FOREIGN KEY (deal_type_code) REFERENCES ref_deal_type (deal_type),
    CONSTRAINT position_classification_direction_chk
        CHECK (direction IS NULL OR direction = ANY (ARRAY['LONG'::text, 'SHORT'::text])),
    CONSTRAINT position_classification_id_len_chk CHECK (octet_length(position_id) = 32),
    CONSTRAINT position_classification_version_pos_chk CHECK (classification_version > 0),
    CONSTRAINT position_classification_coord_nonneg_chk
        CHECK (as_of_block >= 0 AND as_of_block_version >= 0 AND as_of_processing_version >= 0)
);

COMMENT ON TABLE position_classification IS '[Operational] Append-only history of per-position deal-type decisions (VEC-401/VEC-402). One row per DECISION: the deal_type_code, the frozen direction, and the coordinates of the position_state observation it was derived from. Never edited or deleted — a correction, a reorg or a reprocess appends a new classification_version. Consumers read position_classification_current.';
COMMENT ON COLUMN position_classification.position_id IS 'PK. The bytea(32) native position identity from position_id() (VEC-400).';
COMMENT ON COLUMN position_classification.classification_version IS 'PK. Per-position decision sequence, 1-based, allocated as max+1 by the writer. Highest version is the current classification.';
COMMENT ON COLUMN position_classification.deal_type_code IS 'FK->ref_deal_type.deal_type. The classification this decision assigned.';
COMMENT ON COLUMN position_classification.direction IS 'LONG or SHORT, frozen from ref_deal_type at decision time so a later vocabulary change cannot silently restate history.';
COMMENT ON COLUMN position_classification.as_of_block IS 'Provenance. block_number of the observation classified (the canonical latest non-zero for the position at decision time).';
COMMENT ON COLUMN position_classification.as_of_block_version IS 'Provenance. Reorg version of the classified observation.';
COMMENT ON COLUMN position_classification.as_of_processing_version IS 'Provenance. Processing version of the classified observation.';
COMMENT ON COLUMN position_classification.collateral_status IS 'Unwritten pending VEC-408. A decision attribute: a collateral change appends a new classification_version rather than editing this one.';
COMMENT ON COLUMN position_classification.valid_from IS 'Date (UTC) this decision became effective.';
COMMENT ON COLUMN position_classification.change_reason IS 'Audit. The materializer''s p_reason for this decision; never NULL.';
COMMENT ON COLUMN position_classification.created_at IS 'Audit. Row insert time.';

-- Strictly append-only, like position_state: no update channel at all. The owner-side REVOKE makes a
-- stray fix-migration fail loudly rather than silently restating a classification (nothing FKs this
-- table, so the RI-probe privilege trap 20260714_160000 fixed for the reference tables cannot apply).
-- Role-existence guarded: the roles come from the infra bootstrap, not from migrations.
GRANT SELECT ON position_classification TO stl_readonly;
GRANT SELECT, INSERT ON position_classification TO stl_readwrite;
GRANT SELECT ON ref_deal_type TO stl_readwrite;   -- the direction lookup join (idempotent re-grant)
DO $$
DECLARE role text;
BEGIN
    FOREACH role IN ARRAY ARRAY['stl_readwrite','stl_migrator'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role) THEN
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON position_classification FROM %I', role);
        END IF;
    END LOOP;
END $$;

-- ---------------------------------------------------------------- cache maintenance

-- AFTER INSERT so the cache copies the committed row, matching #733. The upsert wins only on a higher
-- classification_version, so a replayed or concurrently-inserted older decision can never regress the
-- cache. search_path is pinned: pg_temp is searched first for unqualified relation names, so a caller
-- with a temp table named position_classification_current could otherwise absorb the write.
CREATE OR REPLACE FUNCTION upsert_position_classification_current() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
AS $$
BEGIN
    INSERT INTO public.position_classification_current AS cur
        (position_id, classification_version, deal_type_code, direction, valid_from, change_reason,
         as_of_block, as_of_block_version, as_of_processing_version)
    VALUES
        (NEW.position_id, NEW.classification_version, NEW.deal_type_code, NEW.direction,
         NEW.valid_from, NEW.change_reason, NEW.as_of_block, NEW.as_of_block_version,
         NEW.as_of_processing_version)
    ON CONFLICT (position_id) DO UPDATE SET
        classification_version   = EXCLUDED.classification_version,
        deal_type_code           = EXCLUDED.deal_type_code,
        direction                = EXCLUDED.direction,
        valid_from               = EXCLUDED.valid_from,
        change_reason            = EXCLUDED.change_reason,
        as_of_block              = EXCLUDED.as_of_block,
        as_of_block_version      = EXCLUDED.as_of_block_version,
        as_of_processing_version = EXCLUDED.as_of_processing_version
    WHERE EXCLUDED.classification_version > cur.classification_version;
    RETURN NULL;
END;
$$;

COMMENT ON FUNCTION upsert_position_classification_current() IS '[Operational] Keeps position_classification_current at the highest classification_version per position (VEC-402). AFTER INSERT on the history; newer-wins on classification_version only, so an out-of-order insert cannot regress the cache.';

-- Created before the backfill: CREATE TRIGGER takes a lock that blocks inserts, so no row can commit in
-- the window between the backfill's snapshot and the trigger going live (#733).
CREATE TRIGGER trigger_upsert_position_classification_current
    AFTER INSERT ON position_classification
    FOR EACH ROW
EXECUTE FUNCTION upsert_position_classification_current();

-- Backfill / rebuild statement. Empty on first apply (the history table is new); this is the recovery
-- path referenced in the cache's COMMENT — TRUNCATE position_classification_current, then run this.
INSERT INTO position_classification_current
    (position_id, classification_version, deal_type_code, direction, valid_from, change_reason,
     as_of_block, as_of_block_version, as_of_processing_version)
SELECT DISTINCT ON (h.position_id)
       h.position_id, h.classification_version, h.deal_type_code, h.direction, h.valid_from,
       h.change_reason, h.as_of_block, h.as_of_block_version, h.as_of_processing_version
FROM position_classification h
ORDER BY h.position_id, h.classification_version DESC
ON CONFLICT (position_id) DO NOTHING;


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
DECLARE n bigint; n_cls bigint; bad text; v_qualname text;
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

    -- TWO STATEMENTS, deliberately (this is what makes table-level enforcement possible). Statement 1
    -- appends the new observations; statement 2 classifies. Because the append has COMMITTED to the
    -- statement's own snapshot by the time statement 2 runs, the classification logic — and the
    -- validating trigger on position_classification — both read the complete post-append state from
    -- position_state alone. The previous single-statement form could not: a data-modifying CTE shares
    -- the pre-statement snapshot, so the guard had to fold the run's rows in by hand (the :278
    -- self-snapshot class) and a trigger could not have seen them at all. Same transaction either way,
    -- so atomicity is unchanged (a raise in statement 2 rolls back the append).
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
    ON CONFLICT (position_id, block_number, block_version, processing_version, block_timestamp) DO NOTHING;
    GET DIAGNOSTICS n = ROW_COUNT;

    -- Statement 2: classify from the post-append stored state. Every CTE now reads position_state, so
    -- there is no run-vs-stored reconciliation left: the append already merged them.
    WITH run_pos AS (
        SELECT DISTINCT position_id FROM pg_temp._mpp_src
    ),
    -- Per-block canonical: highest (block_version, processing_version) at each block, from the stored
    -- table (post-append).
    canonical AS (
        SELECT DISTINCT ON (p.position_id, p.block_number)
               p.position_id, p.block_number, p.block_version, p.processing_version, p.quantity
        FROM public.position_state p
        JOIN run_pos r ON r.position_id = p.position_id
        ORDER BY p.position_id, p.block_number, p.block_version DESC, p.processing_version DESC
    ),
    -- The classifying observation per position: the canonical NON-ZERO row at the highest block. The
    -- codes live only in the run, so the deal_type_code is joined back from the snapshot at exactly
    -- those coordinates; a position whose canonical latest non-zero was not emitted by this run has no
    -- code to write and is skipped (its stored classification stands).
    latest AS (
        SELECT DISTINCT ON (c.position_id)
               c.position_id, c.block_number, c.block_version, c.processing_version
        FROM canonical c
        WHERE c.quantity > 0
        ORDER BY c.position_id, c.block_number DESC
    ),
    cls AS (
        -- APPEND a decision. No update channel: the cache is maintained by the AFTER INSERT trigger,
        -- and an out-of-order run cannot regress it because nothing is overwritten. The NOT EXISTS
        -- against the cache is the idempotency rule that replaces the old recency guard: a rerun that
        -- reaches the same answer from the same observation appends nothing and the run returns 0.
        INSERT INTO public.position_classification
            (position_id, classification_version, deal_type_code, direction, change_reason,
             valid_from, as_of_block, as_of_block_version, as_of_processing_version)
        -- LEFT JOIN + raw deal_type_code: an unseeded / typo'd code must hit the deal_type_code FK
        -- (23503) and fail the run, not be silently dropped by an inner join (CLAUDE.md: fail hard).
        SELECT l.position_id, coalesce(cur.classification_version, 0) + 1,
               s.deal_type_code, d.direction, p_reason, (now() AT TIME ZONE 'utc')::date,
               l.block_number, l.block_version, l.processing_version
        FROM latest l
        JOIN pg_temp._mpp_src s ON s.position_id = l.position_id AND s.block_number = l.block_number
             AND s.block_version = l.block_version AND s.processing_version = l.processing_version
        LEFT JOIN public.ref_deal_type d ON d.deal_type = s.deal_type_code
        LEFT JOIN public.position_classification_current cur ON cur.position_id = l.position_id
        WHERE cur.position_id IS NULL
           OR cur.deal_type_code IS DISTINCT FROM s.deal_type_code
           OR cur.direction      IS DISTINCT FROM d.direction
           OR (cur.as_of_block, cur.as_of_block_version, cur.as_of_processing_version)
              IS DISTINCT FROM (l.block_number, l.block_version, l.processing_version)
        ORDER BY l.position_id
        RETURNING 1
    )
    SELECT count(*) INTO n_cls FROM cls;

    DROP TABLE pg_temp._mpp_src;

    RETURN n;
END $fn$;

COMMENT ON FUNCTION materialize_position_projection(regclass, text) IS '[Operational] VEC-402..407 shared materializer: validate a per-protocol projection view against the position_state column contract, fail hard on contract/type drift, double-emitted keys, or cross-view ownership violations (the stamped projection column); keep-stored-and-warn on a re-emitted key whose block_timestamp or quantity drifted (append-only: a real correction bumps block_version/processing_version), then — evaluating the projection ONCE into a temp table every check and both writes read — APPEND the new observations into position_state and upsert the current deal-type into position_classification. The classifying row is the highest-block run row that is also merged-canonical at its block (filter-then-top), written only when no merged-canonical non-zero observation exists above it; both scans are bounded to blocks at or above the classifying block. A classification can outlive a fully-reorged-out basis (documented; no delete channel). position_id is recomputed via position_id(); runs serialized per view by an advisory lock on the view''s canonical name. Idempotent; run out of band. Returns rows INSERTED.';

INSERT INTO migrations (filename) VALUES ('20260818_130000_create_position_state.sql') ON CONFLICT (filename) DO NOTHING;
