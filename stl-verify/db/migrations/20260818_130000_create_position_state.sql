-- VEC-402..407: position_state — the shared spine every per-protocol position materializer writes into.
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
-- PK. processing_version is the SOURCE's correction version, propagated by the projection -- not a
-- constant. Morpho carries one (20260410_110000 adds the column to morpho_market_state and
-- morpho_market_position; 20260410_150000 installs its assign_processing_version triggers), and an
-- earlier version of this comment wrongly named Morpho as a source without one. A projection that
-- hardcodes 0 makes every source-side correction re-emit the SAME logical key, which then hits the
-- keep-stored-and-warn arm and is discarded permanently -- UPDATE and DELETE are revoked, so only a
-- superuser fix-migration could repair it. Use 0 only where a source genuinely has no such column. The current state per
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
-- Compression IS set here, per the rule that a time-series table gets hypertable + compression +
-- tiering in its creating migration. An earlier revision of this file deferred it to VEC-566, arguing
-- that the full-projection upsert re-touches every chunk and that ON CONFLICT decompresses each
-- compressed chunk it checks before the no-op guard runs, exceeding
-- max_tuples_decompressed_per_dml_transaction. That argument was wrong, and measuring it is what showed
-- why: the no-op guard here is the NOT EXISTS anti-join in the SELECT feeding the INSERT, so it runs
-- FIRST and the INSERT never presents an already-stored row to ON CONFLICT at all.
--
-- Measured on timescale/timescaledb:2.25.1-pg17 (the CI pin), 150,000 observations over 150 chunks,
-- every chunk compressed:
--   full re-projection of the same history      inserted 0, 1.0s, no error
--   reprocess writing 150,000 NEW rows          inserted 150,000, 2.3s, no error
--   PK enforced against compressed rows         yes -- a duplicate re-insert returns INSERT 0 0
-- and the asymmetry that matters, same table, same chunks, same rows:
--   ON CONFLICT DO NOTHING                      INSERT 0 0
--   ON CONFLICT DO UPDATE                       ERROR: tuple decompression limit exceeded
--                                               current limit: 100000, decompressed: 100050
-- The limit is a property of the DO UPDATE arm, which must rewrite stored rows. This table has no such
-- arm -- UPDATE is revoked from every non-owner role below, so it cannot acquire one -- so the write
-- path and compression are compatible. A cache that DOES upsert in place is a different matter and must
-- measure its own shape rather than cite this paragraph.
--

-- S3 tiering below IS set now. Note it can bite immediately, not "in ~1 year": add_tiering_policy
-- measures a chunk's range against now(), and the partition column is block_timestamp (on-chain time,
-- not insert time), so any backfill of historical protocol data writes chunks already past the window.
-- The function below therefore sets timescaledb.enable_tiered_reads explicitly; see its comment.

CREATE TABLE IF NOT EXISTS position_state (
    position_id        bytea       NOT NULL,
    chain_id           integer,                       -- native; nullable per position_id's structural-field convention
    protocol_id        bigint,                        -- native; nullable per convention
    instrument_key     text        NOT NULL,          -- native instrument id (VEC-412 bridge key); resolves security via security_instrument_bridge_current
    holder_id          text        NOT NULL,          -- native on-chain holder: 20-byte wallet / prime vault address, 40 lowercase hex chars, no 0x
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
    -- Which build wrote the row (ADR-0002 code provenance). Soft ref to build_registry.id, no FK, and
    -- 0 = pre-tracking, matching every other history table (20260410_110000). Stamped by this
    -- migration's helper from its p_build_id argument: the writer supplies it, as it does everywhere
    -- else in the repo -- there is no SQL-side current-build helper to read it from.
    build_id           integer     NOT NULL DEFAULT 0,
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
    -- holder_id must be a 20-byte address as lowercase hex, no 0x. position_id() cannot normalise (the
    -- convention is the materializer's contract), and this table is the single chokepoint every
    -- materializer writes through: one materializer emitting '0xAbC…' while another emits 'abc…' would
    -- fork one wallet into two position_ids.
    --
    -- The width is load-bearing, not decoration. An unanchored-length hex pattern also admits a DECIMAL
    -- SURROGATE rendered as text ('12345' is all hex digits), so a materializer author who keys holders
    -- on a row id instead of an address stores it CHECK-clean and forks identity permanently — holder_id
    -- feeds the position_id hash and this table grants no UPDATE. Every holder form in the schema is 20
    -- bytes (user.address, prime.vault_address, and allocation_position's transfer parties), so 40 hex
    -- characters admits all of them and rejects a surrogate. A wider non-EVM address form would need a
    -- new migration; that is cheap in the direction that matters, since ADD CONSTRAINT and
    -- DROP-then-re-ADD were both measured working on a COMPRESSED hypertable on 2.25.1 and 2.27.2
    -- (contrast 20260819_100000's note, which reports ADD CHECK broken on 2.26.x).
    --
    -- instrument_key stays unchecked — heterogeneous native forms (registry:ilk, provider:package) are
    -- legitimate there.
    CONSTRAINT position_state_holder_hex_chk CHECK (holder_id ~ '^[0-9a-f]{40}$'),
    -- instrument_key has an upper bound because a key this table accepts but the bridge cannot store
    -- resolves to nothing, forever. security_instrument_bridge's PK is (instrument_key,
    -- processing_version) -- a btree, whose index entry cannot exceed 2704 bytes. Measured on
    -- 2.25.1-pg17: an INCOMPRESSIBLE key stores at 2,600 characters and is refused at 2,700 ('index row
    -- size 2720 exceeds btree version 4 maximum 2704'), while a repetitive 10,000-character key stores
    -- fine because it compresses inline. So the true limit is on encoded bytes and depends on the
    -- content, which no character count can mirror exactly -- 2,000 is the pragmatic cap that holds for
    -- ANY content. Every real native form is orders of magnitude smaller (a 40-char address, a 64-char
    -- market id, registry:ilk, provider:package), so this rejects nothing legitimate.
    CONSTRAINT position_state_instrument_key_len_chk CHECK (char_length(instrument_key) <= 2000),
    -- Observation coordinates are non-negative by definition (genesis is block 0; versions count from 0).
    -- quantity had a CHECK but the coordinates did not, so a source bug emitting a negative height was
    -- stored silently and — because the recency guard orders on these columns — became the position's
    -- "oldest" observation and skewed classification recency. Reject at the chokepoint instead.
    CONSTRAINT position_state_coord_nonneg_chk
        CHECK (block_number >= 0 AND block_version >= 0 AND processing_version >= 0
               AND build_id >= 0),
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

COMMENT ON TABLE position_state IS '[Hypertable] Partition key: block_timestamp, 1-day chunks. Columnstore compression after 2 days (segment by position_id, order by the version tuple) and S3 tiering with a 1-year policy window -- which can tier on the FIRST policy run rather than a year out, because the window is measured against a chunk''s block_timestamp range (on-chain time, not insert time) and a historical backfill writes chunks already past it. Compression is safe here because the write path is ON CONFLICT DO NOTHING behind a NOT EXISTS anti-join and holds no UPDATE grant: the decompression limit that breaks in-place upserts is never reached. Measured at 150,000 observations over 150 compressed chunks. Shared spine for materialized positions (VEC-402..407), partitioned on block_timestamp. One row per (native position_id, observation): the source''s own native keys (instrument_key -> security via the bridge; holder_id, an on-chain address) plus a single canonical quantity. Identity is native-only (VEC-400); classifications live in position_classification. Current state per position is position_current (VEC-409).';
COMMENT ON COLUMN position_state.position_id IS 'PK. bytea(32) native identity from position_id() (VEC-400): hash(chain_id, protocol_id, instrument_key, holder_id). No mapped value in the hash.';
COMMENT ON COLUMN position_state.chain_id IS 'FK→chain.chain_id (soft ref, no FK constraint). Native chain id. Nullable per the position_id structural-field convention; each materializer uses a fixed NULL-ness convention.';
COMMENT ON COLUMN position_state.protocol_id IS 'FK→protocol.id (soft ref, no FK constraint). NOT an on-chain value: protocol.id is a BIGSERIAL surrogate, so it is environment-local and a reseed renumbers it. Because it feeds the position_id hash, a position_id is reproducible only WITHIN one database lineage -- do not compare position_id across environments or across a protocol reseed. Nullable per the structural-field convention; each materializer must use one fixed documented NULL-ness convention or the same position forks into two ids.';
COMMENT ON COLUMN position_state.instrument_key IS 'Native, globally-unique instrument id (the security_instrument_bridge key). Resolves to a security via security_instrument_bridge_current -> security_master_current (VEC-420).';
COMMENT ON COLUMN position_state.holder_id IS 'Native on-chain holder id: a 20-byte wallet or prime vault address as exactly 40 lowercase hex characters, no 0x. A format contract, CHECK-enforced, because this column feeds the position_id hash and the table grants no UPDATE: a materializer that stores any other holder form forks identity permanently. An address rather than a registry surrogate for the same reason allocation_position stores raw transfer parties -- a holder can be any address that ever transacted, so keying on a registry row id would require minting a row for every unknown address before its position could be stored. Whether an address maps to a legal entity is a consumer concern; this table states the format and stores the observation.';
COMMENT ON COLUMN position_state.quantity IS 'Holder balance in the instrument''s own native units. Scale is source-defined and NOT normalized across protocols (e.g. Morpho shares are raw integers at the loan token''s decimals; Sky/prime flow amounts are decimal-normalized), so each materializer''s projection documents its unit/scale, and a consumer must not SUM across heterogeneous instruments without normalizing. Per-protocol amount breakdowns stay in the raw source.';
COMMENT ON COLUMN position_state.block_number IS 'PK. Block height of the observation.';
COMMENT ON COLUMN position_state.block_version IS 'PK. Reorg version of the block.';
COMMENT ON COLUMN position_state.processing_version IS 'PK. The SOURCE''s correction version, propagated by the projection: 0 = original, N = Nth reprocess. Not a constant -- a projection that hardcodes 0 makes a source-side correction re-emit the same logical key, which is then kept-stored-and-warned and discarded. Use 0 only for a source that genuinely carries no correction version.';
COMMENT ON COLUMN position_state.block_timestamp IS 'PK, Partition. On-chain observation time (UTC); the hypertable partition column, and part of the PK.';
COMMENT ON COLUMN position_state.projection IS 'Audit. Canonical qualified name of the projection view that wrote the row, stamped by materialize_position_projection. One view owns a position_id: the materializer raises when a view emits a position whose stored rows carry a different projection.';
COMMENT ON COLUMN position_state.build_id IS 'Audit. ID of the indexer build (code+config) that wrote this row. Soft ref to build_registry.id (no FK); 0 = pre-tracking.';
COMMENT ON COLUMN position_state.created_at IS 'Audit. Row insert time.';

-- Hypertable on block_timestamp, 1-day chunks (matches the transformed bucket1 position tables).
-- create_default_indexes => FALSE: the PK does NOT lead with block_timestamp, so Timescale's default
-- would silently add a block_timestamp-only index on every chunk that no documented access path uses
-- (all lead with position_id) — an extra index write per upsert. Reverse-lookup indexes are added by
-- whoever first needs them.
SELECT create_hypertable('position_state', 'block_timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE, create_default_indexes => FALSE);

ALTER TABLE position_state SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'position_id',
    timescaledb.compress_orderby = 'block_number DESC, block_version DESC, processing_version DESC'
);

-- Note TimescaleDB APPENDS the partition column to compress_orderby when the DDL omits it, so the
-- stored setting is the version tuple followed by block_timestamp. That is what is wanted -- the
-- version tuple leads, so a compressed batch is ordered for the latest-per-position read rather than by
-- time alone -- but it means the catalogue does not echo this statement verbatim.
--
-- 2 days is the repo-wide default, not a choice keyed on partition semantics: a survey of every
-- add_compression_policy here shows 2 days on both on-chain histories and snapshot-shaped tables, with
-- 7/14/60-day lags being the exceptions (and one of those, fluid_vault_state at 14 days, is itself
-- block_timestamp-partitioned). An earlier version of this comment claimed the 2-day value distinguished
-- on-chain histories from snapshots, which is not a distinction the repo makes. Chunks are 1-day, so a
-- chunk compresses about a day after it stops receiving live blocks; a reprocess reaching further back
-- inserts into a compressed chunk, measured above as safe.
SELECT add_compression_policy('position_state', INTERVAL '2 days', if_not_exists => TRUE);

-- Two things a reviewer measured that are NOT settled by the paragraphs above, recorded so they are
-- actionable rather than lost.
--
-- 1. segmentby's sign depends on a cadence nothing knows yet. compress_segmentby = 'position_id' stores
--    the 32-byte key once per batch, so it pays off only when a position has several observations per
--    chunk. Measured at a constant 200,000 rows, varying only cardinality:
--      1 observation per position per chunk   58 MB -> 173 MB   (ratio 0.34, storage TRIPLES)
--      2                                      58 MB ->  92 MB   (0.63)
--      5                                      58 MB ->  45 MB   (1.28)
--      50                                     58 MB ->   6 MB   (8.48)
--    TimescaleDB itself warns "poor compression ratio detected" below ~1. The crossover is between 2 and
--    5 observations per position per day, and no projection view exists yet, so the real cadence is
--    unknown at merge time. This value follows the house convention (segment by the entity key, as
--    morpho_market_position does) and is provisional on that basis, not on a measurement of THIS data.
--    Revisit with the first materializer: if positions turn out to move less than a few times a day,
--    segment by something coarser or drop segmentby entirely.
--
-- 2. The drift check can go quadratic at volume. A reviewer measured a 1,000,000-row reprocess into
--    compressed chunks at 290,938 ms against 90,635 ms uncompressed on 2.25.1-pg17, root-caused to the
--    LIMIT 5 on check (3): the planner prices the join as a nested loop that will stop early, and in the
--    normal no-drift case it never matches, so it runs source-rows x chunks probes with no Memoize on the
--    compressed side. PostgreSQL 18.4 / 2.27.2 escapes it entirely (7,729 ms) by choosing a merge join.
--    I could NOT reproduce it at 200,000 rows over 100 chunks: both the shipped form and a
--    WITH ... AS MATERIALIZED form that blocks the LIMIT pushdown pick a Parallel Hash Join and run in
--    ~40 ms, compressed or not. So the statement is left alone rather than changed on the strength of a
--    pathology that does not appear at the scale I can test and a fix I cannot show helps. If a real
--    runner hits it, MATERIALIZED on check (3) is the first thing to try.
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
-- (position_id DESC, block_number DESC, block_version DESC, processing_version DESC).
--
-- On COMPRESSED chunks that plan changes, and it changes NOW rather than later: an earlier revision of
-- this comment forecast it as a future problem, but the compression policy above makes it current. A
-- compressed chunk has no PK btree, so the global latest-per-position DISTINCT ON becomes a per-chunk
-- Seq Scan over columnstore data plus a Sort, under a Merge Append. That mechanism is exactly as
-- forecast; the cost was overstated. Measured at 200,000 observations over 120 chunks, before and after
-- compressing every chunk:
--   global DISTINCT ON     90,240 buffers / 53.9 ms  ->  3,840 buffers / 81.1 ms
--   one position's latest     243 buffers /  1.2 ms  ->    243 buffers /  2.2 ms
-- So ~1.5x slower in time and ~23x cheaper in I/O, not a collapse, and the per-position segmentby path
-- is unaffected in buffers. Note the Sort is PER CHUNK, so the time ratio grows with chunk count and 120
-- chunks is four months at 1-day chunks -- this is not measured at multi-year counts, and a global scan
-- is not the shape to rely on there. The mitigation the forecast asked for already exists: VEC-409
-- maintains position_current, an uncompressed one-row-per-position table. No reverse-lookup (instrument_key / holder_id) indexes here: no consumer yet (VEC-417/420
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
-- Guarded by role existence, mirroring security_master (20260713_140000). The guard is load-bearing for
-- stl_migrator ONLY: that role is created by the infra bootstrap and no migration creates it, so it is
-- absent under the test harness, which migrates as its own bootstrap superuser. It is dead code for
-- stl_readwrite -- 20260122_140100 creates that role unconditionally, and the GRANTs above are
-- UNGUARDED, so control cannot reach here with it missing (verified: with the roles absent this
-- migration aborts at the stl_readonly GRANT). An earlier version of this comment claimed the harness
-- has neither role, which is false.
--
-- Consequence worth stating rather than leaving implicit: because stl_migrator does not exist in CI, the
-- owner-side REVOKE never EXECUTES there, so no test run exercises it. TestPositionState asserts it only
-- when the role is present and logs a skip otherwise. Revoking the OWNER's UPDATE is safe here only because nothing
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

-- Shared materializer body for every per-protocol projection (VEC-402..407). Each projection view
-- (position_morpho_market, position_morpho_vault, position_sky_prime_debt, ...) holds its own bespoke
-- projection logic but emits the identical position_state COLUMN CONTRACT — (chain_id integer,
-- protocol_id bigint, instrument_key text, holder_id text, quantity numeric,
-- block_number bigint, block_version integer, processing_version integer, block_timestamp timestamptz).
-- deal_type_code was in this contract and is not any more: it was required and copied into the snapshot
-- but never inserted or read, because deal type is an attribute of the INSTRUMENT, not of an observation
-- (resolved instrument-side, read through VEC-409). Dropping it from the REQUIRED set is backward
-- compatible -- extra view columns are tolerated -- so a projection still emitting it keeps working.
-- RENAMING or schema-moving a projection view WEDGES it, and recovery is an operator action. Ownership
-- is keyed on the view's canonical name, stamped into every row's `projection` column, so a renamed view
-- computes a new name while its stored rows keep the old one: check (4) then raises on every subsequent
-- run for every position it owns, and the run cannot repair itself because UPDATE is revoked from
-- stl_readwrite AND from the owner stl_migrator. Recovery is a superuser re-stamp, and it must be done
-- as one transaction while the materializer is not running:
--     ALTER TABLE position_state OWNER TO <superuser>;  -- or run as the table owner
--     UPDATE position_state SET projection = '<new.qualified.name>' WHERE projection = '<old name>';
--     -- then restore the owner and re-REVOKE UPDATE, DELETE per this migration
-- Prefer NOT renaming: create the new view, let it own new positions, and retire the old name once its
-- positions stop being emitted. A rename-stable registry id would remove this entirely (deferred).
--
-- position_id is NOT part of the contract: the helper recomputes it from the identity fields via the
-- canonical IMMUTABLE position_id(), so a view cannot mint an id that stores but joins to nothing.
-- This function is the identical plumbing shared by all of them (CLAUDE.md: consolidate duplicated
-- code) — it validates the contract and APPENDS the new observations into the spine. Observations
-- only: it writes no classification. deal_type is an attribute of the INSTRUMENT, not of an
-- observation — every projection derives it from the protocol leg, which is already inside
-- instrument_key ('LOAN'::text in position_morpho_vault, 'BORROW'::text in position_sky_prime_debt,
-- a per-leg constant in position_morpho_market) — so it is resolved instrument-side and read through
-- VEC-409, and position_classification is untouched by this migration.
--
-- The contract is ENFORCED, not trusted: before touching data the helper (1) validates p_view emits
-- every contract column at its exact type (catches silent assignment-casts, e.g. a float8 quantity
-- corrupting precision), (2) fails hard if the view double-emits a logical observation key
-- (position_id, block_number, block_version, processing_version) rather than relying on SQLSTATE 21000,
-- (3) KEEPS-STORED-AND-WARNS when the view re-emits a stored key with a changed block_timestamp or
-- quantity (raising would wedge at-least-once wall-clock sources forever; rewriting is the update
-- channel the append-only default removes — a genuine correction bumps block_version or
-- processing_version, and event-time views must dedupe each key to a STABLE pick such as
-- MIN(synced_at)), and (4) fails hard when the view emits a position_id whose stored rows were written
-- by a DIFFERENT projection — disjointness is enforced through the stamped `projection` column, no
-- longer trusted by construction. The four checks all read ONE materialised snapshot of the
-- projection (a temp table). Cross-run safety: the advisory lock serialises same-view runs, and check
-- (4) keeps different views off each other's position_ids.
--
-- Classification is NOT written here. deal_type is an attribute of the instrument (every projection
-- derives it from the protocol leg, which is already inside instrument_key), so it is resolved
-- instrument-side and read through VEC-409. position_classification is untouched by this migration,
-- and collateral_status likewise belongs to whichever PR ships its writer (VEC-408).
--
-- p_view is a regclass, so it must name an existing relation — no SQL injection via the dynamic FROM.
-- Idempotent; run out of band. Returns the number of position_state rows INSERTED (append-only: nothing
-- is ever changed in place, so a rerun over unchanged data returns 0).
CREATE OR REPLACE FUNCTION materialize_position_projection(p_view regclass, p_build_id integer DEFAULT 0)
    RETURNS bigint
    LANGUAGE plpgsql
    -- Pin name resolution to the creating session's search_path, so a hostile caller search_path
    -- cannot redirect an unqualified reference to another SCHEMA. FROM CURRENT is compatible with both
    -- the per-database migrator harness and the per-schema testutil harness, unlike a hardcoded pin.
    --
    -- It does NOT defend against a pg_temp shadow, and an earlier version of this comment wrongly said
    -- it did. pg_temp is searched first for unqualified relation names regardless of the pin -- measured
    -- with this exact proconfig: a session pre-creating pg_temp.position_state absorbed the write, 1 row
    -- into the shadow and 0 into public. What defends against that is the explicit public. qualification
    -- on every permanent-table reference in the body below, which TestPositionState asserts as a class
    -- off pg_proc.prosrc rather than instance by instance. Do not un-qualify a reference on the strength
    -- of this pin.
    SET search_path FROM CURRENT
    -- Every read below probes position_state to decide what is new, what drifted, and who owns a
    -- position. A read that sees only local chunks answers all three over a partial table. The tiering
    -- policy above partitions on block_timestamp -- ON-CHAIN time, not insert time -- so a historical
    -- backfill writes chunks ALREADY older than the policy window and this function reads tiered
    -- history from the first policy run, not in a year's time.
    --
    -- WRITES to a tiered chunk are a different matter and this function cannot prevent them: tiered
    -- chunks are READ-ONLY, so the first late correction -- a new processing_version row for a block
    -- older than the tier window, which is exactly the correction channel this table mandates -- fails
    -- the INSERT and rolls back the whole single-statement run. Every retry fails identically until the
    -- chunk is brought back with `SELECT untier_chunk('<chunk>')`, so a stuck materializer whose error
    -- names a tiered chunk is an operator action, not a code bug. Recorded here because no runbook
    -- mentions it and the 1-year window can be crossed on the FIRST policy run after a historical
    -- backfill, not a year out.
    --
    -- Set explicitly rather than relied on. Measured default is 'on' for 2.25.1-pg17 (the CI pin) and
    -- 2.27.2-pg18 (the PostgreSQL major prod runs), so on those engines this is a no-op; it is here so
    -- correctness does not rest on a GUC default that a Cloud service can set per-instance.
    -- cmd/backfillers/transform-bootstrap treats failing to set it as fatal for the same reason.
    SET timescaledb.enable_tiered_reads = 'on'
    AS $fn$
DECLARE n bigint; bad text; bad_qty text; v_qualname text;
BEGIN
    -- NULL p_view would silently SKIP the advisory lock (hashtextextended is STRICT, so PERFORM of a NULL
    -- lock key is a no-op) and then fail confusingly in the contract check. Fail honestly instead.
    IF p_view IS NULL THEN
        RAISE EXCEPTION 'materialize_position_projection: p_view must not be NULL';
    END IF;

    -- Same guard, same reason: an EXPLICIT NULL p_build_id bypasses the DEFAULT 0 and survives every
    -- check here (build_id >= 0 is NULL, not false, and NOT NULL is a column constraint), so it fails
    -- only on the runs that actually INSERT -- an intermittent 23502 on a no-op-looking argument. The
    -- p_view guard exists to prevent exactly this shape of confusing, run-dependent failure.
    IF p_build_id IS NULL THEN
        RAISE EXCEPTION 'materialize_position_projection: p_build_id must not be NULL (omit it to take the default)';
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
      FROM pg_catalog.pg_class cls JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
     WHERE cls.oid = p_view;
    -- regclass holds any oid, so a dangling reference (a literal oid cast, or the view dropped between
    -- the caller's cast and this lookup) leaves v_qualname NULL. That is not cosmetic: the lock key
    -- would concatenate to NULL and pg_advisory_xact_lock is STRICT, so the run would proceed with NO
    -- lock (reproduced: 0 rows in pg_locks), and check (4) below compares `projection <> v_qualname`,
    -- which is NULL for every row and therefore passes every ownership violation silently.
    IF v_qualname IS NULL THEN
        RAISE EXCEPTION 'materialize_position_projection: p_view (oid %) does not name an existing relation', p_view::oid;
    END IF;

    -- Pin the name for the rest of the transaction, THEN read it again. AccessShareLock conflicts with
    -- the AccessExclusiveLock ALTER ... RENAME takes, so once this returns the name cannot move. Without
    -- it the lookup above was unlocked, and a rename committing between two sessions' lookups gave them
    -- different canonical names -- hence different advisory-lock keys for the same view, and no mutual
    -- exclusion at all. It has to come AFTER the existence check: locking on p_view::regclass directly
    -- renders a dangling oid as a bare number and fails with a syntax error instead of the honest
    -- message above (observed). The second read is the authoritative one: if the view was renamed in the
    -- window before the lock, the LOCK itself fails on the old name, and if it was dropped the re-read
    -- is NULL and raises the same honest error.
    --
    -- Belt-and-braces, and its removal is INVISIBLE to the suite: building the snapshot below reads the
    -- view, and that read already takes AccessShareLock held to commit, so a concurrent rename blocks
    -- with or without this line (measured -- the subtest for it passes either way, and says so). What
    -- this closes is only the window between the name lookup above and that read.
    EXECUTE format('LOCK TABLE %s IN ACCESS SHARE MODE', v_qualname);
    SELECT format('%I.%I', nsp.nspname, cls.relname) INTO v_qualname
      FROM pg_catalog.pg_class cls JOIN pg_catalog.pg_namespace nsp ON nsp.oid = cls.relnamespace
     WHERE cls.oid = p_view;
    IF v_qualname IS NULL THEN
        RAISE EXCEPTION 'materialize_position_projection: p_view (oid %) was dropped while being locked', p_view::oid;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('materialize_position_projection.' || v_qualname, 0));

    -- (1) Enforce the column contract (name + BASE type) before trusting the view. Compare on the base
    -- type with typmod stripped (format_type(atttypid, NULL)) so a losslessly assignment-compatible
    -- source column such as numeric(30,18) passes instead of failing an exact-string match against 'numeric'.
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

    -- (1a) Reject a LOSSY typmod on the two value columns. Check (1) compares the BASE type with typmod
    -- stripped, deliberately, so a wider-but-compatible source column passes -- but that also admits a
    -- narrowing one: numeric(10,0) rounds every fractional quantity to an integer and timestamptz(0)
    -- truncates the observation instant to the second, both silently, which is the same corruption class
    -- the type check was added to catch for float8. Nothing downstream can detect it: the value stores
    -- clean and this table grants no UPDATE.
    --
    -- Lossless here means "carries what the column carries": an unconstrained typmod (-1) always does.
    -- For numeric, scale >= 18 does -- 18 is the ETH-native decimal scale and the numeric(30,18) form
    -- an earlier review round explicitly required to pass. For timestamptz, only full microsecond
    -- precision (6) does. Note the typmod encodings differ per type: numeric packs precision and scale
    -- into one integer, while timestamptz's typmod IS the precision, so the numeric scale arithmetic
    -- applied to a timestamptz reads as 65532 for timestamptz(0) -- it must not be shared.
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

    -- Evaluate the projection EXACTLY ONCE into a temp table. The projection is the dominant cost (full
    -- raw scan + joins + LAG + per-row sha256); the pre-flight checks and the append all read this
    -- materialized snapshot, so the view is never rescanned (was 3x). position_id() is recomputed here,
    -- keeping identity off the view's contract.
    -- pg_temp-qualified so an unqualified _mpp_src on the caller's search_path can never resolve to a
    -- permanent relation of the same name and be dropped; the cleanup still catches a leftover temp
    -- table from an earlier same-transaction call.
    DROP TABLE IF EXISTS pg_temp._mpp_src;
    EXECUTE format($q$
        CREATE TEMP TABLE _mpp_src ON COMMIT DROP AS
        SELECT public.position_id(chain_id, protocol_id, instrument_key, holder_id) AS position_id,
               chain_id, protocol_id, instrument_key, holder_id, quantity,
               block_number, block_version, processing_version, block_timestamp
        FROM %1$s
    $q$, p_view);
    ANALYZE pg_temp._mpp_src;

    -- (1b) NULL-ness, which the column contract above cannot see: it compares name + base type, and a
    -- source column is free to be nullable. prime_debt.processing_version is in schema_master's
    -- nullable_exempt (tiered chunks blocked SET NOT NULL), so a projection over it can emit a NULL
    -- version that passes check (1), then matches NOTHING in the drift joins or the anti-join --
    -- three-valued logic, not a bug in those predicates -- and aborts the run mid-INSERT on the column's
    -- NOT NULL with a bare 23502 naming no view, no row and no position. That wedges permanently,
    -- because the same source row is re-selected on every run. Named here instead, before any write, so
    -- the operator sees which key to fix. A projection over a nullable source must COALESCE.
    SELECT string_agg(msg, ', ') INTO bad FROM (
        SELECT format('%s=NULL at bn=%s bv=%s pv=%s ik=%s',
                      c.col,
                      COALESCE(s.block_number::text, 'NULL'),
                      COALESCE(s.block_version::text, 'NULL'),
                      COALESCE(s.processing_version::text, 'NULL'),
                      COALESCE(s.instrument_key, 'NULL')) AS msg
        FROM pg_temp._mpp_src s
        -- chain_id and protocol_id are OMITTED on purpose: both are nullable on this table, per
        -- position_id()'s structural-field convention, so a NULL there is legal data and not a defect.
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
        -- ORDERed for the same reason as the drift sample below: an unordered LIMIT makes the capped
        -- message a moving target run to run, so an operator cannot tell a new offender from a reshuffle.
        ORDER BY s.block_number, s.block_version, s.processing_version, s.instrument_key, c.col
        LIMIT 5) z;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % emits NULL in a NOT NULL position_state column (a nullable source must COALESCE): %', p_view, bad;
    END IF;

    -- (2) Fail hard on a double-emitted logical observation key (do not rely on 21000). Reads the temp
    -- snapshot, so no view rescan.
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

    -- (3) KEPT-STORED, WARNED — never raised, never rewritten. A re-emitted logical key that disagrees
    -- with its stored row (changed block_timestamp, or changed quantity) keeps the stored row and logs a
    -- WARNING naming the keys. Raising here wedges at-least-once wall-clock sources forever (Sky's
    -- synced_at moves on an SQS retry, no version bump will ever come, and the app role can neither
    -- UPDATE nor DELETE to repair) — and rewriting is the update channel the append-only default (#737)
    -- removes. Contract for event-time views: dedupe each logical key to a STABLE pick (e.g.
    -- MIN(synced_at)) so drift stays the rare exception; a REAL correction arrives as a new
    -- block_version / processing_version row from the source. Recovery from a persistent warn is fixing
    -- the view's pick — nothing wrong is ever stored.
    -- ONE pass, two FILTERed aggregates. As two separate queries the quantity arm carried
    -- `p.block_timestamp = s.block_timestamp`, so a key whose timestamp AND quantity both drifted
    -- reported only the timestamp and the quantity disagreement was never logged at all. The message
    -- names encode(position_id,'hex'): bn/bv/pv alone are identical for two different positions
    -- drifting at the same block, which made the entries un-greppable. ORDER BY makes the capped
    -- sample stable run to run instead of whatever the scan happened to reach first.
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

    -- (4) Cross-view disjointness ENFORCED as data, not trusted: one projection owns a position_id. Every
    -- insert below stamps the writing view's canonical name, and this check probes ONE stored row per run
    -- position (the PK-first row — sound once a position has any stored row, because this check runs
    -- before every insert and all of a position's rows are stamped identically). It does NOT close the
    -- first-ever write: two views emitting the same new position concurrently both see no owner and both
    -- insert, so ownership is established by whichever commits first and the other's rows sit under a
    -- different projection. Views are per-protocol and disjoint by design, so this needs two projections
    -- claiming one position, which is itself the bug this check exists to surface — it will be caught on
    -- the next run rather than prevented. A different owner means two views are interleaving
    -- the same position — the overlap the per-view advisory lock cannot serialize (different views take
    -- different locks) — so fail loudly naming both views.
    -- An ANY-ROW probe, not the min-PK row. The ordered LIMIT 1 asked for each position's OLDEST
    -- observation -- i.e. its coldest chunk, the one most likely tiered to S3 -- so every run paid a
    -- read that grows with history to conclude nothing. It was also strictly WEAKER: it inspected one
    -- row, so a foreign row that did not sort first was invisible, and ownership was effectively decided
    -- by PK sort order rather than by which writer got there first. Any row with a different stamp is a
    -- violation, so the planner may stop at the first one it finds in any chunk.
    SELECT format('position %s owned by %s', encode(p.position_id, 'hex'), p.projection) INTO bad
    FROM (SELECT DISTINCT position_id FROM pg_temp._mpp_src) s
    JOIN public.position_state p
      ON p.position_id = s.position_id AND p.projection <> v_qualname
    LIMIT 1;
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'projection % emits position_ids owned by another projection (cross-view disjointness violated): %', p_view, bad;
    END IF;

    -- One statement: append the observations the view emits that are not already stored. There is no
    -- second statement any more — classification moved instrument-side — so the snapshot subtleties that
    -- shaped earlier revisions (a data-modifying CTE cannot see its own writes) no longer apply here.
    INSERT INTO public.position_state
        (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
         block_number, block_version, processing_version, block_timestamp, projection, build_id)
    SELECT s.position_id, s.chain_id, s.protocol_id, s.instrument_key, s.holder_id, s.quantity,
           s.block_number, s.block_version, s.processing_version, s.block_timestamp, v_qualname,
           p_build_id
    FROM pg_temp._mpp_src s
    WHERE NOT EXISTS (
        SELECT 1 FROM public.position_state p
        WHERE p.position_id = s.position_id AND p.block_number = s.block_number
          AND p.block_version = s.block_version AND p.processing_version = s.processing_version)
    -- Insert in CHUNK order first, then PK order within a chunk. Two reasons, and the first is the one
    -- that changed: the compression and S3 tiering policies above are chunk-level actors that take
    -- AccessExclusiveLock per chunk, and they walk chunks in time order. A writer that acquires chunk
    -- locks in a DIFFERENT order can form a cycle with them, and a review reproduced exactly that --
    -- policy_compression and this function deadlocking, with the materializer the victim and the whole
    -- run aborting on 40P01. Leading with block_timestamp makes this function's lock acquisition follow
    -- the same order the policies use.
    --
    -- Not "the cycle cannot form": a mutation sweep confirms reverting this ORDER BY is undetectable by
    -- the suite, so the ordering is argued from the lock order rather than verified by a test. I could
    -- not reproduce the deadlock in 6 attempts either; the reproduction is a reviewer's, 4/4 on their
    -- harness. Treat this as narrowing the window, with the caller-side retry below as the part that
    -- actually handles it. (An earlier version of this comment
    -- said lock ordering was "not a concern (check (4) makes cross-view row sets disjoint)" -- true of
    -- other MATERIALIZERS, and silent about the policy jobs, which are the actors that matter.)
    --
    -- The second reason is the original one and still holds: within a chunk, PK order gives B-tree
    -- bulk-load locality. Nothing depends on this ORDER BY for correctness -- a mutation sweep confirmed
    -- removing it entirely changes no result -- so reordering it is safe.
    --
    -- This is the house pattern for the problem, not a local invention: token_repository.go,
    -- position_repository.go, token_total_supply_repository.go, curve_repository.go and
    -- uniswap_v3_repository.go all sort to a canonical key "so concurrent transactions lock rows in the
    -- same order, preventing deadlocks (ADR-0002)". The only difference here is that the contended
    -- resource is a CHUNK rather than a row, so the canonical order is the chunk key.
    --
    -- Ordering reduces the window; it cannot make 40P01 impossible, and no migration can. The house
    -- second half is a caller-side retry -- retry.Do(ctx, cfg, isRetryableTxError, ...) in
    -- internal/adapters/outbound/postgres/blockstate_repository.go, whose predicate already covers
    -- 40001 and 40P01. This function is IDEMPOTENT (NOT EXISTS + ON CONFLICT DO NOTHING), so a
    -- deadlocked run loses nothing and a retry redoes it. That belongs in the calling repository, which
    -- is position_materializer_repository.go in the runner PR (#739), not here -- a migration has no
    -- caller to wrap.
    ORDER BY s.block_timestamp, s.position_id, s.block_number, s.block_version, s.processing_version
    ON CONFLICT (position_id, block_number, block_version, processing_version, block_timestamp) DO NOTHING;
    GET DIAGNOSTICS n = ROW_COUNT;

    DROP TABLE pg_temp._mpp_src;

    RETURN n;
END $fn$;

COMMENT ON FUNCTION materialize_position_projection(regclass, integer) IS '[Operational] VEC-402..407 shared materializer: validate a per-protocol projection view against the position_state column contract, fail hard on contract/type drift, double-emitted keys, or cross-view ownership violations (the stamped projection column); keep-stored-and-warn on a re-emitted key whose block_timestamp or quantity drifted (append-only: a real correction bumps block_version/processing_version), then — evaluating the projection ONCE into a temp table that every check reads — APPEND the new observations into position_state. Observations only: this function writes no classification. deal_type is an attribute of the instrument, not of an observation (it is a constant per projection leg), so it is resolved instrument-side and read through VEC-409; position_classification is untouched here. position_id is recomputed via position_id(); runs serialized per view by an advisory lock on the view''s canonical name. Idempotent; run out of band. Returns rows INSERTED.';
INSERT INTO migrations (filename) VALUES ('20260818_130000_create_position_state.sql') ON CONFLICT (filename) DO NOTHING;
