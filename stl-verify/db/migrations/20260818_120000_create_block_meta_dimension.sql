-- VEC-491 (supersedes the block_time dimension): block_meta — canonical
-- (chain_id, block_number, block_version) -> on-chain block metadata. block_timestamp today; the table
-- is the home for further per-block fields as consumers need them (base_fee, gas_used, miner, ...).
--
-- The bucket-2 transform and the position materializers recover block_timestamp for the raw tables that
-- carry no event-time column (borrower, borrower_collateral, allocation_position, prime_debt,
-- protocol_event, sparklend_reserve_data) by joining this dimension on (chain_id, block_number,
-- block_version) — the schema_master `block_time` fill. Those tables therefore never store
-- block_timestamp natively; they only need block_number + block_version + a resolvable chain.
--
-- Source: the AUTHORITATIVE block header in the S3 raw-block archive (raw_data_backup writes
-- {partition}/{block}_{version}_block.json.gz; the header carries the exact on-chain timestamp),
-- parsed out of band by a loader. This replaces block_time's block_states + onchain_token_price fill:
-- block_states is a rolling ~1-month window (no history), and onchain_token_price is a <=12s
-- price-observation proxy with ~850 conflicting-timestamp blocks — the S3 header is exact and reaches
-- the historical deep tail. block_time is empty and unconsumed (bucket-2 unbuilt), so it is dropped.
--
-- block_version is in the PK because a reorg replaces the block at a height with a different block, with
-- its own header timestamp; the S3 archive is likewise keyed by (block_number, version).
--
-- SCOPE (this migration assumes it): block_meta holds only the blocks the observation tables
-- REFERENCE — the VEC-491 loader's work-list is that referenced set, per chain — NOT every backed-up
-- block on every chain. At that scope (tens of millions backfilled once, then a few million rows/month)
-- a PLAIN table is correct: point lookups by (chain_id, block_number, block_version), no time predicate,
-- no retention. If the scope ever widens to every backed-up block on all chains (block_states' rate,
-- ~19M rows/month, largest table in the DB within a year), revisit this as an INTEGER hypertable
-- partitioned by block_number — never by block_timestamp, which would force the timestamp into the PK
-- and lose the lookup-key uniqueness and never get chunk exclusion (the join carries no timestamp) —
-- with manual per-chain columnstore compression (there is no single integer_now across chains for an
-- add_columnstore_policy). create_hypertable on the empty table is free; migrate_data on a populated
-- one is not, hence deciding now.
--
-- Plain table (point lookups by PK): a curated dimension populated out of band by an append-only loader
-- (a block's metadata is immutable once known; a reorg appends a new block_version, it never rewrites an
-- existing row, so there is no full-table-upsert / compression interaction). DDL only; the historical
-- load runs out of band (a full-history load does not belong in the migrator txn).

DROP TABLE IF EXISTS block_time;  -- superseded by block_meta; empty and unconsumed (bucket-2 not built)

CREATE TABLE IF NOT EXISTS block_meta (
    chain_id           integer     NOT NULL,
    block_number       bigint      NOT NULL,
    block_version      integer     NOT NULL DEFAULT 0,
    block_timestamp    timestamptz NOT NULL,          -- authoritative on-chain header time, stored as a UTC instant
    -- Further per-block header fields (base_fee_per_gas, gas_used, gas_limit, miner, ...) live here as
    -- jsonb rather than a migration per field; one that becomes hot (indexed / filtered) can be promoted
    -- to a typed column later. Mirrors the protocol.metadata jsonb pattern. Nullable: block_timestamp is
    -- the only field the loader writes today.
    metadata           jsonb,
    -- Auditability (ADR-0002), full pattern. processing_version is IN the PK: the loader is the fallible
    -- producer — a mis-parsed header, a wrong S3 key, or a corrected chain-keying is an "internal
    -- processing error" in the ADR's sense — so a corrected row must be able to coexist with the original
    -- rather than be silently dropped by ON CONFLICT. It is assigned by assign_processing_version_block_meta
    -- (below); consumers take the latest via DISTINCT ON (...) ORDER BY processing_version DESC. build_id is
    -- metadata (which loader build wrote the row), NOT in the PK. NOT NULL DEFAULT 0 inline (new/empty
    -- table, so no tiered-scan constraint like the 20260410_110000 retrofit).
    processing_version integer     NOT NULL DEFAULT 0,
    build_id           integer     NOT NULL DEFAULT 0,
    created_at         timestamptz NOT NULL DEFAULT now(),  -- row insert time (timestamptz = UTC instant)
    CONSTRAINT block_meta_pkey PRIMARY KEY (chain_id, block_number, block_version, processing_version),
    -- Corruption guards at the chokepoint (same rationale as position_state's round-5 CHECKs). The loader
    -- parses hex header fields, so an epoch-zero timestamp from a parse bug, or a negative/zero coordinate
    -- from a bad S3 key, must fail loudly rather than be served as event-time to every fill consumer.
    -- No blockchain predates Bitcoin's genesis (2009-01-03); deliberately no upper bound (now()-relative
    -- CHECKs are non-immutable and would reject legitimate clock-skewed live blocks).
    CONSTRAINT block_meta_coord_nonneg_chk
        CHECK (block_number >= 0 AND block_version >= 0 AND processing_version >= 0),
    CONSTRAINT block_meta_chain_pos_chk CHECK (chain_id > 0),
    CONSTRAINT block_meta_ts_sane_chk CHECK (block_timestamp >= '2009-01-03 00:00:00+00'::timestamptz)
);

COMMENT ON TABLE block_meta IS '[Dimension] Canonical (chain_id, block_number, block_version) -> on-chain block metadata. block_timestamp today; extensible for per-block fields (base_fee, gas, miner). The source of block_timestamp for observation tables that carry no event-time column, via the schema_master block_time fill (VEC-491). Populated out of band from the authoritative block header in the S3 raw-block archive. Supersedes block_time.';
COMMENT ON COLUMN block_meta.chain_id IS 'PK. Chain the block belongs to.';
COMMENT ON COLUMN block_meta.block_number IS 'PK. Block height on that chain.';
COMMENT ON COLUMN block_meta.block_version IS 'PK. Reorg version; a reorged block at the same height is a distinct block with its own timestamp. Matches the S3 object version.';
COMMENT ON COLUMN block_meta.block_timestamp IS 'On-chain block-header timestamp as a UTC instant: the header''s Unix epoch seconds, stored via timestamptz (the loader normalizes to UTC). NOT node receipt time (block_states.received_at) and NOT row ingestion time.';
COMMENT ON COLUMN block_meta.metadata IS 'Optional further per-block header fields as jsonb (e.g. base_fee_per_gas, gas_used, gas_limit, miner). Extensibility point so new fields need no per-field migration; promote a hot field to a typed column when it warrants indexing. Mirrors protocol.metadata.';
COMMENT ON COLUMN block_meta.processing_version IS 'PK. Audit (ADR-0002): internal correction version (0 = original, N = Nth reprocessing), assigned by the trigger. Lets a corrected block_timestamp coexist with the original for audit; consumers take the highest processing_version per (chain_id, block_number, block_version).';
COMMENT ON COLUMN block_meta.build_id IS 'Audit (ADR-0002). Pointer to build_registry.id — which loader build wrote this row (metadata, not identity). Set by the out-of-band loader; matters here because the loader''s chain-keying is still evolving.';
COMMENT ON COLUMN block_meta.created_at IS 'Audit. Row insert time (timestamptz, a UTC instant).';

-- Assign processing_version per ADR-0002 (same structure as the assign_processing_version_* family):
-- an idempotent retry (same natural key AND same build_id) reuses the existing version so the ON
-- CONFLICT on the full PK dedupes it; a reprocess under a corrected build (different build_id) gets
-- MAX+1, so the corrected row coexists with the original for audit. Natural key is
-- (chain_id, block_number, block_version). plan_cache_mode is set inside the function per ADR-0002
-- (CREATE OR REPLACE resets proconfig, so a detached ALTER would not survive a later edit).
CREATE OR REPLACE FUNCTION assign_processing_version_block_meta()
RETURNS TRIGGER
LANGUAGE plpgsql
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    -- Serialize concurrent inserts for the same natural key (READ COMMITTED required).
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('bm|%s|%s|%s', NEW.chain_id, NEW.block_number, NEW.block_version), 0));

    -- Retry: this build already produced a version for this key -> reuse it, ON CONFLICT dedupes.
    SELECT processing_version INTO existing_ver
    FROM block_meta
    WHERE chain_id = NEW.chain_id
      AND block_number = NEW.block_number
      AND block_version = NEW.block_version
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        -- Reprocess (or first write): assign the next version.
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM block_meta
        WHERE chain_id = NEW.chain_id
          AND block_number = NEW.block_number
          AND block_version = NEW.block_version;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON block_meta
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_block_meta();

INSERT INTO migrations (filename) VALUES ('20260818_120000_create_block_meta_dimension.sql') ON CONFLICT (filename) DO NOTHING;
