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
-- SCOPE: block_meta holds only the blocks the observation tables REFERENCE — the VEC-491 loader's
-- work-list is that referenced set, per chain — NOT every backed-up block on every chain.
--
-- MEASURED against prod on 2026-08-25, not estimated. An earlier version of this header guessed "tens
-- of millions backfilled once, then a few million rows/month"; both terms were wrong by more than an
-- order of magnitude. The referenced set is 1,192,342 rows across 6 chains, of which 1,325 carry
-- block_version > 0 (reorgs, ~0.1%) — roughly 68 MB of heap. Per table before dedup: protocol_event
-- 1,093,915, sparklend_reserve_data 801,593, borrower_collateral 760,210, borrower 295,038,
-- allocation_position 147,347, prime_debt 15,484. They sum to 3.1M and dedup to 1.2M, because the six
-- tables observe heavily overlapping blocks. Re-run to re-check before any decision that depends on it:
--
--   WITH referenced AS (
--       SELECT p.chain_id, b.block_number, b.block_version
--         FROM borrower b JOIN protocol p ON p.id = b.protocol_id
--       UNION SELECT p.chain_id, bc.block_number, bc.block_version
--         FROM borrower_collateral bc JOIN protocol p ON p.id = bc.protocol_id
--       UNION SELECT chain_id, block_number, block_version FROM allocation_position
--       UNION SELECT chain_id, block_number, block_version FROM protocol_event
--       UNION SELECT p.chain_id, sr.block_number, sr.block_version
--         FROM sparklend_reserve_data sr JOIN protocol p ON p.id = sr.protocol_id
--       UNION SELECT 1::int, block_number, block_version FROM prime_debt)
--   SELECT count(*) FROM referenced;
--
-- At 1.2M rows a PLAIN table is correct, and not marginally: point lookups by
-- (chain_id, block_number, block_version), no time predicate, no retention. Compression would save
-- megabytes, against a standing manual compress_chunk chore — an integer dimension gets one
-- integer_now_func for the whole table and there is no single "now" across six chains, so no
-- add_columnstore_policy is possible and something would have to compress bands by hand, forever.
--
-- If the scope ever widens to every backed-up block on all chains (block_states' rate, ~19M rows/month,
-- largest table in the DB within a year), revisit as an INTEGER hypertable partitioned by block_number
-- — never by block_timestamp, which would force the timestamp into the PK, lose the lookup-key
-- uniqueness, and never get chunk exclusion since the join carries no timestamp. create_hypertable on
-- an empty table is free and migrate_data on a populated one is not, so re-run the count above before
-- the table grows rather than after.
--
-- Plain table (point lookups by PK): a curated dimension populated out of band by an append-only loader
-- (a block's metadata is immutable once known; a reorg appends a new block_version, it never rewrites an
-- existing row, so there is no full-table-upsert / compression interaction). DDL only; the historical
-- load runs out of band (a full-history load does not belong in the migrator txn).

-- Superseded by block_meta. "Empty and unconsumed" is a review-time claim about the environments we
-- checked, not a property the migrator can assume: docs/runbooks/block-time-backfill.md is a live,
-- idempotent 19M-row population procedure, so any environment that ran it holds rows a bare DROP would
-- destroy silently. Guard it -- a loud stop is recoverable, a silent truncate of an out-of-band backfill
-- is not. (No CASCADE either, so a dependent object also aborts rather than being dropped.)
DO $$
DECLARE n bigint;
BEGIN
    IF to_regclass('public.block_time') IS NOT NULL THEN
        EXECUTE 'SELECT count(*) FROM block_time' INTO n;
        IF n > 0 THEN
            RAISE EXCEPTION 'block_time holds % row(s); refusing to drop. Migrate or truncate deliberately, then re-run.', n;
        END IF;
        DROP TABLE block_time;
    END IF;
END $$;

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
    -- (below); consumers read block_meta_current, which does the DISTINCT ON for them. build_id is
    -- metadata (which loader build wrote the row), NOT in the PK. NOT NULL DEFAULT 0 inline (new/empty
    -- table, so no tiered-scan constraint like the 20260410_110000 retrofit).
    processing_version integer     NOT NULL DEFAULT 0,
    build_id           integer     NOT NULL DEFAULT 0,
    created_at         timestamptz NOT NULL DEFAULT now(),  -- row insert time (timestamptz = UTC instant)
    CONSTRAINT block_meta_pkey PRIMARY KEY (chain_id, block_number, block_version, processing_version),
    -- Corruption guards at the chokepoint (same rationale as position_state's round-5 CHECKs). The loader
    -- parses hex header fields, so an epoch-zero timestamp from a parse bug, or a negative/zero coordinate
    -- from a bad S3 key, must fail loudly rather than be served as event-time to every fill consumer.
    -- No blockchain predates Bitcoin's genesis (2009-01-03). The upper bound is a FIXED constant, not
    -- now()-relative: a now() CHECK is non-immutable, but a static ceiling is not, and the parse-bug space
    -- is lopsided -- a wrong hex field, a dropped nibble, or ms-for-s all overshoot, so a floor-only guard
    -- faces the less likely direction. Measured against a floor-only guard, all three passed: base_fee
    -- parsed as the timestamp (year 2603), seconds x1000 (year 55055), a dropped leading nibble (2043).
    -- The 2100 ceiling catches the first two and NOT the third -- stated plainly because the limit is the
    -- point: a static bound can only catch gross overshoots. A modest misparse still lands inside it, and
    -- the only thing that catches those is a cross-check against a known-good source (block_states.created_at
    -- is 0-delta accurate over its rolling window, per the block-time runbook) -- loader-side work, not a
    -- CHECK. A tighter ceiling is not the answer either: it buys a few more parse bugs and costs a
    -- migration when the constant is reached.
    CONSTRAINT block_meta_coord_nonneg_chk
        CHECK (block_number >= 0 AND block_version >= 0 AND processing_version >= 0 AND build_id >= 0),
    CONSTRAINT block_meta_chain_pos_chk CHECK (chain_id > 0),
    CONSTRAINT block_meta_ts_sane_chk CHECK (block_timestamp >= '2009-01-03 00:00:00+00'::timestamptz
                                         AND block_timestamp <  '2100-01-01 00:00:00+00'::timestamptz)
);

COMMENT ON TABLE block_meta IS '[Dimension] Canonical (chain_id, block_number, block_version) -> on-chain block metadata. block_timestamp today; extensible for per-block fields (base_fee, gas, miner). The source of block_timestamp for observation tables that carry no event-time column, via the schema_master block_time fill (VEC-491). Populated out of band from the authoritative block header in the S3 raw-block archive. Supersedes block_time. PLAIN table, not a hypertable: every read is an equality point lookup on the PK prefix and no consumer join carries a time-range predicate, so there is nothing for chunk exclusion to act on; scope is the referenced-block set per chain, not every archived block. If scope ever widens, partition per chain -- never a shared block_number space, which would collocate unrelated chains (ETH ~16-25M and Avalanche ~88-91M coexist in one raw table).';
COMMENT ON COLUMN block_meta.chain_id IS 'PK. Chain the block belongs to.';
COMMENT ON COLUMN block_meta.block_number IS 'PK. Block height on that chain.';
COMMENT ON COLUMN block_meta.block_version IS 'PK. Reorg version; a reorged block at the same height is a distinct block with its own timestamp. Matches the S3 object version.';
COMMENT ON COLUMN block_meta.block_timestamp IS 'On-chain block-header timestamp as a UTC instant: the header''s Unix epoch seconds, stored via timestamptz (the loader normalizes to UTC). NOT node receipt time (block_states.received_at) and NOT row ingestion time.';
COMMENT ON COLUMN block_meta.metadata IS 'Optional further per-block header fields as jsonb (e.g. base_fee_per_gas, gas_used, gas_limit, miner). Extensibility point so new fields need no per-field migration; promote a hot field to a typed column when it warrants indexing. Mirrors protocol.metadata.';
COMMENT ON COLUMN block_meta.processing_version IS 'PK. Audit (ADR-0002): internal correction version (0 = original, N = Nth reprocessing), assigned by the trigger. Lets a corrected block_timestamp coexist with the original for audit. Consumers MUST read block_meta_current, not this table: a raw three-column join fans out once any correction exists and silently doubles downstream aggregates.';
COMMENT ON COLUMN block_meta.build_id IS 'Audit (ADR-0002). Pointer to build_registry.id — which loader build wrote this row (metadata, not identity). Set by the out-of-band loader; matters here because the loader''s chain-keying is still evolving.';
COMMENT ON COLUMN block_meta.created_at IS 'Audit. Row insert time (timestamptz, a UTC instant).';

-- Current view. processing_version is in the PK, so the natural key (chain_id, block_number,
-- block_version) is NOT unique in the base table: the three-column join the schema_master fill performs
-- returns one row per correction, multiplying the observation row and silently doubling any aggregate
-- over it. That is a wrong number, not an error, across six raw tables. The repo's answer to exactly this
-- is a shipped view rather than a convention every consumer must remember -- security_master_current,
-- entity_master_current, entity_ref_codes_current, security_instrument_bridge_current, maple_*_current --
-- and ADR-0002 is explicit that latest-wins "is a convention, not a database constraint". So the fill and
-- every consumer join block_meta_current, never block_meta.
--
-- No valid-time predicate here (contrast the SCD2 masters): a block header is immutable on-chain, so
-- processing_version is the only axis, and there is nothing whose effectivity could be dated.
CREATE OR REPLACE VIEW block_meta_current AS
SELECT DISTINCT ON (chain_id, block_number, block_version) *
FROM block_meta
ORDER BY chain_id, block_number, block_version, processing_version DESC;

COMMENT ON VIEW block_meta_current IS '[Dimension] Latest processing_version per (chain_id, block_number, block_version). THE read surface for the schema_master block_time fill and every consumer -- joining block_meta directly fans out once a correction exists.';

GRANT SELECT ON block_meta, block_meta_current TO stl_readonly;
-- Append-only, per the team default in db/migrations/AGENTS.md ("append-only is the DEFAULT, for every
-- table"). The whole design rests on it: a block header is immutable once known, a reorg appends a new
-- block_version, and a correction appends a new processing_version. Nothing legitimately rewrites a
-- stored row. The roles migration's ALTER DEFAULT PRIVILEGES grants stl_readwrite full DML on every
-- stl_migrator table, so the narrowed GRANT below removes nothing by itself -- the explicit REVOKE does.
-- Without it the header's claim that the loader "never rewrites an existing row" is unenforced, and an
-- UPDATE would silently rewrite an audited timestamp with processing_version unchanged.
GRANT SELECT, INSERT ON block_meta TO stl_readwrite;
GRANT SELECT ON block_meta_current TO stl_readwrite;
-- Role-existence guard mirrors position_state (20260818_130000): stl_migrator is created by the infra
-- bootstrap, not by any migration, so it is absent under the test harness and the owner-side REVOKE does
-- not execute in CI. Revoking the owner's UPDATE is safe here because nothing FKs block_meta -- a future
-- FK would hit the RI-probe privilege trap that 20260714_160000 fixed for the reference tables.
DO $$
DECLARE role text;
BEGIN
    FOREACH role IN ARRAY ARRAY['stl_readwrite','stl_migrator'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role) THEN
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON block_meta FROM %I', role);
        END IF;
    END LOOP;
END $$;

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

INSERT INTO migrations (filename) VALUES ('20260822_120000_create_block_meta_dimension.sql') ON CONFLICT (filename) DO NOTHING;
