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
-- Plain table (point lookups by PK), not a hypertable — a curated dimension populated out of band by an
-- append-only loader (a block's metadata is immutable once known; a reorg appends a new block_version,
-- it never rewrites an existing row, so there is no full-table-upsert / compression interaction). DDL
-- only; the historical load runs out of band (a full-history load does not belong in the migrator txn).

DROP TABLE IF EXISTS block_time;  -- superseded by block_meta; empty and unconsumed (bucket-2 not built)

CREATE TABLE IF NOT EXISTS block_meta (
    chain_id        integer     NOT NULL,
    block_number    bigint      NOT NULL,
    block_version   integer     NOT NULL DEFAULT 0,
    block_timestamp timestamptz NOT NULL,          -- authoritative on-chain header time (UTC)
    -- Room for further per-block metadata as consumers need it, added in a later migration, e.g.:
    --   base_fee_per_gas numeric, gas_used bigint, gas_limit bigint, miner bytea
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT block_meta_pkey PRIMARY KEY (chain_id, block_number, block_version)
);

COMMENT ON TABLE block_meta IS '[Dimension] Canonical (chain_id, block_number, block_version) -> on-chain block metadata. block_timestamp today; extensible for per-block fields (base_fee, gas, miner). The source of block_timestamp for observation tables that carry no event-time column, via the schema_master block_time fill (VEC-491). Populated out of band from the authoritative block header in the S3 raw-block archive. Supersedes block_time.';
COMMENT ON COLUMN block_meta.chain_id IS 'PK. Chain the block belongs to.';
COMMENT ON COLUMN block_meta.block_number IS 'PK. Block height on that chain.';
COMMENT ON COLUMN block_meta.block_version IS 'PK. Reorg version; a reorged block at the same height is a distinct block with its own timestamp. Matches the S3 object version.';
COMMENT ON COLUMN block_meta.block_timestamp IS 'On-chain block-header timestamp (UTC), parsed from the S3 raw-block archive. NOT node receipt time (block_states.received_at) and NOT row ingestion time.';
COMMENT ON COLUMN block_meta.created_at IS 'Audit. Row insert time.';

INSERT INTO migrations (filename) VALUES ('20260818_120000_create_block_meta_dimension.sql') ON CONFLICT (filename) DO NOTHING;
