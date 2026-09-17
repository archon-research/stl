-- migrate: no-transaction
-- VEC-829: the allocation tracker's V4 posm valuation reads uniswap_v4_position
-- by (owner, salt) and uniswap_v4_position_nft_transfer by (position_manager_id,
-- to_address) — neither is a prefix of an existing index on either
-- unbounded-growth table, so both queries would otherwise seq-scan.
--
-- Both target tables are plain (non-hypertable) and are actively written by the
-- live V4 indexer, so CONCURRENTLY avoids the ShareLock a plain CREATE INDEX
-- would hold for the build's duration (see 20260130_210000_simplify_publish_tracking.sql
-- for the same live-write, non-hypertable precedent).

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_uniswap_v4_position_owner_salt_block
    ON uniswap_v4_position (owner, salt, block_number DESC, block_version DESC, processing_version DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_uniswap_v4_position_nft_transfer_to_address
    ON uniswap_v4_position_nft_transfer (position_manager_id, to_address, block_number);

INSERT INTO migrations (filename)
VALUES ('20260917_120000_add_uniswap_v4_position_valuation_indexes.sql')
ON CONFLICT (filename) DO NOTHING;
