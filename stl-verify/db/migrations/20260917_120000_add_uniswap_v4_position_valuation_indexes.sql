-- VEC-829: the allocation tracker's V4 posm valuation reads uniswap_v4_position
-- by (owner, salt) and uniswap_v4_position_nft_transfer by (position_manager_id,
-- to_address) — neither is a prefix of an existing index on either
-- unbounded-growth table, so both queries would otherwise seq-scan.

CREATE INDEX IF NOT EXISTS idx_uniswap_v4_position_owner_salt_block
    ON uniswap_v4_position (owner, salt, block_number DESC, block_version DESC, processing_version DESC);

CREATE INDEX IF NOT EXISTS idx_uniswap_v4_position_nft_transfer_to_address
    ON uniswap_v4_position_nft_transfer (position_manager_id, to_address, block_number);

INSERT INTO migrations (filename)
VALUES ('20260917_120000_add_uniswap_v4_position_valuation_indexes.sql')
ON CONFLICT (filename) DO NOTHING;
