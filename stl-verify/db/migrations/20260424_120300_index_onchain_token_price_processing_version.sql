-- VEC-185: Cover assign_processing_version_onchain_token_price() lookup.
CREATE INDEX IF NOT EXISTS idx_onchain_token_price_pv_lookup
    ON onchain_token_price (token_id, oracle_id, block_number, block_version, timestamp, processing_version DESC);

INSERT INTO migrations (filename)
VALUES ('20260424_120300_index_onchain_token_price_processing_version.sql')
ON CONFLICT (filename) DO NOTHING;
