-- VEC-185: Cover assign_processing_version_offchain_token_price() lookup.
CREATE INDEX IF NOT EXISTS idx_offchain_token_price_pv_lookup
    ON offchain_token_price (token_id, source_id, timestamp, processing_version DESC);

INSERT INTO migrations (filename)
VALUES ('20260424_121300_index_offchain_token_price_processing_version.sql')
ON CONFLICT (filename) DO NOTHING;
