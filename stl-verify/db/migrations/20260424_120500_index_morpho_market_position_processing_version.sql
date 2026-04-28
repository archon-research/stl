-- VEC-185: Cover assign_processing_version_morpho_market_position() lookup.
CREATE INDEX IF NOT EXISTS idx_morpho_market_position_pv_lookup
    ON morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, processing_version DESC);

INSERT INTO migrations (filename)
VALUES ('20260424_120500_index_morpho_market_position_processing_version.sql')
ON CONFLICT (filename) DO NOTHING;
