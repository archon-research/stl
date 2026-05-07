-- VEC-185: Cover processing_version trigger lookups across append-only snapshot tables.
CREATE INDEX IF NOT EXISTS idx_borrower_pv_lookup
    ON borrower (user_id, protocol_id, token_id, block_number, block_version, created_at, processing_version DESC);

CREATE INDEX IF NOT EXISTS idx_borrower_collateral_pv_lookup
    ON borrower_collateral (user_id, protocol_id, token_id, block_number, block_version, created_at, processing_version DESC);

CREATE INDEX IF NOT EXISTS idx_sparklend_reserve_data_pv_lookup
    ON sparklend_reserve_data (protocol_id, token_id, block_number, block_version, processing_version DESC);

CREATE INDEX IF NOT EXISTS idx_onchain_token_price_pv_lookup
    ON onchain_token_price (token_id, oracle_id, block_number, block_version, timestamp, processing_version DESC);

CREATE INDEX IF NOT EXISTS idx_morpho_market_state_pv_lookup
    ON morpho_market_state (morpho_market_id, block_number, block_version, timestamp, processing_version DESC);

CREATE INDEX IF NOT EXISTS idx_morpho_market_position_pv_lookup
    ON morpho_market_position (user_id, morpho_market_id, block_number, block_version, timestamp, processing_version DESC);

CREATE INDEX IF NOT EXISTS idx_morpho_vault_state_pv_lookup
    ON morpho_vault_state (morpho_vault_id, block_number, block_version, timestamp, processing_version DESC);

CREATE INDEX IF NOT EXISTS idx_morpho_vault_position_pv_lookup
    ON morpho_vault_position (user_id, morpho_vault_id, block_number, block_version, timestamp, processing_version DESC);

CREATE INDEX IF NOT EXISTS idx_prime_debt_pv_lookup
    ON prime_debt (prime_id, block_number, block_version, synced_at, processing_version DESC);

CREATE INDEX IF NOT EXISTS idx_allocation_position_pv_lookup
    ON allocation_position (chain_id, token_id, prime_id, proxy_address, block_number, block_version, tx_hash, log_index, direction, created_at, processing_version DESC);

CREATE INDEX IF NOT EXISTS idx_protocol_event_pv_lookup
    ON protocol_event (chain_id, block_number, block_version, tx_hash, log_index, created_at, processing_version DESC);

CREATE INDEX IF NOT EXISTS idx_anchorage_package_snapshot_pv_lookup
    ON anchorage_package_snapshot (prime_id, package_id, asset_type, custody_type, snapshot_time, processing_version DESC);

CREATE INDEX IF NOT EXISTS idx_anchorage_operation_pv_lookup
    ON anchorage_operation (operation_id, created_at, processing_version DESC);

CREATE INDEX IF NOT EXISTS idx_offchain_token_price_pv_lookup
    ON offchain_token_price (token_id, source_id, timestamp, processing_version DESC);

INSERT INTO migrations (filename)
VALUES ('20260424_120000_index_processing_version_lookups.sql')
ON CONFLICT (filename) DO NOTHING;
