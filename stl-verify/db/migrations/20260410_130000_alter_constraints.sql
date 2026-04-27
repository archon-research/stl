-- Add processing_version to all PK and UNIQUE constraints.
--
-- Assumes all hypertable chunks are decompressed (previous migration).
-- Uses dynamic constraint name lookup (same pattern as hypertable conversion migrations).
--
-- See ADR-0002: Data Auditability and Processing Versioning.

-- ============================================================================
-- Helper: drop PK or UNIQUE constraint by type for a given table.
-- ============================================================================

CREATE OR REPLACE FUNCTION _drop_constraint(p_table regclass, p_type char)
RETURNS void AS $$
DECLARE v_conname text;
BEGIN
    SELECT conname INTO v_conname FROM pg_constraint
    WHERE conrelid = p_table AND contype = p_type;
    IF v_conname IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', p_table::text, v_conname);
    ELSE
        RAISE NOTICE '_drop_constraint: no constraint of type % found on %', p_type, p_table;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Tables where PK is the natural key (processing_version added before
-- the partition column so the partition column remains last in the key).
-- ============================================================================

-- borrower: PK was (user_id, protocol_id, token_id, block_number, block_version, created_at)
SELECT _drop_constraint('borrower', 'p');
ALTER TABLE borrower ADD PRIMARY KEY
    (user_id, protocol_id, token_id, block_number, block_version, processing_version, created_at);

-- borrower_collateral: same structure
SELECT _drop_constraint('borrower_collateral', 'p');
ALTER TABLE borrower_collateral ADD PRIMARY KEY
    (user_id, protocol_id, token_id, block_number, block_version, processing_version, created_at);

-- onchain_token_price: PK was (token_id, oracle_id, block_number, block_version, timestamp)
SELECT _drop_constraint('onchain_token_price', 'p');
ALTER TABLE onchain_token_price ADD PRIMARY KEY
    (token_id, oracle_id, block_number, block_version, processing_version, timestamp);

-- morpho_market_state: PK was (morpho_market_id, block_number, block_version, timestamp)
SELECT _drop_constraint('morpho_market_state', 'p');
ALTER TABLE morpho_market_state ADD PRIMARY KEY
    (morpho_market_id, block_number, block_version, processing_version, timestamp);

-- morpho_market_position: PK was (user_id, morpho_market_id, block_number, block_version, timestamp)
SELECT _drop_constraint('morpho_market_position', 'p');
ALTER TABLE morpho_market_position ADD PRIMARY KEY
    (user_id, morpho_market_id, block_number, block_version, processing_version, timestamp);

-- morpho_vault_state: PK was (morpho_vault_id, block_number, block_version, timestamp)
SELECT _drop_constraint('morpho_vault_state', 'p');
ALTER TABLE morpho_vault_state ADD PRIMARY KEY
    (morpho_vault_id, block_number, block_version, processing_version, timestamp);

-- morpho_vault_position: PK was (user_id, morpho_vault_id, block_number, block_version, timestamp)
SELECT _drop_constraint('morpho_vault_position', 'p');
ALTER TABLE morpho_vault_position ADD PRIMARY KEY
    (user_id, morpho_vault_id, block_number, block_version, processing_version, timestamp);

-- offchain_token_price: PK was (token_id, source_id, timestamp)
SELECT _drop_constraint('offchain_token_price', 'p');
ALTER TABLE offchain_token_price ADD PRIMARY KEY
    (token_id, source_id, processing_version, timestamp);

-- protocol_event: PK was (chain_id, block_number, block_version, tx_hash, log_index, created_at)
SELECT _drop_constraint('protocol_event', 'p');
ALTER TABLE protocol_event ADD PRIMARY KEY
    (chain_id, block_number, block_version, tx_hash, log_index, processing_version, created_at);

-- allocation_position: PK was (chain_id, token_id, prime_id, proxy_address, block_number,
--                               block_version, tx_hash, log_index, direction, created_at)
SELECT _drop_constraint('allocation_position', 'p');
ALTER TABLE allocation_position ADD PRIMARY KEY
    (chain_id, token_id, prime_id, proxy_address, block_number, block_version,
     tx_hash, log_index, direction, processing_version, created_at);

-- ============================================================================
-- Tables where a UNIQUE constraint is the natural key (surrogate PK or no PK).
-- processing_version added to the UNIQUE constraint, not the PK.
-- ============================================================================

-- sparklend_reserve_data: UNIQUE was (protocol_id, token_id, block_number, block_version)
-- Surrogate PK (id, block_number) is unchanged.
SELECT _drop_constraint('sparklend_reserve_data', 'u');
ALTER TABLE sparklend_reserve_data ADD UNIQUE
    (protocol_id, token_id, block_number, block_version, processing_version);

-- prime_debt: UNIQUE was (prime_id, block_number, block_version, synced_at). No PK.
SELECT _drop_constraint('prime_debt', 'u');
ALTER TABLE prime_debt ADD UNIQUE
    (prime_id, block_number, block_version, processing_version, synced_at);

-- anchorage_package_snapshot: UNIQUE was (prime_id, package_id, asset_type, custody_type, snapshot_time)
SELECT _drop_constraint('anchorage_package_snapshot', 'u');
ALTER TABLE anchorage_package_snapshot ADD UNIQUE
    (prime_id, package_id, asset_type, custody_type, processing_version, snapshot_time);

-- anchorage_operation: UNIQUE was (operation_id, created_at)
SELECT _drop_constraint('anchorage_operation', 'u');
ALTER TABLE anchorage_operation ADD UNIQUE
    (operation_id, processing_version, created_at);

-- ============================================================================
-- Cleanup helper function
-- ============================================================================
DROP FUNCTION _drop_constraint(regclass, char);

INSERT INTO migrations (filename)
VALUES ('20260410_130000_alter_constraints.sql')
ON CONFLICT (filename) DO NOTHING;
