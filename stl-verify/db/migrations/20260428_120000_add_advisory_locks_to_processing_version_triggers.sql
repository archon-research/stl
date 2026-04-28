-- Add pg_advisory_xact_lock to every assign_processing_version_* trigger function
-- so concurrent inserts at the same natural key from different build_ids are
-- serialized at the database level — fulfilling the contract in ADR-0002 §3
-- (Concurrency).
--
-- Why this exists: the original 20260410_150000_create_processing_version_triggers.sql
-- omitted the lock and relied on row-level locking under the unique index, but row
-- locking only kicks in after the trigger has already computed
-- MAX(processing_version) + 1 — so two concurrent inserts at the same natural key
-- both compute the same version, collide on the unique constraint, and the loser is
-- silently dropped by the caller's ON CONFLICT DO NOTHING. See VEC-194.
--
-- Triggers and existing data are unchanged. Lock-key prefixes are unique per table
-- and mirror ADR-0002's `mms` / `ofp` convention.
--
-- Callers must sort batched rows by natural key before INSERT to keep
-- per-row lock acquisition order consistent across transactions; see VEC-194 PR.

-- ============================================================================
-- Blockchain-derived tables
-- ============================================================================

CREATE OR REPLACE FUNCTION assign_processing_version_borrower()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('bwr|%s|%s|%s|%s|%s|%s',
            NEW.user_id, NEW.protocol_id, NEW.token_id,
            NEW.block_number, NEW.block_version, NEW.created_at),
        0));

    SELECT processing_version INTO existing_ver
    FROM borrower
    WHERE user_id = NEW.user_id
      AND protocol_id = NEW.protocol_id
      AND token_id = NEW.token_id
      AND block_number = NEW.block_number
      AND block_version = NEW.block_version
      AND created_at = NEW.created_at
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM borrower
        WHERE user_id = NEW.user_id
          AND protocol_id = NEW.protocol_id
          AND token_id = NEW.token_id
          AND block_number = NEW.block_number
          AND block_version = NEW.block_version
          AND created_at = NEW.created_at;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_processing_version_borrower_collateral()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('bwc|%s|%s|%s|%s|%s|%s',
            NEW.user_id, NEW.protocol_id, NEW.token_id,
            NEW.block_number, NEW.block_version, NEW.created_at),
        0));

    SELECT processing_version INTO existing_ver
    FROM borrower_collateral
    WHERE user_id = NEW.user_id
      AND protocol_id = NEW.protocol_id
      AND token_id = NEW.token_id
      AND block_number = NEW.block_number
      AND block_version = NEW.block_version
      AND created_at = NEW.created_at
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM borrower_collateral
        WHERE user_id = NEW.user_id
          AND protocol_id = NEW.protocol_id
          AND token_id = NEW.token_id
          AND block_number = NEW.block_number
          AND block_version = NEW.block_version
          AND created_at = NEW.created_at;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_processing_version_sparklend_reserve_data()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('srd|%s|%s|%s|%s',
            NEW.protocol_id, NEW.token_id,
            NEW.block_number, NEW.block_version),
        0));

    SELECT processing_version INTO existing_ver
    FROM sparklend_reserve_data
    WHERE protocol_id = NEW.protocol_id
      AND token_id = NEW.token_id
      AND block_number = NEW.block_number
      AND block_version = NEW.block_version
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM sparklend_reserve_data
        WHERE protocol_id = NEW.protocol_id
          AND token_id = NEW.token_id
          AND block_number = NEW.block_number
          AND block_version = NEW.block_version;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_processing_version_onchain_token_price()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('otp|%s|%s|%s|%s|%s',
            NEW.token_id, NEW.oracle_id,
            NEW.block_number, NEW.block_version, NEW.timestamp),
        0));

    SELECT processing_version INTO existing_ver
    FROM onchain_token_price
    WHERE token_id = NEW.token_id
      AND oracle_id = NEW.oracle_id
      AND block_number = NEW.block_number
      AND block_version = NEW.block_version
      AND timestamp = NEW.timestamp
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM onchain_token_price
        WHERE token_id = NEW.token_id
          AND oracle_id = NEW.oracle_id
          AND block_number = NEW.block_number
          AND block_version = NEW.block_version
          AND timestamp = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_processing_version_morpho_market_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('mms|%s|%s|%s|%s',
            NEW.morpho_market_id, NEW.block_number, NEW.block_version, NEW.timestamp),
        0));

    SELECT processing_version INTO existing_ver
    FROM morpho_market_state
    WHERE morpho_market_id = NEW.morpho_market_id
      AND block_number = NEW.block_number
      AND block_version = NEW.block_version
      AND timestamp = NEW.timestamp
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM morpho_market_state
        WHERE morpho_market_id = NEW.morpho_market_id
          AND block_number = NEW.block_number
          AND block_version = NEW.block_version
          AND timestamp = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_processing_version_morpho_market_position()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('mmp|%s|%s|%s|%s|%s',
            NEW.user_id, NEW.morpho_market_id,
            NEW.block_number, NEW.block_version, NEW.timestamp),
        0));

    SELECT processing_version INTO existing_ver
    FROM morpho_market_position
    WHERE user_id = NEW.user_id
      AND morpho_market_id = NEW.morpho_market_id
      AND block_number = NEW.block_number
      AND block_version = NEW.block_version
      AND timestamp = NEW.timestamp
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM morpho_market_position
        WHERE user_id = NEW.user_id
          AND morpho_market_id = NEW.morpho_market_id
          AND block_number = NEW.block_number
          AND block_version = NEW.block_version
          AND timestamp = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_processing_version_morpho_vault_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('mvs|%s|%s|%s|%s',
            NEW.morpho_vault_id, NEW.block_number, NEW.block_version, NEW.timestamp),
        0));

    SELECT processing_version INTO existing_ver
    FROM morpho_vault_state
    WHERE morpho_vault_id = NEW.morpho_vault_id
      AND block_number = NEW.block_number
      AND block_version = NEW.block_version
      AND timestamp = NEW.timestamp
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM morpho_vault_state
        WHERE morpho_vault_id = NEW.morpho_vault_id
          AND block_number = NEW.block_number
          AND block_version = NEW.block_version
          AND timestamp = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_processing_version_morpho_vault_position()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('mvp|%s|%s|%s|%s|%s',
            NEW.user_id, NEW.morpho_vault_id,
            NEW.block_number, NEW.block_version, NEW.timestamp),
        0));

    SELECT processing_version INTO existing_ver
    FROM morpho_vault_position
    WHERE user_id = NEW.user_id
      AND morpho_vault_id = NEW.morpho_vault_id
      AND block_number = NEW.block_number
      AND block_version = NEW.block_version
      AND timestamp = NEW.timestamp
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM morpho_vault_position
        WHERE user_id = NEW.user_id
          AND morpho_vault_id = NEW.morpho_vault_id
          AND block_number = NEW.block_number
          AND block_version = NEW.block_version
          AND timestamp = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_processing_version_prime_debt()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('pdb|%s|%s|%s|%s',
            NEW.prime_id, NEW.block_number, NEW.block_version, NEW.synced_at),
        0));

    SELECT processing_version INTO existing_ver
    FROM prime_debt
    WHERE prime_id = NEW.prime_id
      AND block_number = NEW.block_number
      AND block_version = NEW.block_version
      AND synced_at = NEW.synced_at
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM prime_debt
        WHERE prime_id = NEW.prime_id
          AND block_number = NEW.block_number
          AND block_version = NEW.block_version
          AND synced_at = NEW.synced_at;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_processing_version_allocation_position()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('alp|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s',
            NEW.chain_id, NEW.token_id, NEW.prime_id, NEW.proxy_address,
            NEW.block_number, NEW.block_version,
            NEW.tx_hash, NEW.log_index, NEW.direction, NEW.created_at),
        0));

    SELECT processing_version INTO existing_ver
    FROM allocation_position
    WHERE chain_id = NEW.chain_id
      AND token_id = NEW.token_id
      AND prime_id = NEW.prime_id
      AND proxy_address = NEW.proxy_address
      AND block_number = NEW.block_number
      AND block_version = NEW.block_version
      AND tx_hash = NEW.tx_hash
      AND log_index = NEW.log_index
      AND direction = NEW.direction
      AND created_at = NEW.created_at
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM allocation_position
        WHERE chain_id = NEW.chain_id
          AND token_id = NEW.token_id
          AND prime_id = NEW.prime_id
          AND proxy_address = NEW.proxy_address
          AND block_number = NEW.block_number
          AND block_version = NEW.block_version
          AND tx_hash = NEW.tx_hash
          AND log_index = NEW.log_index
          AND direction = NEW.direction
          AND created_at = NEW.created_at;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_processing_version_protocol_event()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('pev|%s|%s|%s|%s|%s|%s',
            NEW.chain_id, NEW.block_number, NEW.block_version,
            NEW.tx_hash, NEW.log_index, NEW.created_at),
        0));

    SELECT processing_version INTO existing_ver
    FROM protocol_event
    WHERE chain_id = NEW.chain_id
      AND block_number = NEW.block_number
      AND block_version = NEW.block_version
      AND tx_hash = NEW.tx_hash
      AND log_index = NEW.log_index
      AND created_at = NEW.created_at
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM protocol_event
        WHERE chain_id = NEW.chain_id
          AND block_number = NEW.block_number
          AND block_version = NEW.block_version
          AND tx_hash = NEW.tx_hash
          AND log_index = NEW.log_index
          AND created_at = NEW.created_at;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Off-chain / polled tables
-- ============================================================================

CREATE OR REPLACE FUNCTION assign_processing_version_anchorage_package_snapshot()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('aps|%s|%s|%s|%s|%s',
            NEW.prime_id, NEW.package_id, NEW.asset_type,
            NEW.custody_type, NEW.snapshot_time),
        0));

    SELECT processing_version INTO existing_ver
    FROM anchorage_package_snapshot
    WHERE prime_id = NEW.prime_id
      AND package_id = NEW.package_id
      AND asset_type = NEW.asset_type
      AND custody_type = NEW.custody_type
      AND snapshot_time = NEW.snapshot_time
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM anchorage_package_snapshot
        WHERE prime_id = NEW.prime_id
          AND package_id = NEW.package_id
          AND asset_type = NEW.asset_type
          AND custody_type = NEW.custody_type
          AND snapshot_time = NEW.snapshot_time;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_processing_version_anchorage_operation()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('aop|%s|%s', NEW.operation_id, NEW.created_at),
        0));

    SELECT processing_version INTO existing_ver
    FROM anchorage_operation
    WHERE operation_id = NEW.operation_id
      AND created_at = NEW.created_at
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM anchorage_operation
        WHERE operation_id = NEW.operation_id
          AND created_at = NEW.created_at;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_processing_version_offchain_token_price()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('ofp|%s|%s|%s', NEW.token_id, NEW.source_id, NEW.timestamp),
        0));

    SELECT processing_version INTO existing_ver
    FROM offchain_token_price
    WHERE token_id = NEW.token_id
      AND source_id = NEW.source_id
      AND timestamp = NEW.timestamp
      AND build_id = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM offchain_token_price
        WHERE token_id = NEW.token_id
          AND source_id = NEW.source_id
          AND timestamp = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

INSERT INTO migrations (filename)
VALUES ('20260428_120000_add_advisory_locks_to_processing_version_triggers.sql')
ON CONFLICT (filename) DO NOTHING;
