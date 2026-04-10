-- Per-table trigger functions for automatic processing_version assignment.
--
-- Each trigger is build-aware:
--   - If a row with the same natural key AND same build_id exists → retry,
--     reuse existing processing_version (ON CONFLICT DO NOTHING deduplicates).
--   - If build_id differs or no row exists → assign MAX(processing_version) + 1.
--
-- Advisory locking is the caller's responsibility (repository layer), not the
-- trigger's. The caller must use READ COMMITTED + pg_advisory_xact_lock before
-- inserting, matching the pattern in blockstate_repository.go.
--
-- See ADR-0002: Data Auditability and Processing Versioning.

-- ============================================================================
-- Blockchain-derived tables
-- ============================================================================

-- borrower
CREATE OR REPLACE FUNCTION assign_processing_version_borrower()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
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

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON borrower
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_borrower();

-- borrower_collateral
CREATE OR REPLACE FUNCTION assign_processing_version_borrower_collateral()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
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

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON borrower_collateral
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_borrower_collateral();

-- sparklend_reserve_data (matches UNIQUE constraint columns, not surrogate PK)
CREATE OR REPLACE FUNCTION assign_processing_version_sparklend_reserve_data()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
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

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON sparklend_reserve_data
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_sparklend_reserve_data();

-- onchain_token_price
CREATE OR REPLACE FUNCTION assign_processing_version_onchain_token_price()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
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

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON onchain_token_price
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_onchain_token_price();

-- morpho_market_state
CREATE OR REPLACE FUNCTION assign_processing_version_morpho_market_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
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

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON morpho_market_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_morpho_market_state();

-- morpho_market_position
CREATE OR REPLACE FUNCTION assign_processing_version_morpho_market_position()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
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

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON morpho_market_position
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_morpho_market_position();

-- morpho_vault_state
CREATE OR REPLACE FUNCTION assign_processing_version_morpho_vault_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
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

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON morpho_vault_state
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_morpho_vault_state();

-- morpho_vault_position
CREATE OR REPLACE FUNCTION assign_processing_version_morpho_vault_position()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
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

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON morpho_vault_position
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_morpho_vault_position();

-- prime_debt (matches UNIQUE constraint columns)
CREATE OR REPLACE FUNCTION assign_processing_version_prime_debt()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
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

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON prime_debt
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_prime_debt();

-- allocation_position
CREATE OR REPLACE FUNCTION assign_processing_version_allocation_position()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
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

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON allocation_position
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_allocation_position();

-- protocol_event
CREATE OR REPLACE FUNCTION assign_processing_version_protocol_event()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
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

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON protocol_event
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_protocol_event();

-- ============================================================================
-- Off-chain / polled tables
-- ============================================================================

-- anchorage_package_snapshot
CREATE OR REPLACE FUNCTION assign_processing_version_anchorage_package_snapshot()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
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

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON anchorage_package_snapshot
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_anchorage_package_snapshot();

-- anchorage_operation
CREATE OR REPLACE FUNCTION assign_processing_version_anchorage_operation()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
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

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON anchorage_operation
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_anchorage_operation();

-- offchain_token_price
CREATE OR REPLACE FUNCTION assign_processing_version_offchain_token_price()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver INT;
BEGIN
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

CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON offchain_token_price
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_offchain_token_price();

INSERT INTO migrations (filename)
VALUES ('20260410_150000_create_processing_version_triggers.sql')
ON CONFLICT (filename) DO NOTHING;
