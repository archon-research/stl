-- Convert borrower and borrower_collateral to TimescaleDB hypertables with columnstore.
--
-- REQUIRES:
--   - Tiered storage enabled in Tiger Console (for add_tiering_policy)
--
-- Workers do NOT need to be stopped. create_hypertable takes an ACCESS EXCLUSIVE lock;
-- concurrent INSERTs queue on the lock, then fail transiently once the schema changes.
-- SQS retry handles recovery after the new Go code is deployed.
--
-- created_at is set to the block timestamp by application code (deterministic for dedup).
-- DEFAULT NOW() is a safety net only; all INSERT paths set created_at explicitly.

-- ============================================================================
-- borrower
-- ============================================================================

-- 1. Drop PK, unique constraint, and synthetic id column.
--    Uses dynamic lookup to avoid hardcoded constraint names.
DO $$
DECLARE
    v_conname text;
BEGIN
    -- Drop unique constraint
    SELECT conname INTO v_conname FROM pg_constraint
    WHERE conrelid = 'borrower'::regclass AND contype = 'u';
    IF v_conname IS NOT NULL THEN
        EXECUTE format('ALTER TABLE borrower DROP CONSTRAINT %I', v_conname);
    END IF;

    -- Drop PK
    SELECT conname INTO v_conname FROM pg_constraint
    WHERE conrelid = 'borrower'::regclass AND contype = 'p';
    IF v_conname IS NOT NULL THEN
        EXECUTE format('ALTER TABLE borrower DROP CONSTRAINT %I', v_conname);
    END IF;
END $$;

ALTER TABLE borrower DROP COLUMN id;

-- 2. New PK includes created_at for hypertable compatibility
ALTER TABLE borrower ADD PRIMARY KEY
    (user_id, protocol_id, token_id, block_number, block_version, created_at);

-- 3. Lock referenced tables to prevent deadlock during migrate_data
--    (TimescaleDB docs: FK constraints can cause deadlock during hypertable conversion)
LOCK TABLE "user", protocol, token IN SHARE ROW EXCLUSIVE MODE;

-- 4. Convert to hypertable (migrates existing data into chunks)
SELECT create_hypertable('borrower', by_range('created_at', INTERVAL '1 day'),
    migrate_data => true);

-- 5. Columnstore
ALTER TABLE borrower SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'protocol_id, token_id',
    timescaledb.orderby = 'block_number DESC, block_version DESC'
);

CALL add_columnstore_policy('borrower', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('borrower', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for borrower';
END $$;

-- ============================================================================
-- borrower_collateral
-- ============================================================================

DO $$
DECLARE
    v_conname text;
BEGIN
    SELECT conname INTO v_conname FROM pg_constraint
    WHERE conrelid = 'borrower_collateral'::regclass AND contype = 'u';
    IF v_conname IS NOT NULL THEN
        EXECUTE format('ALTER TABLE borrower_collateral DROP CONSTRAINT %I', v_conname);
    END IF;

    SELECT conname INTO v_conname FROM pg_constraint
    WHERE conrelid = 'borrower_collateral'::regclass AND contype = 'p';
    IF v_conname IS NOT NULL THEN
        EXECUTE format('ALTER TABLE borrower_collateral DROP CONSTRAINT %I', v_conname);
    END IF;
END $$;

ALTER TABLE borrower_collateral DROP COLUMN id;

ALTER TABLE borrower_collateral ADD PRIMARY KEY
    (user_id, protocol_id, token_id, block_number, block_version, created_at);

LOCK TABLE "user", protocol, token IN SHARE ROW EXCLUSIVE MODE;

SELECT create_hypertable('borrower_collateral', by_range('created_at', INTERVAL '1 day'),
    migrate_data => true);

ALTER TABLE borrower_collateral SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'protocol_id, token_id',
    timescaledb.orderby = 'block_number DESC, block_version DESC'
);

CALL add_columnstore_policy('borrower_collateral', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('borrower_collateral', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for borrower_collateral';
END $$;

INSERT INTO migrations (filename)
VALUES ('20260409_120000_convert_borrower_tables_to_hypertables.sql')
ON CONFLICT (filename) DO NOTHING;
