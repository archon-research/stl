-- Convert protocol_event and allocation_position to TimescaleDB hypertables with columnstore.
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
-- protocol_event
-- ============================================================================

DO $$
DECLARE
    v_conname text;
BEGIN
    SELECT conname INTO v_conname FROM pg_constraint
    WHERE conrelid = 'protocol_event'::regclass AND contype = 'u';
    IF v_conname IS NOT NULL THEN
        EXECUTE format('ALTER TABLE protocol_event DROP CONSTRAINT %I', v_conname);
    END IF;

    SELECT conname INTO v_conname FROM pg_constraint
    WHERE conrelid = 'protocol_event'::regclass AND contype = 'p';
    IF v_conname IS NOT NULL THEN
        EXECUTE format('ALTER TABLE protocol_event DROP CONSTRAINT %I', v_conname);
    END IF;
END $$;

ALTER TABLE protocol_event DROP COLUMN id;

ALTER TABLE protocol_event ADD PRIMARY KEY
    (chain_id, block_number, block_version, tx_hash, log_index, created_at);

LOCK TABLE chain, protocol IN SHARE ROW EXCLUSIVE MODE;

SELECT create_hypertable('protocol_event', by_range('created_at', INTERVAL '1 day'),
    migrate_data => true);

ALTER TABLE protocol_event SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'chain_id, protocol_id',
    timescaledb.orderby = 'block_number DESC, block_version DESC'
);

SELECT add_columnstore_policy('protocol_event', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('protocol_event', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for protocol_event';
END $$;

-- ============================================================================
-- allocation_position
-- ============================================================================

DO $$
DECLARE
    v_conname text;
BEGIN
    SELECT conname INTO v_conname FROM pg_constraint
    WHERE conrelid = 'allocation_position'::regclass AND contype = 'u';
    IF v_conname IS NOT NULL THEN
        EXECUTE format('ALTER TABLE allocation_position DROP CONSTRAINT %I', v_conname);
    END IF;

    SELECT conname INTO v_conname FROM pg_constraint
    WHERE conrelid = 'allocation_position'::regclass AND contype = 'p';
    IF v_conname IS NOT NULL THEN
        EXECUTE format('ALTER TABLE allocation_position DROP CONSTRAINT %I', v_conname);
    END IF;
END $$;

ALTER TABLE allocation_position DROP COLUMN id;

ALTER TABLE allocation_position ADD PRIMARY KEY
    (chain_id, token_id, prime_id, proxy_address, block_number, block_version,
     tx_hash, log_index, direction, created_at);

LOCK TABLE chain, token, prime IN SHARE ROW EXCLUSIVE MODE;

SELECT create_hypertable('allocation_position', by_range('created_at', INTERVAL '1 day'),
    migrate_data => true);

ALTER TABLE allocation_position SET (
    timescaledb.enable_columnstore,
    timescaledb.segmentby = 'chain_id, prime_id',
    timescaledb.orderby = 'block_number DESC, block_version DESC'
);

SELECT add_columnstore_policy('allocation_position', INTERVAL '2 days', if_not_exists => TRUE);

DO $$ BEGIN
    PERFORM add_tiering_policy('allocation_position', INTERVAL '1 year', if_not_exists => TRUE);
EXCEPTION WHEN undefined_function THEN
    RAISE NOTICE 'add_tiering_policy not available, skipping tiering for allocation_position';
END $$;

INSERT INTO migrations (filename)
VALUES ('20260409_130000_convert_event_tables_to_hypertables.sql')
ON CONFLICT (filename) DO NOTHING;
