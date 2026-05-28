-- Add pg_advisory_xact_lock to the maple_vault_state and maple_vault_position
-- processing-version trigger functions, completing the contract documented in
-- ADR-0002 §3 (Concurrency) and migration
-- 20260428_120000_add_advisory_locks_to_processing_version_triggers.sql.
--
-- The original maple-table migration (20260527_100000_create_maple_tables.sql)
-- created these two triggers without the advisory lock, exposing the same
-- silent-row-drop race that VEC-194 fixed for every prior assign_processing_version_*
-- function: two concurrent inserts at the same natural key but different
-- build_ids both read MAX(processing_version), both compute MAX+1, the first
-- wins, the second collides on the unique index, and the caller's
-- ON CONFLICT DO NOTHING silently discards a row that was meant to be a new
-- version. See ADR-0002 and the VEC-194 PR for the full incident write-up.
--
-- Lock-key prefixes mirror ADR-0002 conventions:
--   syv = SYrup Vault state   (maple_vault_state)
--   syp = SYrup vault Position (maple_vault_position)
-- mvs/mvp were already taken by the morpho_vault_state / morpho_vault_position
-- triggers; syv/syp are unique across all assign_processing_version_* functions.
--
-- The timestamp argument is wrapped in EXTRACT(epoch FROM …) so the lock key
-- does not depend on session TimeZone or DateStyle settings.

CREATE OR REPLACE FUNCTION assign_processing_version_maple_vault_state()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('syv|%s|%s|%s|%s',
            NEW.maple_vault_id, NEW.block_number, NEW.block_version, EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM maple_vault_state
    WHERE maple_vault_id = NEW.maple_vault_id
      AND block_number   = NEW.block_number
      AND block_version  = NEW.block_version
      AND timestamp      = NEW.timestamp
      AND build_id       = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM maple_vault_state
        WHERE maple_vault_id = NEW.maple_vault_id
          AND block_number   = NEW.block_number
          AND block_version  = NEW.block_version
          AND timestamp      = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION assign_processing_version_maple_vault_position()
RETURNS TRIGGER AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('syp|%s|%s|%s|%s|%s',
            NEW.user_id, NEW.maple_vault_id,
            NEW.block_number, NEW.block_version, EXTRACT(epoch FROM NEW.timestamp)),
        0));

    SELECT processing_version INTO existing_ver
    FROM maple_vault_position
    WHERE user_id        = NEW.user_id
      AND maple_vault_id = NEW.maple_vault_id
      AND block_number   = NEW.block_number
      AND block_version  = NEW.block_version
      AND timestamp      = NEW.timestamp
      AND build_id       = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM maple_vault_position
        WHERE user_id        = NEW.user_id
          AND maple_vault_id = NEW.maple_vault_id
          AND block_number   = NEW.block_number
          AND block_version  = NEW.block_version
          AND timestamp      = NEW.timestamp;
        NEW.processing_version := max_ver + 1;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

INSERT INTO migrations (filename)
VALUES ('20260528_100000_add_advisory_locks_to_maple_triggers.sql')
ON CONFLICT (filename) DO NOTHING;
