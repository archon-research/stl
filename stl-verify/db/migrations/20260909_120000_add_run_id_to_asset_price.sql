SET LOCAL lock_timeout = '10s';

ALTER TABLE asset_price ADD COLUMN IF NOT EXISTS run_id BIGINT;

COMMENT ON COLUMN asset_price.run_id IS
    'Roles: Audit. writer_run.id of the process start that wrote this row (ADR-0006 §2): resolves to the artefact (build_registry) and the reference snapshot/effective instant the writer ran with. NULL = written before run tracking. Not an FK, like build_id; never used for ordering or to pick the latest row.';

INSERT INTO migrations (filename) VALUES ('20260909_120000_add_run_id_to_asset_price.sql') ON CONFLICT (filename) DO NOTHING;
