-- VEC-401: the position stack's rows name the writer run that wrote them (ADR-0006 §2), as every
-- governed table already does. Carried wherever build_id is: the observations, the run records and
-- the refusals. Nullable with no default, so this is a catalogue-only change on the hypertable, and
-- no FK, matching build_id: an FK probe per appended batch buys no reproducibility.
SET LOCAL lock_timeout = '10s';

ALTER TABLE position_state ADD COLUMN IF NOT EXISTS run_id bigint;
ALTER TABLE position_projection_run ADD COLUMN IF NOT EXISTS run_id bigint;
ALTER TABLE position_projection_refusal ADD COLUMN IF NOT EXISTS run_id bigint;

COMMENT ON COLUMN position_state.run_id IS 'Roles: Audit. writer_run.id of the materializer run that appended the observation (ADR-0006 §2); NULL means it predates run tracking. Resolves the row to its code artefact and to the reference data the run read. Metadata, not identity: never used to pick a row.';
COMMENT ON COLUMN position_projection_run.run_id IS 'Roles: Audit. writer_run.id of the run this record describes (ADR-0006 §2); NULL means it predates run tracking.';
COMMENT ON COLUMN position_projection_refusal.run_id IS 'Roles: Audit. writer_run.id of the run that first recorded the refusal (ADR-0006 §2); NULL means it predates run tracking.';

INSERT INTO migrations (filename) VALUES ('20260909_155000_add_run_id_to_position_stack.sql') ON CONFLICT (filename) DO NOTHING;
