-- VEC-598: row provenance names the exact artefact and process run that wrote it (ADR-0006 §2).
--
-- build_registry keyed by git_hash alone names a commit, not an artefact: one commit builds one
-- image per service, and the same service rebuilt from the same commit gets a new digest. The
-- key becomes (git_hash, service, image_digest). Rows registered before this migration carry
-- the sentinel 'unknown' in the two new columns; every git_hash was unique, so the triple stays
-- unique. Pre-tracking on a data row is signalled by run_id IS NULL, never by that sentinel.
--
-- writer_run records one process start of a writer: which artefact, when, and the reference
-- data it ran with — the MVCC snapshot taken in the transaction that loaded it, and the
-- effective instant it resolved reference versions at (REFERENCE_EFFECTIVE_AT, VEC-597).
--
-- Both are provenance tables: insert-only, or a recipe silently resolves to a different
-- artefact. stl_readwrite keeps SELECT and INSERT only. The owner keeps UPDATE because the FK
-- integrity probe on a child INSERT (writer_run -> build_registry, and later
-- processing_version_log -> writer_run) runs as the parent's owner and needs it (the trap
-- 20260714_160000 fixed); a statement-level trigger raises on any real UPDATE/DELETE/TRUNCATE
-- instead. Role existence is guarded because unit-test databases have no roles.

SET LOCAL lock_timeout = '10s';

ALTER TABLE build_registry ADD COLUMN IF NOT EXISTS service TEXT;
ALTER TABLE build_registry ADD COLUMN IF NOT EXISTS image_digest TEXT;

UPDATE build_registry
SET service = COALESCE(service, 'unknown'),
    image_digest = COALESCE(image_digest, 'unknown')
WHERE service IS NULL OR image_digest IS NULL;

ALTER TABLE build_registry ALTER COLUMN service SET NOT NULL;
ALTER TABLE build_registry ALTER COLUMN image_digest SET NOT NULL;

ALTER TABLE build_registry DROP CONSTRAINT IF EXISTS build_registry_service_chk;
ALTER TABLE build_registry ADD CONSTRAINT build_registry_service_chk CHECK (btrim(service) <> '');
ALTER TABLE build_registry DROP CONSTRAINT IF EXISTS build_registry_image_digest_chk;
ALTER TABLE build_registry ADD CONSTRAINT build_registry_image_digest_chk CHECK (btrim(image_digest) <> '');

ALTER TABLE build_registry DROP CONSTRAINT IF EXISTS build_registry_git_hash_key;
ALTER TABLE build_registry DROP CONSTRAINT IF EXISTS build_registry_artefact_key;
ALTER TABLE build_registry ADD CONSTRAINT build_registry_artefact_key UNIQUE (git_hash, service, image_digest);

CREATE TABLE IF NOT EXISTS writer_run (
    id                     BIGSERIAL PRIMARY KEY,
    build_id               INT         NOT NULL REFERENCES build_registry(id),
    started_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    reference_snapshot     TEXT        NOT NULL,
    reference_effective_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS writer_run_build_id_idx ON writer_run (build_id);

COMMENT ON TABLE build_registry IS
'[Operational] Provenance: the deploy artefacts that have written governed rows. One row per (git_hash, service, image_digest); a process registers its own at startup (buildregistry.New) and every governed row reaches it through writer_run.run_id -> build_id (or, for pre-VEC-598 rows, build_id directly). Insert-only: UPDATE/DELETE/TRUNCATE raise, because a mutable artefact row would silently change what a recipe resolves to (ADR-0006 §1, §2).';
COMMENT ON COLUMN build_registry.git_hash IS
'Audit. Commit the binary was built from. Not unique on its own since VEC-598: one commit yields one image per service, and a rebuild yields a new digest.';
COMMENT ON COLUMN build_registry.service IS
'Audit. Binary name of the writer (os.Args[0] basename), e.g. sparklend-indexer. ''unknown'' on rows registered before VEC-598.';
COMMENT ON COLUMN build_registry.image_digest IS
'Audit. Digest of the container image that ran (sha256:<64 hex>), the artefact retained indefinitely for bit-for-bit reproduction. ''unknown'' on rows registered before VEC-598; ''dev'' for a local or test process (STL_DEV_IDENTITY=1), which is never a deployed environment.';

COMMENT ON TABLE writer_run IS
'[Operational] Provenance: one row per process start of a governed-row writer (ADR-0006 §2). Governed rows carry run_id -> writer_run.id (NULL = written before tracking). Pins the writer''s reference data on both temporal axes: reference_snapshot (which reference rows existed) and reference_effective_at (which of them applied). A process that reloads its reference data opens a new run. Insert-only: UPDATE/DELETE/TRUNCATE raise. A plain table: one row per process start.';
COMMENT ON COLUMN writer_run.id IS
'PK. BIGSERIAL; the run_id governed rows carry.';
COMMENT ON COLUMN writer_run.build_id IS
'FK→build_registry.id. The artefact this process ran as.';
COMMENT ON COLUMN writer_run.started_at IS
'Audit. Wall-clock instant the run was opened (timestamptz, UTC). A human label, not the ordering key.';
COMMENT ON COLUMN writer_run.reference_snapshot IS
'Audit. pg_current_snapshot()::text of the REPEATABLE READ transaction that loaded the writer''s reference data, taken in that same transaction. Reference tables are append-only, so the rows visible in this snapshot are exactly the rows the writer saw; resolve with pg_visible_in_snapshot(<row xid>, reference_snapshot::pg_snapshot). Cluster-local: meaningful only against this database and its physical replicas.';
COMMENT ON COLUMN writer_run.reference_effective_at IS
'Audit. The effective instant the writer resolved reference versions at: valid_from <= reference_effective_at (REFERENCE_EFFECTIVE_AT, or process start). Together with reference_snapshot it makes "reference data as of the run" exact.';

CREATE OR REPLACE FUNCTION provenance_table_immutable() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'provenance table %.% is insert-only; % is not allowed (ADR-0006 §2)',
        TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP;
END $$;
COMMENT ON FUNCTION provenance_table_immutable() IS
'Raises on UPDATE/DELETE/TRUNCATE of a provenance table (build_registry, writer_run). Statement-level, so it never fires in normal operation; row locks from the FK integrity probe do not fire it, which is why the owner keeps UPDATE (see 20260714_160000).';

DROP TRIGGER IF EXISTS build_registry_immutable ON build_registry;
CREATE TRIGGER build_registry_immutable
    BEFORE UPDATE OR DELETE OR TRUNCATE ON build_registry
    FOR EACH STATEMENT EXECUTE FUNCTION provenance_table_immutable();
DROP TRIGGER IF EXISTS writer_run_immutable ON writer_run;
CREATE TRIGGER writer_run_immutable
    BEFORE UPDATE OR DELETE OR TRUNCATE ON writer_run
    FOR EACH STATEMENT EXECUTE FUNCTION provenance_table_immutable();

DO $$
DECLARE tbl text;
BEGIN
    FOREACH tbl IN ARRAY ARRAY['build_registry', 'writer_run'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_readonly') THEN
            EXECUTE format('GRANT SELECT ON %I TO stl_readonly', tbl);
        END IF;
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_readwrite') THEN
            EXECUTE format('GRANT SELECT, INSERT ON %I TO stl_readwrite', tbl);
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON %I FROM stl_readwrite', tbl);
        END IF;
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_migrator') THEN
            EXECUTE format('REVOKE DELETE, TRUNCATE ON %I FROM stl_migrator', tbl);
        END IF;
    END LOOP;
END $$;

INSERT INTO migrations (filename) VALUES ('20260902_120000_widen_build_registry_add_writer_run.sql') ON CONFLICT (filename) DO NOTHING;
