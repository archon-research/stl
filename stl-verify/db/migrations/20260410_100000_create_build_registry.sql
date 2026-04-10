-- Build registry: maps deployed builds (git commits) to integer IDs for
-- space-efficient provenance tracking on state tables.
--
-- See ADR-0002: Data Auditability and Processing Versioning.

CREATE TABLE IF NOT EXISTS build_registry (
    id           SERIAL PRIMARY KEY,
    git_hash     TEXT NOT NULL UNIQUE,
    built_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    docker_sha   TEXT,
    notes        TEXT
);

-- Pre-tracking entry for data produced before provenance tracking was enabled.
-- All existing state-table rows will default to build_id = 0.
INSERT INTO build_registry (id, git_hash, built_at, notes)
VALUES (0, 'pre-tracking', NOW(), 'Data produced before provenance tracking was enabled');

-- SERIAL sequence is unaware of the explicit id=0 insert above.
-- Advance it so the next auto-generated id is 1, not 0.
SELECT setval(pg_get_serial_sequence('build_registry', 'id'), 1, false);

INSERT INTO migrations (filename)
VALUES ('20260410_100000_create_build_registry.sql')
ON CONFLICT (filename) DO NOTHING;
