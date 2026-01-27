CREATE TABLE IF NOT EXISTS migrations
(
    id         SERIAL PRIMARY KEY,
    filename   TEXT        NOT NULL UNIQUE,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checksum   TEXT
);

CREATE INDEX IF NOT EXISTS idx_migrations_filename ON migrations (filename);
CREATE INDEX IF NOT EXISTS idx_migrations_applied_at ON migrations (applied_at DESC);

INSERT INTO migrations (filename)
VALUES ('20260122_140000_create_migrations.sql')
ON CONFLICT (filename) DO NOTHING;