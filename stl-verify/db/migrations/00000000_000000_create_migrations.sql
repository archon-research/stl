-- This migration should be run manually FIRST before any others

CREATE TABLE IF NOT EXISTS migrations (
                                          id SERIAL PRIMARY KEY,
                                          filename TEXT NOT NULL UNIQUE,
                                          applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                                          checksum TEXT -- Optional: hash of file contents for integrity checking
);

CREATE INDEX IF NOT EXISTS idx_migrations_filename ON migrations(filename);
CREATE INDEX IF NOT EXISTS idx_migrations_applied_at ON migrations(applied_at DESC);