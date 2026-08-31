-- VEC-597: oracle_asset becomes append-on-change. Toggling `enabled` was an in-place UPDATE
-- (20260423_071108), which destroyed the reference view past calculations had read (VEC-549).
--
-- Storage follows ADR-0006 §4; the read side deliberately does not. §4 also calls for a
-- <table>_current view, a <table>_versions view and a <table>_as_of() function, and this
-- migration ships none of them: logic belongs in the application unless a trigger needs it
-- (PR #822 review). The pinned read is a SQL fragment each caller interpolates instead.
-- Reconciling §4 with that rule is VEC-687.
--
-- The PK is the natural key + processing_version. feed_address is NULL for aave-style rows and a
-- PK column cannot be NULL, so the key carries `feed_key`: feed_address with NULL folded to an
-- empty bytea, which also collapses the feed / non-feed partial-index split 20260212_120000
-- needed. `id` survives as a per-VERSION surrogate, not an asset identity.
--
-- Rows predating this migration keep one version whose valid_from is their created_at. Their real
-- change history was overwritten before the conversion and is not recoverable.

ALTER TABLE oracle_asset ADD COLUMN IF NOT EXISTS processing_version integer NOT NULL DEFAULT 0;
ALTER TABLE oracle_asset ADD COLUMN IF NOT EXISTS valid_from timestamptz;
ALTER TABLE oracle_asset ADD COLUMN IF NOT EXISTS change_reason text;

UPDATE oracle_asset
SET valid_from = COALESCE(valid_from, created_at),
    change_reason = COALESCE(change_reason, 'pre-VEC-597 row; earlier changes were applied in place and are not recoverable')
WHERE valid_from IS NULL OR change_reason IS NULL;

ALTER TABLE oracle_asset ALTER COLUMN valid_from SET DEFAULT now();
ALTER TABLE oracle_asset ALTER COLUMN valid_from SET NOT NULL;
ALTER TABLE oracle_asset ALTER COLUMN change_reason SET NOT NULL;

ALTER TABLE oracle_asset DROP CONSTRAINT IF EXISTS oracle_asset_processing_version_chk;
ALTER TABLE oracle_asset ADD CONSTRAINT oracle_asset_processing_version_chk CHECK (processing_version >= 0);
-- An empty string is not a reason.
ALTER TABLE oracle_asset DROP CONSTRAINT IF EXISTS oracle_asset_change_reason_chk;
ALTER TABLE oracle_asset ADD CONSTRAINT oracle_asset_change_reason_chk CHECK (btrim(change_reason) <> '');

-- The NULL-to-'\x' fold below is only injective while an empty feed_address is unrepresentable:
-- '\x' would otherwise share a feed_key with the NULL-feed row for the same (oracle_id, token_id),
-- so an appended version would land on a different logical asset than the writer named.
ALTER TABLE oracle_asset DROP CONSTRAINT IF EXISTS oracle_asset_feed_address_len_chk;
ALTER TABLE oracle_asset ADD CONSTRAINT oracle_asset_feed_address_len_chk
  CHECK (feed_address IS NULL OR octet_length(feed_address) = 20);

ALTER TABLE oracle_asset ADD COLUMN IF NOT EXISTS feed_key bytea
  GENERATED ALWAYS AS (COALESCE(feed_address, '\x'::bytea)) STORED;

-- The versioned PK replaces both pre-conversion unique indexes, including the feed/non-feed split.
DROP INDEX IF EXISTS oracle_asset_nonfeed_unique;
DROP INDEX IF EXISTS oracle_asset_feed_unique;
ALTER TABLE oracle_asset DROP CONSTRAINT IF EXISTS oracle_asset_pkey;
ALTER TABLE oracle_asset ADD CONSTRAINT oracle_asset_pkey
  PRIMARY KEY (oracle_id, token_id, feed_key, processing_version);
-- No longer the key, but readers carry it and the sequence alone does not stop an explicit insert.
CREATE UNIQUE INDEX IF NOT EXISTS oracle_asset_id_key ON oracle_asset (id);
-- Serves the ORDER BY of the pinned read and of a writer's read-latest lookup.
CREATE INDEX IF NOT EXISTS oracle_asset_version_lookup_idx
  ON oracle_asset (oracle_id, token_id, feed_key, valid_from DESC, processing_version DESC);

COMMENT ON TABLE oracle_asset IS
'[Configuration] Append-on-change map of which tokens an oracle prices. PK (oracle_id, token_id, feed_key, processing_version); `id` is a per-VERSION surrogate, never an asset identity. UPDATE/DELETE are revoked, so every change — a retirement, a re-enable, a feed_decimals or quote_currency correction — is an INSERT carrying the next processing_version, valid_from and change_reason. A reader resolves the version effective at a recorded instant: valid_from <= effective_at, latest per (oracle_id, token_id, feed_key). Reading the raw table unqualified matches a retired version as readily as the live one. A plain table, not a hypertable: governance rows, on the order of a few per month.';
COMMENT ON COLUMN oracle_asset.processing_version IS
'PK. Version of this (oracle_id, token_id, feed_key); monotonic from 0. An appending writer reads the current maximum and adds one, so concurrent appenders on one key must serialize on it (pg_advisory_xact_lock, ADR-0002 §3).';
COMMENT ON COLUMN oracle_asset.feed_key IS
'PK. Derived: feed_address with NULL folded to an empty bytea, so the natural key is NULL-free and one key covers feed and non-feed oracles. Never written directly.';
COMMENT ON COLUMN oracle_asset.valid_from IS
'Audit. Instant this version became effective; the only temporal field stored (timestamptz, so it cannot shift with a session TimeZone). There is no valid_to: a version runs until the next valid_from for the same natural key. Reads resolve a version with valid_from <= effective_at.';
COMMENT ON COLUMN oracle_asset.change_reason IS
'Audit. Mandatory: why this version exists. Rows predating VEC-597 say so explicitly.';
COMMENT ON COLUMN oracle_asset.id IS
'Audit. BIGSERIAL surrogate, one per VERSION row; unique but not the key. NOT an asset identity and not stable across versions — resolve by (oracle_id, token_id, feed_key).';

-- Mutation is revoked from the owner too, so a history rewrite costs a visible re-GRANT in a
-- migration (recorded per db/migrations/AGENTS.md). Safe only because nothing FKs oracle_asset, so
-- no RI probe needs FOR KEY SHARE on it (the trap 20260714_160000 fixed). Role existence is
-- guarded because unit-test databases have no roles.
DO $$
DECLARE role_name text;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_readonly') THEN
        GRANT SELECT ON oracle_asset TO stl_readonly;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_readwrite') THEN
        GRANT SELECT, INSERT ON oracle_asset TO stl_readwrite;
    END IF;
    FOREACH role_name IN ARRAY ARRAY['stl_readwrite', 'stl_migrator'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON oracle_asset FROM %I', role_name);
        END IF;
    END LOOP;
END $$;

INSERT INTO migrations (filename) VALUES ('20260821_120000_convert_oracle_asset_append_on_change.sql') ON CONFLICT (filename) DO NOTHING;
