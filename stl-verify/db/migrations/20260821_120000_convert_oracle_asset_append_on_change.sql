-- VEC-597: oracle_asset becomes append-on-change (ADR-0006 §4), on the security_master pattern
-- (VEC-411). Toggling `enabled` was an in-place UPDATE (20260423_071108), which destroyed the
-- reference view past calculations had read (VEC-549).
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
-- and oracle_asset_set_enabled would toggle a different logical asset than the caller named.
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
-- Serves the ORDER BY of _current, _as_of and the writer's read-latest lookup.
CREATE INDEX IF NOT EXISTS oracle_asset_version_lookup_idx
  ON oracle_asset (oracle_id, token_id, feed_key, valid_from DESC, processing_version DESC);

COMMENT ON TABLE oracle_asset IS
'[Configuration] Append-on-change map of which tokens an oracle prices (ADR-0006 §4). PK (oracle_id, token_id, feed_key, processing_version); `id` is a per-VERSION surrogate, never an asset identity. UPDATE/DELETE are revoked, so every change is a new row: use oracle_asset_set_enabled() to toggle `enabled`, and an explicit INSERT carrying the next processing_version for a feed_decimals/quote_currency correction (there is no writer function for those). Calculation and writer SQL read oracle_asset_as_of(effective_at) with a recorded effective_at; oracle_asset_current is for operational reads only. A plain table, not a hypertable: governance rows, on the order of a few per month.';
COMMENT ON COLUMN oracle_asset.processing_version IS
'PK. Version of this (oracle_id, token_id, feed_key); monotonic from 0, assigned by oracle_asset_set_enabled under an advisory lock.';
COMMENT ON COLUMN oracle_asset.feed_key IS
'PK. Derived: feed_address with NULL folded to an empty bytea, so the natural key is NULL-free and one key covers feed and non-feed oracles. Never written directly.';
COMMENT ON COLUMN oracle_asset.valid_from IS
'Audit. Instant this version became effective; the only temporal field stored (timestamptz, so it cannot shift with a session TimeZone). valid_to is derived in oracle_asset_versions. Reads resolve a version with valid_from <= effective_at.';
COMMENT ON COLUMN oracle_asset.change_reason IS
'Audit. Mandatory: why this version exists. Rows predating VEC-597 say so explicitly.';
COMMENT ON COLUMN oracle_asset.id IS
'Audit. BIGSERIAL surrogate, one per VERSION row; unique but not the key. NOT an asset identity and not stable across versions — resolve by (oracle_id, token_id, feed_key).';

-- Disabled versions are returned too, so callers filtering on `enabled` can still tell
-- "retired then" from "not registered then".
CREATE OR REPLACE FUNCTION oracle_asset_as_of(p_effective_at timestamptz)
RETURNS SETOF oracle_asset
LANGUAGE sql
STABLE
AS $$
    SELECT DISTINCT ON (oracle_id, token_id, feed_key) *
    FROM oracle_asset
    WHERE valid_from <= p_effective_at
    ORDER BY oracle_id, token_id, feed_key, valid_from DESC, processing_version DESC
$$;
COMMENT ON FUNCTION oracle_asset_as_of(timestamptz) IS
'Effective oracle_asset version per natural key as of p_effective_at (ADR-0006 §4). The calculation/writer read path; pass a recorded effective_at, never now(). Includes disabled versions.';

CREATE OR REPLACE VIEW oracle_asset_current AS
SELECT DISTINCT ON (oracle_id, token_id, feed_key) *
FROM oracle_asset
WHERE valid_from <= now()
ORDER BY oracle_id, token_id, feed_key, valid_from DESC, processing_version DESC;
COMMENT ON VIEW oracle_asset_current IS
'[Configuration] Latest effective oracle_asset version per natural key as of now. OPERATIONAL reads only — calculation and writer SQL must use oracle_asset_as_of(effective_at) (ADR-0006 §4).';

-- Columns are listed explicitly so a later ADD COLUMN cannot shift the trailing computed columns
-- in a `*` expansion and break CREATE OR REPLACE VIEW.
CREATE OR REPLACE VIEW oracle_asset_versions AS
SELECT
    v.id,
    v.oracle_id,
    v.token_id,
    v.enabled,
    v.created_at,
    v.feed_address,
    v.feed_decimals,
    v.quote_currency,
    v.feed_key,
    v.processing_version,
    v.valid_from,
    v.change_reason,
    v.valid_to_exclusive,
    (v.valid_from <= now()
        AND (v.valid_to_exclusive IS NULL OR now() < v.valid_to_exclusive)) AS is_current
FROM (
    SELECT oracle_asset.*,
        lead(valid_from) OVER (
            PARTITION BY oracle_id, token_id, feed_key
            ORDER BY valid_from, processing_version
        ) AS valid_to_exclusive
    FROM oracle_asset
) v;
COMMENT ON VIEW oracle_asset_versions IS
'[Configuration] Full oracle_asset history per natural key with derived valid_to_exclusive (half-open [valid_from, valid_to_exclusive)) and is_current as of now. Audit/history reads.';

CREATE OR REPLACE FUNCTION oracle_asset_set_enabled(
    p_oracle_id     bigint,
    p_token_id      bigint,
    p_feed_address  bytea,
    p_enabled       boolean,
    p_effective_at  timestamptz,
    p_change_reason text
) RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    current_version oracle_asset;
    next_version    integer;
BEGIN
    -- Both the version number and the change decision are read before the insert, so ON CONFLICT
    -- cannot guard them. Serialize appenders on the natural key (ADR-0002 §3).
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('oracle_asset:%s:%s:%s', p_oracle_id, p_token_id, COALESCE(encode(p_feed_address, 'hex'), '')), 0));

    -- The superseded version is the one effective AT p_effective_at, not the newest row: with a
    -- future-dated version recorded, comparing against that reports "no change" for a toggle now.
    SELECT * INTO current_version
    FROM oracle_asset
    WHERE oracle_id = p_oracle_id
      AND token_id = p_token_id
      AND feed_key = COALESCE(p_feed_address, '\x'::bytea)
      AND valid_from <= p_effective_at
    ORDER BY valid_from DESC, processing_version DESC
    LIMIT 1;

    IF NOT FOUND THEN
        IF EXISTS (
            SELECT 1 FROM oracle_asset
            WHERE oracle_id = p_oracle_id
              AND token_id = p_token_id
              AND feed_key = COALESCE(p_feed_address, '\x'::bytea)
        ) THEN
            RAISE EXCEPTION 'effective time % predates the first oracle_asset version for (oracle_id=%, token_id=%, feed_address=%); there is nothing to supersede',
                p_effective_at, p_oracle_id, p_token_id, COALESCE(encode(p_feed_address, 'hex'), 'NULL');
        END IF;
        RAISE EXCEPTION 'oracle_asset (oracle_id=%, token_id=%, feed_address=%) is not registered; register it with an INSERT before toggling it',
            p_oracle_id, p_token_id, COALESCE(encode(p_feed_address, 'hex'), 'NULL');
    END IF;

    IF current_version.enabled = p_enabled THEN
        RETURN NULL;
    END IF;

    -- Monotonic over ALL versions of the key, so a version inserted between two existing ones
    -- still wins its same-valid_from ties and the PK stays collision-free.
    SELECT max(processing_version) + 1 INTO next_version
    FROM oracle_asset
    WHERE oracle_id = p_oracle_id
      AND token_id = p_token_id
      AND feed_key = COALESCE(p_feed_address, '\x'::bytea);

    INSERT INTO oracle_asset (
        oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency,
        processing_version, valid_from, change_reason)
    VALUES (
        current_version.oracle_id, current_version.token_id, p_enabled, current_version.feed_address,
        current_version.feed_decimals, current_version.quote_currency,
        next_version, p_effective_at, p_change_reason);

    RETURN next_version;
END $$;
COMMENT ON FUNCTION oracle_asset_set_enabled(bigint, bigint, bytea, boolean, timestamptz, text) IS
'Appends a new oracle_asset version toggling `enabled` (ADR-0006 §4). Compares against the version effective at p_effective_at. Returns the new processing_version, or NULL if unchanged. Advisory-locked on the natural key; raises on an unregistered key or an effective time before the first version.';

-- Mutation is revoked from the owner too, so a history rewrite costs a visible re-GRANT in a
-- migration (recorded per db/migrations/AGENTS.md). Safe only because nothing FKs oracle_asset, so
-- no RI probe needs FOR KEY SHARE on it (the trap 20260714_160000 fixed). Role existence is
-- guarded because unit-test databases have no roles.
DO $$
DECLARE role_name text;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_readonly') THEN
        GRANT SELECT ON oracle_asset, oracle_asset_current, oracle_asset_versions TO stl_readonly;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_readwrite') THEN
        GRANT SELECT, INSERT ON oracle_asset TO stl_readwrite;
        GRANT SELECT ON oracle_asset_current, oracle_asset_versions TO stl_readwrite;
    END IF;
    FOREACH role_name IN ARRAY ARRAY['stl_readwrite', 'stl_migrator'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON oracle_asset FROM %I', role_name);
        END IF;
    END LOOP;
END $$;

INSERT INTO migrations (filename) VALUES ('20260821_120000_convert_oracle_asset_append_on_change.sql') ON CONFLICT (filename) DO NOTHING;
