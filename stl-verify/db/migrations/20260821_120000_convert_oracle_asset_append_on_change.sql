-- VEC-597: oracle_asset becomes append-on-change (ADR-0006 §4).
--
-- Toggling `enabled` used to be an in-place UPDATE (20260423_071108), so the reference view any
-- past calculation read was destroyed the moment a source was retired or re-enabled — the
-- information loss VEC-549 hit. Every mutation is now a new version, following the security_master
-- pattern (VEC-411): natural key + processing_version, `valid_from` as the only stored temporal
-- field, a `_current` view for operational reads, a `_versions` view for history, and an
-- `oracle_asset_as_of(effective_at)` read path for calculation and writer SQL.
--
-- The PK is the natural key + processing_version. feed_address is NULL for aave-style rows and a
-- PK column cannot be NULL, so the key carries `feed_key` — feed_address with NULL folded to an
-- empty bytea — which also collapses the feed / non-feed partial-index split 20260212_120000
-- needed. `id` survives as a per-VERSION surrogate the Go entity still reads; it is not an asset
-- identity, so resolve an asset by its natural key, never by id.
--
-- Cutover (ADR-0006): rows that predate this migration keep a single version whose valid_from is
-- their created_at and whose payload is whatever the last in-place UPDATE left. Their real
-- change history was overwritten before this conversion and is not recoverable; change_reason
-- records that explicitly rather than implying the row was never touched.

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
-- change_reason is mandatory in substance, not just NOT NULL: an empty string is not a reason.
ALTER TABLE oracle_asset DROP CONSTRAINT IF EXISTS oracle_asset_change_reason_chk;
ALTER TABLE oracle_asset ADD CONSTRAINT oracle_asset_change_reason_chk CHECK (btrim(change_reason) <> '');

-- feed_key is the NULL-free form of feed_address, so one key covers both oracle shapes.
ALTER TABLE oracle_asset ADD COLUMN IF NOT EXISTS feed_key bytea
  GENERATED ALWAYS AS (COALESCE(feed_address, '\x'::bytea)) STORED;

-- The pre-conversion unique indexes allowed exactly one row per natural key, which is what made
-- a toggle an UPDATE; the versioned PK replaces both, including the feed / non-feed split.
DROP INDEX IF EXISTS oracle_asset_nonfeed_unique;
DROP INDEX IF EXISTS oracle_asset_feed_unique;
ALTER TABLE oracle_asset DROP CONSTRAINT IF EXISTS oracle_asset_pkey;
ALTER TABLE oracle_asset ADD CONSTRAINT oracle_asset_pkey
  PRIMARY KEY (oracle_id, token_id, feed_key, processing_version);
-- id stops being the key but stays unique: readers carry it, and the sequence is no guarantee
-- against an explicit insert.
CREATE UNIQUE INDEX IF NOT EXISTS oracle_asset_id_key ON oracle_asset (id);
-- Serves the ORDER BY of _current, _as_of and the writer's read-latest lookup.
CREATE INDEX IF NOT EXISTS oracle_asset_version_lookup_idx
  ON oracle_asset (oracle_id, token_id, feed_key, valid_from DESC, processing_version DESC);

COMMENT ON TABLE oracle_asset IS
'[Configuration] Append-on-change map of which tokens an oracle prices (ADR-0006 §4). PK (oracle_id, token_id, feed_key, processing_version); `id` is a per-VERSION surrogate, never an asset identity. Every change — enabled included — is a new row via oracle_asset_set_enabled(); UPDATE/DELETE are revoked. Calculation and writer SQL read oracle_asset_as_of(effective_at) with a recorded effective_at; oracle_asset_current is for operational reads only. A plain table, not a hypertable: governance rows, on the order of a few per month.';
COMMENT ON COLUMN oracle_asset.processing_version IS
'PK. Version of this (oracle_id, token_id, feed_key); monotonic from 0, assigned by oracle_asset_set_enabled under an advisory lock.';
COMMENT ON COLUMN oracle_asset.feed_key IS
'PK. Derived: feed_address with NULL folded to an empty bytea, so the natural key is NULL-free and one key covers feed and non-feed oracles. Never written directly.';
COMMENT ON COLUMN oracle_asset.valid_from IS
'Instant this version became effective; the only temporal field stored (timestamptz, so it cannot shift with a session TimeZone). valid_to is derived in oracle_asset_versions. Reads resolve a version with valid_from <= effective_at.';
COMMENT ON COLUMN oracle_asset.change_reason IS
'Mandatory: why this version exists. Rows predating VEC-597 say so explicitly.';
COMMENT ON COLUMN oracle_asset.id IS
'Audit. BIGSERIAL surrogate, one per VERSION row; unique but not the key. NOT an asset identity and not stable across versions — resolve by (oracle_id, token_id, feed_key).';

-- Effective version per natural key as of an explicit, recorded instant. THE read path for
-- calculation and writer SQL (ADR-0006 §4): a replay passes the recorded effective_at and gets
-- the same rows, which now()/CURRENT_DATE can never guarantee. Returns disabled versions too —
-- callers filter on `enabled`, so "retired then" and "not registered then" stay distinguishable.
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

-- Operational reads only (ADR-0006 §4): bounded on the wall clock, so a future-dated version is
-- not current until its valid_from arrives — which is exactly why it is banned from calculation
-- SQL, where the same query must not change answer with the wall clock.
CREATE OR REPLACE VIEW oracle_asset_current AS
SELECT DISTINCT ON (oracle_id, token_id, feed_key) *
FROM oracle_asset
WHERE valid_from <= now()
ORDER BY oracle_id, token_id, feed_key, valid_from DESC, processing_version DESC;
COMMENT ON VIEW oracle_asset_current IS
'[Configuration] Latest effective oracle_asset version per natural key as of now. OPERATIONAL reads only — calculation and writer SQL must use oracle_asset_as_of(effective_at) (ADR-0006 §4).';

-- Full history with a derived half-open validity window [valid_from, valid_to_exclusive).
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

-- The append-on-change writer: the only sanctioned way to change `enabled`, now that UPDATE is
-- revoked. Appends the next version, carrying the current version's feed columns forward.
-- Returns the new processing_version, or NULL when the value is unchanged — append-on-CHANGE, so
-- a re-assertion writes nothing (ADR-0006 recorded ~880 payload-identical rows from the ADR-0002
-- trigger doing the opposite).
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
    -- The version number AND the change decision are both read before the insert, so ON CONFLICT
    -- cannot guard them: serialize appenders on the natural key (ADR-0002 §3).
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('oracle_asset:%s:%s:%s', p_oracle_id, p_token_id, COALESCE(encode(p_feed_address, 'hex'), '')), 0));

    -- The version this supersedes is the one effective AT p_effective_at, not the newest row:
    -- with a future-dated version already recorded, comparing against that would report "no
    -- change" for a toggle taking effect now and silently write nothing.
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

-- Reads for both roles; append-only writes for the application role, and mutation revoked from
-- the owner too so a future migration cannot quietly rewrite history either. Safe to revoke the
-- owner's UPDATE here only because nothing FKs oracle_asset: no RI probe needs FOR KEY SHARE on
-- it (the trap 20260714_160000 fixed for the reference layer). Guarded on role existence — unit
-- test databases have no roles.
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
