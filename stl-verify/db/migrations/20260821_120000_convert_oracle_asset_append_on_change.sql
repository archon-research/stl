-- VEC-597: oracle_asset becomes append-on-change (ADR-0006 §4).
--
-- Toggling `enabled` used to be an in-place UPDATE (20260423_071108), so the reference view any
-- past calculation read was destroyed the moment a source was retired or re-enabled — the
-- information loss VEC-549 hit. Every mutation is now a new version, following the security_master
-- pattern (VEC-411): natural key + processing_version, `valid_from` as the only stored temporal
-- field, a `_current` view for operational reads, a `_versions` view for history, and an
-- `oracle_asset_as_of(effective_at)` read path for calculation and writer SQL.
--
-- Deviation from security_master, deliberate: `id` stays the primary key. It is a pre-existing
-- BIGSERIAL surrogate that the Go entity and every reader carry, and the natural key
-- (oracle_id, token_id, feed_address) cannot be a primary key because feed_address is NULL for
-- aave-style rows. The natural key + processing_version is enforced by the two partial unique
-- indexes below instead — the same split 20260212_120000 introduced, now version-aware. One `id`
-- per VERSION, not per asset: resolve an asset by its natural key, never by id.
--
-- Cutover (ADR-0006): rows that predate this migration keep a single version whose valid_from is
-- their created_at date and whose payload is whatever the last in-place UPDATE left. Their real
-- change history was overwritten before this conversion and is not recoverable; change_reason
-- records that explicitly rather than implying the row was never touched.

ALTER TABLE oracle_asset ADD COLUMN IF NOT EXISTS processing_version integer NOT NULL DEFAULT 0;
ALTER TABLE oracle_asset ADD COLUMN IF NOT EXISTS valid_from date;
ALTER TABLE oracle_asset ADD COLUMN IF NOT EXISTS change_reason text;

UPDATE oracle_asset
SET valid_from = COALESCE(valid_from, (created_at AT TIME ZONE 'utc')::date),
    change_reason = COALESCE(change_reason, 'pre-VEC-597 row; earlier changes were applied in place and are not recoverable')
WHERE valid_from IS NULL OR change_reason IS NULL;

-- UTC so which version is current does not shift with the writer's session TimeZone.
ALTER TABLE oracle_asset ALTER COLUMN valid_from SET DEFAULT ((now() AT TIME ZONE 'utc')::date);
ALTER TABLE oracle_asset ALTER COLUMN valid_from SET NOT NULL;
ALTER TABLE oracle_asset ALTER COLUMN change_reason SET NOT NULL;

ALTER TABLE oracle_asset DROP CONSTRAINT IF EXISTS oracle_asset_processing_version_chk;
ALTER TABLE oracle_asset ADD CONSTRAINT oracle_asset_processing_version_chk CHECK (processing_version >= 0);
-- change_reason is mandatory in substance, not just NOT NULL: an empty string is not a reason.
ALTER TABLE oracle_asset DROP CONSTRAINT IF EXISTS oracle_asset_change_reason_chk;
ALTER TABLE oracle_asset ADD CONSTRAINT oracle_asset_change_reason_chk CHECK (btrim(change_reason) <> '');

-- The pre-conversion unique indexes allowed exactly one row per natural key, which is what made
-- a toggle an UPDATE. Version-aware replacements keep the same feed / non-feed split.
DROP INDEX IF EXISTS oracle_asset_nonfeed_unique;
DROP INDEX IF EXISTS oracle_asset_feed_unique;
CREATE UNIQUE INDEX IF NOT EXISTS oracle_asset_nonfeed_version_unique
  ON oracle_asset (oracle_id, token_id, processing_version) WHERE feed_address IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS oracle_asset_feed_version_unique
  ON oracle_asset (oracle_id, token_id, feed_address, processing_version) WHERE feed_address IS NOT NULL;
-- Serves the ORDER BY of _current, _as_of and the writer's read-latest lookup.
CREATE INDEX IF NOT EXISTS oracle_asset_version_lookup_idx
  ON oracle_asset (oracle_id, token_id, feed_address, valid_from DESC, processing_version DESC);

COMMENT ON TABLE oracle_asset IS
'[Configuration] Append-on-change map of which tokens an oracle prices (ADR-0006 §4). Natural key (oracle_id, token_id, feed_address) + processing_version, enforced by the two partial unique indexes; `id` is a per-VERSION surrogate, never an asset identity. Every change — enabled included — is a new row via oracle_asset_set_enabled(); UPDATE/DELETE are revoked. Calculation and writer SQL read oracle_asset_as_of(effective_at) with a recorded effective_at; oracle_asset_current is for operational reads only. A plain table, not a hypertable: governance rows, on the order of a few per month.';
COMMENT ON COLUMN oracle_asset.processing_version IS
'PK component of the natural key. Version of this (oracle_id, token_id, feed_address); monotonic from 0, assigned by oracle_asset_set_enabled under an advisory lock.';
COMMENT ON COLUMN oracle_asset.valid_from IS
'Date this version became effective (UTC); the only temporal field stored. valid_to is derived in oracle_asset_versions. Reads resolve a version with valid_from <= effective_at.';
COMMENT ON COLUMN oracle_asset.change_reason IS
'Mandatory: why this version exists. Rows predating VEC-597 say so explicitly.';
COMMENT ON COLUMN oracle_asset.id IS
'Audit. BIGSERIAL surrogate, one per VERSION row. NOT an asset identity and not stable across versions — resolve by (oracle_id, token_id, feed_address).';

-- Effective version per natural key as of an explicit, recorded date. THE read path for
-- calculation and writer SQL (ADR-0006 §4): a replay passes the recorded effective_at and gets
-- the same rows, which now()/CURRENT_DATE can never guarantee. Returns disabled versions too —
-- callers filter on `enabled`, so "retired then" and "not registered then" stay distinguishable.
CREATE OR REPLACE FUNCTION oracle_asset_as_of(p_effective_at date)
RETURNS SETOF oracle_asset
LANGUAGE sql
STABLE
AS $$
    SELECT DISTINCT ON (oracle_id, token_id, feed_address) *
    FROM oracle_asset
    WHERE valid_from <= p_effective_at
    ORDER BY oracle_id, token_id, feed_address, valid_from DESC, processing_version DESC
$$;
COMMENT ON FUNCTION oracle_asset_as_of(date) IS
'Effective oracle_asset version per natural key as of p_effective_at (ADR-0006 §4). The calculation/writer read path; pass a recorded effective_at, never now(). Includes disabled versions.';

-- Operational reads only (ADR-0006 §4): bounded on UTC today, so a future-dated version is not
-- current until its valid_from arrives — which is exactly why it is banned from calculation SQL,
-- where the same query must not change answer with the wall clock.
CREATE OR REPLACE VIEW oracle_asset_current AS
SELECT DISTINCT ON (oracle_id, token_id, feed_address) *
FROM oracle_asset
WHERE valid_from <= (now() AT TIME ZONE 'utc')::date
ORDER BY oracle_id, token_id, feed_address, valid_from DESC, processing_version DESC;
COMMENT ON VIEW oracle_asset_current IS
'[Configuration] Latest effective oracle_asset version per natural key as of UTC today. OPERATIONAL reads only — calculation and writer SQL must use oracle_asset_as_of(effective_at) (ADR-0006 §4).';

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
    v.processing_version,
    v.valid_from,
    v.change_reason,
    v.valid_to_exclusive,
    (v.valid_from <= (now() AT TIME ZONE 'utc')::date
        AND (v.valid_to_exclusive IS NULL OR (now() AT TIME ZONE 'utc')::date < v.valid_to_exclusive)) AS is_current
FROM (
    SELECT oracle_asset.*,
        lead(valid_from) OVER (
            PARTITION BY oracle_id, token_id, feed_address
            ORDER BY valid_from, processing_version
        ) AS valid_to_exclusive
    FROM oracle_asset
) v;
COMMENT ON VIEW oracle_asset_versions IS
'[Configuration] Full oracle_asset history per natural key with derived valid_to_exclusive (half-open [valid_from, valid_to_exclusive)) and is_current as of UTC today. Audit/history reads.';

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
    p_effective_at  date,
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

    SELECT * INTO current_version
    FROM oracle_asset
    WHERE oracle_id = p_oracle_id
      AND token_id = p_token_id
      AND feed_address IS NOT DISTINCT FROM p_feed_address
    ORDER BY valid_from DESC, processing_version DESC
    LIMIT 1;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'oracle_asset (oracle_id=%, token_id=%, feed_address=%) is not registered; register it with an INSERT before toggling it',
            p_oracle_id, p_token_id, COALESCE(encode(p_feed_address, 'hex'), 'NULL');
    END IF;

    IF current_version.enabled = p_enabled THEN
        RETURN NULL;
    END IF;

    -- valid_from is the effective-ordering key, so a version dated before the one it supersedes
    -- could never become current: fail loudly instead of appending a row that does nothing.
    IF p_effective_at < current_version.valid_from THEN
        RAISE EXCEPTION 'valid_from % predates the current version''s % for oracle_asset (oracle_id=%, token_id=%); a correction must be dated on or after the row it supersedes',
            p_effective_at, current_version.valid_from, p_oracle_id, p_token_id;
    END IF;

    SELECT max(processing_version) + 1 INTO next_version
    FROM oracle_asset
    WHERE oracle_id = p_oracle_id
      AND token_id = p_token_id
      AND feed_address IS NOT DISTINCT FROM p_feed_address;

    INSERT INTO oracle_asset (
        oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency,
        processing_version, valid_from, change_reason)
    VALUES (
        current_version.oracle_id, current_version.token_id, p_enabled, current_version.feed_address,
        current_version.feed_decimals, current_version.quote_currency,
        next_version, p_effective_at, p_change_reason);

    RETURN next_version;
END $$;
COMMENT ON FUNCTION oracle_asset_set_enabled(bigint, bigint, bytea, boolean, date, text) IS
'Appends a new oracle_asset version toggling `enabled` (ADR-0006 §4). Returns the new processing_version, or NULL if unchanged. Advisory-locked on the natural key; raises on an unregistered key or a backdated valid_from.';

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
