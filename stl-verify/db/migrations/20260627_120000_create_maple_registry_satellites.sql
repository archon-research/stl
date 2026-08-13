-- Maple registry data lineage: hub + SCD2 satellite.
-- Each Maple registry splits into a stable identity hub and an append-only
-- attribute satellite keyed (hub_id, synced_at). Editorial attributes live in
-- the satellite; a change appends a satellite row, giving full editorial
-- history while the hub id (and every FK to it) never moves.
--
-- Axis: synced_at only. Maple data is off-chain GraphQL with no reorg concept
-- (see the create_maple_graphql_tables header), so there is no block_version
-- and no reorg supersession here. Syncs are monotonic cron cycles, so
-- append-on-change compares against the latest satellite row.
--
-- hashdiff: md5 over a canonical encoding of the editorial fields. Fields are
-- joined by 0x1f and SQL NULL is rendered as 0x1e so the encoding is injective:
-- 0x1f delimits fields (else ['a','bc'] and ['ab','c'] would collide) and 0x1e
-- distinguishes NULL from '' (else a NULL<->'' change would hash identically).
-- Both bytes are control chars that never occur in the editorial text values.
-- The application computes the identical hash (metaHashdiff in the postgres
-- adapter); the backfill below must match it byte-for-byte so the first
-- post-deploy sync sees no false change. md5 is sufficient (change detection
-- over tiny registries, not security) and needs no pgcrypto extension.
--
-- Append-only is enforced: UPDATE/DELETE are revoked from the application role.
--
-- is_internal is removed; internal-ness (loan_meta_type IN ('amm','strategy'))
-- is now derived in code and the *_current view.

-- ============================================================================
-- Satellites
-- ============================================================================

CREATE TABLE IF NOT EXISTS maple_pool_meta
(
    maple_pool_id BIGINT      NOT NULL REFERENCES maple_pool (id),
    synced_at     TIMESTAMPTZ NOT NULL,
    name          VARCHAR(255),
    is_syrup      BOOLEAN     NOT NULL,
    hashdiff      BYTEA       NOT NULL,
    is_present    BOOLEAN     NOT NULL DEFAULT TRUE,
    build_id      INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (maple_pool_id, synced_at)
);

CREATE TABLE IF NOT EXISTS maple_loan_meta
(
    maple_loan_id            BIGINT      NOT NULL REFERENCES maple_loan (id),
    synced_at                TIMESTAMPTZ NOT NULL,
    loan_type                VARCHAR(16) NOT NULL,
    loan_meta_type           VARCHAR(32),
    loan_meta_asset_symbol   VARCHAR(50),
    loan_meta_dex            VARCHAR(64),
    loan_meta_wallet_address VARCHAR(255),
    loan_meta_wallet_type    VARCHAR(32),
    loan_meta_location       VARCHAR(64),
    hashdiff                 BYTEA       NOT NULL,
    is_present               BOOLEAN     NOT NULL DEFAULT TRUE,
    build_id                 INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (maple_loan_id, synced_at)
);

CREATE TABLE IF NOT EXISTS maple_sky_strategy_meta
(
    maple_sky_strategy_id BIGINT      NOT NULL REFERENCES maple_sky_strategy (id),
    synced_at             TIMESTAMPTZ NOT NULL,
    version               INT,
    hashdiff              BYTEA       NOT NULL,
    is_present            BOOLEAN     NOT NULL DEFAULT TRUE,
    build_id              INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (maple_sky_strategy_id, synced_at)
);

-- ============================================================================
-- Backfill: one satellite row per existing hub row, stamped at the entity's
-- earliest observed synced_at (its first state snapshot), falling back to the
-- hub's creation timestamp when no state row exists yet. This single row is a
-- synthetic "state as of migration" seed carrying the current editorial values,
-- not a reconstruction of historical editorial values.
--
-- hashdiff must equal metaHashdiff byte-for-byte so the first post-deploy sync
-- sees no false change. The NULL fallback per column mirrors how the Go encoder
-- renders that column's zero value: name/version/loan_type are non-pointer in
-- the entity, so Go renders an absent value as '' / '0' (never the NULL
-- sentinel); only the genuinely-nullable loan_meta_* columns (Go *string, nil
-- when absent) use the E'\x1e' NULL sentinel.
-- ============================================================================

INSERT INTO maple_pool_meta (maple_pool_id, synced_at, name, is_syrup, hashdiff, is_present, build_id)
SELECT p.id,
       COALESCE((SELECT MIN(synced_at) FROM maple_pool_state s WHERE s.maple_pool_id = p.id), p.created_at),
       p.name,
       p.is_syrup,
       decode(md5(
           COALESCE(p.name, '') || E'\x1f' ||
           (CASE WHEN p.is_syrup THEN 'true' ELSE 'false' END)
       ), 'hex'),
       TRUE,
       0
FROM maple_pool p
ON CONFLICT (maple_pool_id, synced_at) DO NOTHING;

INSERT INTO maple_loan_meta (maple_loan_id, synced_at, loan_type, loan_meta_type, loan_meta_asset_symbol,
                             loan_meta_dex, loan_meta_wallet_address, loan_meta_wallet_type, loan_meta_location,
                             hashdiff, is_present, build_id)
SELECT l.id,
       COALESCE((SELECT MIN(synced_at) FROM maple_loan_state s WHERE s.maple_loan_id = l.id), l.first_seen_at),
       l.loan_type,
       l.loan_meta_type,
       l.loan_meta_asset_symbol,
       l.loan_meta_dex,
       l.loan_meta_wallet_address,
       l.loan_meta_wallet_type,
       l.loan_meta_location,
       decode(md5(
           l.loan_type || E'\x1f' ||
           COALESCE(l.loan_meta_type, E'\x1e') || E'\x1f' ||
           COALESCE(l.loan_meta_asset_symbol, E'\x1e') || E'\x1f' ||
           COALESCE(l.loan_meta_dex, E'\x1e') || E'\x1f' ||
           COALESCE(l.loan_meta_wallet_address, E'\x1e') || E'\x1f' ||
           COALESCE(l.loan_meta_wallet_type, E'\x1e') || E'\x1f' ||
           COALESCE(l.loan_meta_location, E'\x1e')
       ), 'hex'),
       TRUE,
       0
FROM maple_loan l
ON CONFLICT (maple_loan_id, synced_at) DO NOTHING;

INSERT INTO maple_sky_strategy_meta (maple_sky_strategy_id, synced_at, version, hashdiff, is_present, build_id)
SELECT st.id,
       COALESCE((SELECT MIN(synced_at) FROM maple_sky_strategy_state s WHERE s.maple_sky_strategy_id = st.id), st.created_at),
       st.version,
       decode(md5(COALESCE(st.version::text, '0')), 'hex'),
       TRUE,
       0
FROM maple_sky_strategy st
ON CONFLICT (maple_sky_strategy_id, synced_at) DO NOTHING;

-- ============================================================================
-- Drop editorial columns from the hubs. is_internal (GENERATED over
-- loan_meta_type) must go before its source column. Indexes on dropped columns
-- (idx_maple_pool_is_syrup, idx_maple_loan_internal) drop automatically.
-- ============================================================================

ALTER TABLE maple_pool
    DROP COLUMN IF EXISTS name,
    DROP COLUMN IF EXISTS is_syrup;

ALTER TABLE maple_loan DROP COLUMN IF EXISTS is_internal;
ALTER TABLE maple_loan
    DROP COLUMN IF EXISTS loan_type,
    DROP COLUMN IF EXISTS loan_meta_type,
    DROP COLUMN IF EXISTS loan_meta_asset_symbol,
    DROP COLUMN IF EXISTS loan_meta_dex,
    DROP COLUMN IF EXISTS loan_meta_wallet_address,
    DROP COLUMN IF EXISTS loan_meta_wallet_type,
    DROP COLUMN IF EXISTS loan_meta_location;

ALTER TABLE maple_sky_strategy DROP COLUMN IF EXISTS version;

-- ============================================================================
-- Convenience views: hub identity + the latest satellite row, presenting a flat
-- column shape for external SQL consumers (Grafana, analysts). The
-- lateral takes the latest row regardless of is_present, then the outer filter
-- drops entities whose latest row is a tombstone, so "current" means "still
-- live". internal-ness is derived from the current loan_meta_type.
-- ============================================================================

CREATE OR REPLACE VIEW maple_pool_current AS
SELECT p.id, p.chain_id, p.protocol_id, p.address, p.asset_token_id, p.created_at,
       m.name, m.is_syrup
FROM maple_pool p
LEFT JOIN LATERAL (
    SELECT name, is_syrup, is_present
    FROM maple_pool_meta
    WHERE maple_pool_id = p.id
    ORDER BY synced_at DESC
    LIMIT 1
) m ON TRUE
WHERE m.is_present;

CREATE OR REPLACE VIEW maple_loan_current AS
SELECT l.id, l.chain_id, l.protocol_id, l.loan_address, l.maple_pool_id, l.borrower_user_id, l.first_seen_at,
       m.loan_type, m.loan_meta_type, m.loan_meta_asset_symbol, m.loan_meta_dex,
       m.loan_meta_wallet_address, m.loan_meta_wallet_type, m.loan_meta_location,
       COALESCE(m.loan_meta_type IN ('amm', 'strategy'), FALSE) AS is_internal
FROM maple_loan l
LEFT JOIN LATERAL (
    SELECT loan_type, loan_meta_type, loan_meta_asset_symbol, loan_meta_dex,
           loan_meta_wallet_address, loan_meta_wallet_type, loan_meta_location, is_present
    FROM maple_loan_meta
    WHERE maple_loan_id = l.id
    ORDER BY synced_at DESC
    LIMIT 1
) m ON TRUE
WHERE m.is_present;

CREATE OR REPLACE VIEW maple_sky_strategy_current AS
SELECT st.id, st.chain_id, st.strategy_address, st.maple_pool_id, st.created_at,
       m.version
FROM maple_sky_strategy st
LEFT JOIN LATERAL (
    SELECT version, is_present
    FROM maple_sky_strategy_meta
    WHERE maple_sky_strategy_id = st.id
    ORDER BY synced_at DESC
    LIMIT 1
) m ON TRUE
WHERE m.is_present;

-- ============================================================================
-- Append-only enforcement: the application role may SELECT and INSERT satellite
-- rows but never mutate or delete them. (Table owner stl_migrator is unaffected,
-- as Postgres owners bypass these grants.)
-- ============================================================================

REVOKE UPDATE, DELETE ON maple_pool_meta FROM stl_readwrite;
REVOKE UPDATE, DELETE ON maple_loan_meta FROM stl_readwrite;
REVOKE UPDATE, DELETE ON maple_sky_strategy_meta FROM stl_readwrite;

INSERT INTO migrations (filename)
VALUES ('20260627_120000_create_maple_registry_satellites.sql')
ON CONFLICT (filename) DO NOTHING;
