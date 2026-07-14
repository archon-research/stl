-- VEC-412: security_instrument_bridge: the one table that maps every on-chain instrument shape to a
-- security (closes Gap A). Replaces the old inline token_id / morpho_market_id / morpho_vault_id
-- resolution columns: a position resolves its security_sk by looking its (instrument_kind, instrument_key)
-- up here, then joining security_master_current on security_id (VEC-420). Rows are loaded in VEC-419.
--
-- Covers all 7 instrument shapes. Today only the token-based ones resolve; sky_ilk, anchorage, and the
-- maple_* kinds are the verified gap this table closes once VEC-419 populates them.
--
-- No FK on security_id: security_master.security_id is an SCD2 natural key with many versions, so it is
-- not unique and cannot be an FK target. Resolution goes through security_master_current, not a row-level
-- FK. instrument_key is the instrument identity within the kind (a token's key is its <token_id> alone;
-- the proxy that holds the token is a HOLDER, not part of instrument identity, so the same token held at
-- many proxies is one security). For kinds that genuinely need more than one instrument-level component,
-- composite keys are joined with ':' (e.g. a Sky ilk is its ilk_name).
--
-- SCD2-lite: re-pointing an instrument to a different security is a new INSERT with a higher
-- processing_version (and a later valid_from); the prior row is kept. Append-only: UPDATE / DELETE /
-- TRUNCATE are revoked below. The current mapping is the latest valid_from, with processing_version
-- breaking same-day ties so two re-points on the same calendar day are both representable; the loader
-- (VEC-419) owns monotonic processing_version assignment per (instrument_kind, instrument_key), matching
-- security_master.
-- Append-only guard note: revoking UPDATE / DELETE / TRUNCATE works here only because nothing FKs this
-- table. The moment any table declares an FK against security_instrument_bridge, it hits the
-- reference-table FK-insert privilege trap fixed in 20260714_160000 (Simon's note).
-- No `SET search_path`: a session-level SET leaks onto the migrator's pooled connection and
-- desyncs the per-test-schema integration harness (see VEC-411). Objects land in the connection's
-- search_path (public in prod, the per-test schema under tests), like every other table migration.

CREATE TABLE IF NOT EXISTS security_instrument_bridge (
    instrument_kind    text NOT NULL,   -- which instrument shape (see CHECK); pinned so a typo'd kind fails hard
    instrument_key     text NOT NULL,   -- id within the kind; composite values joined with ':' (components are ':'-free: hex/int)
    security_id        text NOT NULL,   -- soft ref to security_master.security_id (resolve via security_master_current)
    processing_version integer NOT NULL DEFAULT 1,  -- monotonic per (kind,key); dedup + version key
    valid_from         date NOT NULL DEFAULT CURRENT_DATE,
    change_reason      text NOT NULL,   -- mandatory: why this mapping / re-point exists
    created_at         timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_kind, instrument_key, processing_version),
    CONSTRAINT sib_processing_version_chk CHECK (processing_version >= 1),
    -- The 7 shapes (10 kinds). Extending the vocabulary for a new protocol is a deliberate new migration.
    CONSTRAINT sib_kind_chk CHECK (instrument_kind IN (
        'token', 'lending_reserve',            -- Morpho / SparkLend / Aave / prices (token-based)
        'morpho_market', 'morpho_vault',       -- Morpho Blue market + ERC-4626 vault
        'sky_ilk',                             -- Sky prime_debt (ilk)
        'anchorage_package',                   -- Anchorage custody package
        'maple_loan', 'maple_pool', 'maple_sky_strategy', 'maple_ftl_loan'  -- Maple (pool/loan, maple_sky_strategy, maple_ftl_loan)
    ))
);

-- Catalog metadata (downstream data-dictionary / schema_master tooling reads pg_catalog comments).
COMMENT ON TABLE security_instrument_bridge IS '[Dimension] Maps every on-chain instrument shape (instrument_kind, instrument_key) to a security_id. Soft ref to security_master (no FK: security_id is a non-unique SCD2 key); resolve via security_master_current. Append-only; latest mapping per instrument is security_instrument_bridge_current.';
COMMENT ON COLUMN security_instrument_bridge.instrument_kind IS 'PK. Instrument shape; one of the CHECK-pinned kinds (token, lending_reserve, morpho_market, morpho_vault, sky_ilk, anchorage_package, maple_loan/pool, maple_sky_strategy, maple_ftl_loan).';
COMMENT ON COLUMN security_instrument_bridge.instrument_key IS 'PK. Instrument identity within the kind (a token key is its <token_id> alone; the holding proxy is not part of it). Composite values joined with '':'' for kinds needing more than one instrument-level component (components are '':''-free: hex/int).';
COMMENT ON COLUMN security_instrument_bridge.security_id IS 'Soft ref to security_master.security_id (resolve via security_master_current; not an FK).';
COMMENT ON COLUMN security_instrument_bridge.processing_version IS 'PK. Monotonic version per (instrument_kind, instrument_key) (>=1); loader-assigned. The current mapping is the latest valid_from, with processing_version breaking same-day ties.';
COMMENT ON COLUMN security_instrument_bridge.valid_from IS 'Date this mapping became effective.';
COMMENT ON COLUMN security_instrument_bridge.change_reason IS 'Mandatory: why this mapping / re-point exists.';
COMMENT ON COLUMN security_instrument_bridge.created_at IS 'Audit. Write timestamp.';

-- Reverse lookup: every instrument mapped to a given security. Forward resolution
-- (WHERE instrument_kind = ? AND instrument_key = ?) is served by the leading PK columns.
CREATE INDEX IF NOT EXISTS sib_security_idx ON security_instrument_bridge (security_id);

-- Supports the current view's sort (mirrors security_master's sm_current_idx).
CREATE INDEX IF NOT EXISTS sib_current_idx ON security_instrument_bridge (instrument_kind, instrument_key, valid_from DESC, processing_version DESC);

-- Current mapping per instrument: latest valid_from wins, processing_version breaks same-day ties
-- (deterministic, matching security_master_current). VEC-420 must resolve security_sk against THIS view,
-- not the base table, or a historical re-point would fan out to multiple rows.
CREATE OR REPLACE VIEW security_instrument_bridge_current AS
SELECT DISTINCT ON (instrument_kind, instrument_key) *
FROM security_instrument_bridge
WHERE valid_from <= CURRENT_DATE
ORDER BY instrument_kind, instrument_key, valid_from DESC, processing_version DESC;
COMMENT ON VIEW security_instrument_bridge_current IS '[Dimension] Latest effective mapping per (instrument_kind, instrument_key) (valid_from <= today). What VEC-420 resolves security_sk against.';

-- Reads for both roles; append-only writes for the indexer role (INSERT, never UPDATE/DELETE).
GRANT SELECT ON security_instrument_bridge, security_instrument_bridge_current TO stl_readonly;
GRANT SELECT, INSERT ON security_instrument_bridge TO stl_readwrite;
GRANT SELECT ON security_instrument_bridge_current TO stl_readwrite;

-- Append-only guard: revoke mutation on the indexer role and the table owner so any UPDATE / DELETE /
-- TRUNCATE errors out. Guarded by role existence; mirrors the reference-table and security_master guards.
DO $$
DECLARE role text;
BEGIN
    FOREACH role IN ARRAY ARRAY['stl_readwrite','stl_migrator'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role) THEN
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON security_instrument_bridge FROM %I', role);
        END IF;
    END LOOP;
END $$;

INSERT INTO migrations (filename) VALUES ('20260713_150000_create_security_instrument_bridge.sql') ON CONFLICT (filename) DO NOTHING;
