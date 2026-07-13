-- VEC-412: security_instrument_bridge — the one table that maps every on-chain instrument shape to a
-- security (closes Gap A). Replaces the old inline token_id / morpho_market_id / morpho_vault_id
-- resolution columns: a position resolves its security_sk by looking its (instrument_kind, instrument_key)
-- up here, then joining security_master_current on security_id (VEC-420). Rows are loaded in VEC-419.
--
-- Covers all 7 instrument shapes. Today only the token-based ones resolve; sky_ilk, anchorage, and the
-- maple_* kinds are the verified gap this table closes once VEC-419 populates them.
--
-- No FK on security_id: security_master.security_id is an SCD2 natural key with many versions, so it is
-- not unique and cannot be an FK target. Resolution goes through security_master_current, not a row-level
-- FK. instrument_key is the id within the kind; composite keys are joined with ':' (e.g. a token at a
-- proxy is '<token_id>:<proxy_hex>', a Sky ilk is its ilk_name).
--
-- SCD2-lite via valid_from: re-pointing an instrument to a different security is a new INSERT with a
-- later valid_from; the prior row is kept. Append-only — UPDATE / DELETE / TRUNCATE are revoked below.
SET search_path TO public;

CREATE TABLE security_instrument_bridge (
    instrument_kind text NOT NULL,   -- which instrument shape (see CHECK); pinned so a typo'd kind fails hard
    instrument_key  text NOT NULL,   -- id within the kind; composite values joined with ':'
    security_id     text NOT NULL,   -- soft ref to security_master.security_id (resolve via security_master_current)
    valid_from      date NOT NULL DEFAULT CURRENT_DATE,
    change_reason   text NOT NULL,   -- mandatory: why this mapping / re-point exists
    created_at      timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_kind, instrument_key, valid_from),
    -- The 7 shapes (9 kinds). Extending the vocabulary for a new protocol is a deliberate new migration.
    CONSTRAINT sib_kind_chk CHECK (instrument_kind IN (
        'token', 'lending_reserve',            -- Morpho / SparkLend / Aave / prices (token-based)
        'morpho_market', 'morpho_vault',       -- Morpho Blue market + ERC-4626 vault
        'sky_ilk',                             -- Sky prime_debt (ilk)
        'anchorage_package',                   -- Anchorage custody package
        'maple_loan', 'maple_pool', 'maple_strategy'  -- Maple
    ))
);

-- Reverse lookup: every instrument mapped to a given security. Forward resolution
-- (WHERE instrument_kind = ? AND instrument_key = ?) is served by the leading PK columns.
CREATE INDEX sib_security_idx ON security_instrument_bridge (security_id);

-- Current mapping per instrument: the latest valid_from wins. VEC-420 resolves security_sk against this.
CREATE VIEW security_instrument_bridge_current AS
SELECT DISTINCT ON (instrument_kind, instrument_key) *
FROM security_instrument_bridge
ORDER BY instrument_kind, instrument_key, valid_from DESC;

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
