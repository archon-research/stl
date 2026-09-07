-- VEC-412: security_instrument_bridge: the one table that maps every on-chain instrument to a
-- security (closes Gap A). Replaces the old inline token_id / morpho_market_id / morpho_vault_id
-- resolution columns: a position resolves its security by looking its instrument_key up here, then
-- joining security_master_current on security_id (VEC-420). Rows are loaded in VEC-419.
--
-- instrument_key is the instrument's NATIVE, globally-unique identifier, never one of our own
-- classifications. Rule: no house classifier may appear in a hashed key (position_id hashes
-- instrument_key), so the key is intrinsic, assigned by the chain or the source, not by us:
--   token / aToken / debtToken / ERC-4626 vault / loan  -> the on-chain contract address
--   Morpho market                                       -> the protocol-emitted market id (bytes32)
--   Sky ilk                                             -> ilk-registry address ':' ilk (bytes32)
--   Anchorage package                                   -> provider ':' package_id (source-native id)
-- Because the key is the native id it is globally unique on its own; position_id and VEC-420 rely on
-- that. chain_id (carried in position_id and at resolution) qualifies cross-chain address reuse.
-- Composite native keys join components with ':' (each component is ':'-free hex/text). An aToken and
-- its debtToken are different contract addresses, so the supply and borrow legs are distinct keys with
-- no classifier (VEC-413 supplies the debt-token address).
--
-- No instrument_kind column: an earlier revision namespaced the key by a house "kind" taxonomy
-- (token / lending_reserve / morpho_market / ...). That was dropped: (a) kind is our classification,
-- not protocol-native, and must not sit in a key that feeds position_id; (b) once instrument_key is
-- the native id it is globally unique on its own, so kind adds nothing to resolution.
--
-- No FK on security_id: security_master.security_id is an SCD2 natural key with many versions, so it is
-- not unique and cannot be an FK target. Resolution goes through security_master_current, not a
-- row-level FK.
--
-- SCD2-lite: re-pointing an instrument to a different security is a new INSERT with a higher
-- processing_version (and a later valid_from); the prior row is kept. Append-only: UPDATE / DELETE /
-- TRUNCATE are revoked below. The current mapping is the latest valid_from, with processing_version
-- breaking same-day ties so two re-points on the same calendar day are both representable; the loader
-- (VEC-419) owns monotonic processing_version assignment per instrument_key, matching security_master.
-- Append-only guard note: revoking UPDATE / DELETE / TRUNCATE works here only because nothing FKs this
-- table. The moment any table declares an FK against security_instrument_bridge, it hits the
-- reference-table FK-insert privilege trap fixed in 20260714_160000 (Simon's note).
-- No `SET search_path`: a session-level SET leaks onto the migrator's pooled connection and
-- desyncs the per-test-schema integration harness (see VEC-411). Objects land in the connection's
-- search_path (public in prod, the per-test schema under tests), like every other table migration.

CREATE TABLE IF NOT EXISTS security_instrument_bridge (
    instrument_key     text NOT NULL,   -- the instrument's native, globally-unique id (address / market id / registry:ilk / provider:package); never a house classifier
    security_id        text NOT NULL,   -- soft ref to security_master.security_id (resolve via security_master_current)
    processing_version integer NOT NULL DEFAULT 1,  -- monotonic per instrument_key; dedup + version key
    valid_from         date NOT NULL DEFAULT CURRENT_DATE,
    change_reason      text NOT NULL,   -- mandatory: why this mapping / re-point exists
    created_at         timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_key, processing_version),
    CONSTRAINT sib_processing_version_chk CHECK (processing_version >= 1)
);

-- Catalog metadata (downstream data-dictionary / schema_master tooling reads pg_catalog comments).
COMMENT ON TABLE security_instrument_bridge IS '[Dimension] Maps every on-chain instrument to a security_id, keyed by the instrument''s native globally-unique id (never a house classifier). Soft ref to security_master (no FK: security_id is a non-unique SCD2 key); resolve via security_master_current. Append-only; latest mapping per instrument is security_instrument_bridge_current.';
COMMENT ON COLUMN security_instrument_bridge.instrument_key IS 'PK. The instrument''s native, globally-unique identifier: on-chain contract address (token/aToken/debtToken/vault/loan), Morpho market id (bytes32), ilk-registry address '':'' ilk, or provider '':'' package_id. Not a house classification; this is what position_id hashes.';
COMMENT ON COLUMN security_instrument_bridge.security_id IS 'Soft ref to security_master.security_id (resolve via security_master_current; not an FK).';
COMMENT ON COLUMN security_instrument_bridge.processing_version IS 'PK. Monotonic version per instrument_key (>=1); loader-assigned. Latest valid_from is the current mapping; this breaks same-day ties.';
COMMENT ON COLUMN security_instrument_bridge.valid_from IS 'Date this mapping became effective.';
COMMENT ON COLUMN security_instrument_bridge.change_reason IS 'Mandatory: why this mapping / re-point exists.';
COMMENT ON COLUMN security_instrument_bridge.created_at IS 'Audit. Write timestamp.';

-- Reverse lookup: every instrument mapped to a given security. Forward resolution
-- (WHERE instrument_key = ?) is served by the leading PK column.
CREATE INDEX IF NOT EXISTS sib_security_idx ON security_instrument_bridge (security_id);
-- Supports the current-view sort (mirrors security_master's sm_current_idx).
CREATE INDEX IF NOT EXISTS sib_current_idx ON security_instrument_bridge (instrument_key, valid_from DESC, processing_version DESC);

-- Current mapping per instrument: latest valid_from wins, processing_version breaks same-day ties
-- (deterministic, matching security_master_current). Bounded on CURRENT_DATE so a future-dated re-point
-- does not become current early. VEC-420 resolves security against THIS view, not the base table.
CREATE OR REPLACE VIEW security_instrument_bridge_current AS
SELECT DISTINCT ON (instrument_key) *
FROM security_instrument_bridge
WHERE valid_from <= CURRENT_DATE
ORDER BY instrument_key, valid_from DESC, processing_version DESC;
COMMENT ON VIEW security_instrument_bridge_current IS '[Dimension] Latest effective mapping per instrument_key (valid_from <= today). What VEC-420 resolves security against.';

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
