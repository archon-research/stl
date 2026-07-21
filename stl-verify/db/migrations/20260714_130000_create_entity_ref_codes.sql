-- VEC-414: entity_ref_codes, the (code_type, code_value) -> entity_id resolver. Maps an external
-- identifier to a legal entity so holders and counterparties resolve to a single entity_id.
--
-- This is the table the Gap B holder step (VEC-417) joins through: a wallet holder resolves by its
-- on-chain address as a BLOCKCHAIN_ADDRESS code (hex-encoded, no 0x prefix), which lets many
-- addresses map to one entity. Custodians / counterparties resolve the same way from LEI, SWIFT/BIC,
-- an INTERNAL code, or a CONTRACT_ADDRESS.
--
-- No FK on entity_id: entity_master.entity_id is an SCD2 natural key with many versions, so it is
-- not unique and cannot be an FK target. Resolution goes through entity_master_current, not a
-- row-level FK (mirrors security_instrument_bridge -> security_master).
--
-- SCD2-lite (mirrors security_instrument_bridge): re-pointing a code to a different entity is a new
-- INSERT with a higher processing_version (and a later valid_from); the prior row is kept.
-- Append-only, UPDATE / DELETE / TRUNCATE are revoked below. processing_version (not valid_from) is
-- the version axis so two re-points on the same calendar day are both representable; the loader
-- (VEC-418) owns monotonic assignment per (code_type, code_value).
--
-- The ticket's original schema used an explicit valid_to column; this follows the shipped house
-- pattern instead (append-only, valid_to derived by taking the current row), matching the four
-- sibling masters/bridges.
-- No `SET search_path`: a session-level SET leaks onto the migrator's pooled connection and
-- desyncs the per-test-schema integration harness (see VEC-411). Objects land in the connection's
-- search_path (public in prod, the per-test schema under tests), like every other table migration.

CREATE TABLE IF NOT EXISTS entity_ref_codes (
    code_type          text NOT NULL,   -- which identifier namespace (see CHECK); pinned so a typo'd type fails hard
    code_value         text NOT NULL,   -- the code within the namespace (e.g. hex address, LEI string)
    entity_id          text NOT NULL,   -- soft ref to entity_master.entity_id (resolve via entity_master_current)
    processing_version integer NOT NULL DEFAULT 1,  -- monotonic per (code_type, code_value); dedup + version key
    valid_from         date NOT NULL DEFAULT CURRENT_DATE,
    change_reason      text NOT NULL,   -- mandatory: why this mapping / re-point exists
    created_at         timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (code_type, code_value, processing_version),
    CONSTRAINT erc_processing_version_chk CHECK (processing_version >= 1),
    CONSTRAINT erc_code_type_chk CHECK (code_type IN (
        'BLOCKCHAIN_ADDRESS',   -- on-chain wallet / EOA address, hex-encoded (VEC-417 holder path)
        'CONTRACT_ADDRESS',     -- on-chain contract address
        'LEI',                  -- ISO 17442 Legal Entity Identifier
        'SWIFT_BIC',            -- SWIFT/BIC code
        'INTERNAL'              -- internal code (e.g. a prime's vault address / house identifier)
    )),
    -- Address codes are stored hex-encoded, lowercase, no 0x prefix (the encode(addr,'hex') form), so
    -- VEC-417's holder join cannot silently miss on a 0x-prefixed or mixed-case value. Length-agnostic
    -- on purpose (non-EVM address widths); tighten to {40} if this ever becomes EVM-only.
    CONSTRAINT erc_address_value_chk CHECK (
        code_type NOT IN ('BLOCKCHAIN_ADDRESS','CONTRACT_ADDRESS') OR code_value ~ '^[0-9a-f]+$'
    )
);

-- Catalog metadata (downstream data-dictionary / schema_master tooling reads pg_catalog comments).
COMMENT ON TABLE entity_ref_codes IS '[Dimension] Maps an external identifier (code_type, code_value) to an entity_id. Soft ref to entity_master (no FK: entity_id is a non-unique SCD2 key); resolve via entity_master_current. Append-only; latest mapping per code is entity_ref_codes_current. The holder/counterparty resolver (VEC-417) joins through this.';
COMMENT ON COLUMN entity_ref_codes.code_type IS 'PK. Identifier namespace; one of the CHECK-pinned types (BLOCKCHAIN_ADDRESS, CONTRACT_ADDRESS, LEI, SWIFT_BIC, INTERNAL).';
COMMENT ON COLUMN entity_ref_codes.code_value IS 'PK. The code within the namespace (e.g. a hex-encoded on-chain address with no 0x prefix, an LEI string).';
COMMENT ON COLUMN entity_ref_codes.entity_id IS 'Soft ref to entity_master.entity_id (resolve via entity_master_current; not an FK).';
COMMENT ON COLUMN entity_ref_codes.processing_version IS 'PK. Monotonic version per (code_type, code_value) (>=1); loader-assigned. Latest is the current mapping.';
COMMENT ON COLUMN entity_ref_codes.valid_from IS 'Date this mapping became effective.';
COMMENT ON COLUMN entity_ref_codes.change_reason IS 'Mandatory: why this mapping / re-point exists.';
COMMENT ON COLUMN entity_ref_codes.created_at IS 'Audit. Write timestamp.';

-- Reverse lookup: every code mapped to a given entity. Forward resolution
-- (WHERE code_type = ? AND code_value = ?) is served by the leading PK columns.
CREATE INDEX IF NOT EXISTS erc_entity_idx ON entity_ref_codes (entity_id);
-- Supports entity_ref_codes_current's ORDER BY (the PK can't: valid_from isn't in it). VEC-417 joins
-- holders through that view, so this is the hot path (mirrors security_instrument_bridge.sib_current_idx).
CREATE INDEX IF NOT EXISTS erc_current_idx
    ON entity_ref_codes (code_type, code_value, valid_from DESC, processing_version DESC);

-- Current mapping per code: latest valid_from wins, processing_version breaks same-day ties
-- (deterministic, matching security_instrument_bridge_current). VEC-417 resolves against THIS view,
-- not the base table, or a historical re-point would fan out to multiple rows.
CREATE OR REPLACE VIEW entity_ref_codes_current AS
SELECT DISTINCT ON (code_type, code_value) *
FROM entity_ref_codes
WHERE valid_from <= CURRENT_DATE
ORDER BY code_type, code_value, valid_from DESC, processing_version DESC;
COMMENT ON VIEW entity_ref_codes_current IS '[Dimension] Latest effective mapping per (code_type, code_value) (valid_from <= today). What the holder/counterparty resolver (VEC-417) joins through.';

-- Reads for both roles; append-only writes for the indexer role (INSERT, never UPDATE/DELETE).
GRANT SELECT ON entity_ref_codes, entity_ref_codes_current TO stl_readonly;
GRANT SELECT, INSERT ON entity_ref_codes TO stl_readwrite;
GRANT SELECT ON entity_ref_codes_current TO stl_readwrite;

-- Append-only guard: revoke mutation on the indexer role and the table owner so any UPDATE / DELETE /
-- TRUNCATE errors out. Guarded by role existence; mirrors the reference-table and bridge guards.
DO $$
DECLARE role text;
BEGIN
    FOREACH role IN ARRAY ARRAY['stl_readwrite','stl_migrator'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role) THEN
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON entity_ref_codes FROM %I', role);
        END IF;
    END LOOP;
END $$;

INSERT INTO migrations (filename) VALUES ('20260714_130000_create_entity_ref_codes.sql') ON CONFLICT (filename) DO NOTHING;
