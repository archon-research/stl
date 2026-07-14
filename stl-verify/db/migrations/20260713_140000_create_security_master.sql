-- VEC-411: security_master (append-only SCD2 instrument classification) + current/versioned views.
--
-- The canonical record for every instrument held. Pure CLASSIFICATION: what an instrument IS
-- (asset_class / security_type / security_subtype), its identifiers, issuer, status, and tokenisation.
-- Instrument -> security RESOLUTION is NOT here: it lives in security_instrument_bridge (VEC-412),
-- which replaces the old inline token_id / morpho_market_id / morpho_vault_id resolution columns.
-- Data load (classifying real instruments) is VEC-419.
--
-- SCD2 (framework Part 6): append-only. Only valid_from is stored; valid_to / is_current are derived
-- in security_master_versions. Every change is an INSERT; UPDATE / DELETE / TRUNCATE are revoked below,
-- and stl_readwrite is granted INSERT (not UPDATE/DELETE) so the append-only rule holds at the grant level.
--
-- Classification is FK-validated against the merged reference vocabulary (20260630_130000):
--   (asset_class)                                -> asset_class_ref
--   (asset_class, security_type)                 -> security_type_ref
--   (asset_class, security_type, security_subtype) -> security_subtype_ref  (MATCH SIMPLE: a NULL
--       subtype skips the FK, so a security whose type has no finer class carries subtype = NULL)
--   currency                                     -> currency_ref
--   country_of_risk / country_of_issuance        -> country_ref
-- issuer_entity_id is a soft ref: entity_master (VEC-410) is not built yet, so no FK is declared here.
--
-- Facet attributes (credit_tranche / credit_quality / collateral_pool / agency_status / backing) are
-- carried here as nullable columns with inline CHECK enums. Their reference table (instrument_facet_ref,
-- VEC-445) was cancelled, so the vocabularies are pinned in the CHECKs rather than a composite FK.
-- No `SET search_path` here: like every other table migration, objects land in the connection's
-- search_path (public in prod; the per-test schema under the integration harness). A session-level
-- SET would leak onto the pooled connection and desync the schema-isolated integration tests.

CREATE TABLE IF NOT EXISTS security_master (
    security_sk          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- per-version surrogate; NOT stamped onto positions (they resolve via security_id)
    security_id          text NOT NULL,                 -- natural key (sm-...), stable across all versions
    processing_version   integer NOT NULL DEFAULT 1,    -- monotonic per security_id; SCD2 dedup key
    valid_from           date NOT NULL DEFAULT CURRENT_DATE,  -- when this version became effective (only temporal field stored)
    change_reason        text NOT NULL,                 -- mandatory: why this version exists
    security_name        text,                          -- full display name
    ticker               text,
    isin                 text,                          -- ISO 6166; n/a for on-chain instruments
    cusip                text,
    sedol                text,
    figi                 text,
    asset_class          text NOT NULL,
    security_type        text NOT NULL,
    security_subtype     text,                          -- nullable: only when a finer class exists
    currency             text,                          -- ISO 4217 denomination
    country_of_issuance  text,
    country_of_risk      text,
    issuer_entity_id     text,                          -- soft ref to entity_master.entity_id (no FK: SCD2 key non-unique; resolve via entity_master_current)
    security_status      text NOT NULL DEFAULT 'ACTIVE',
    is_tokenised         boolean,                        -- TRUE = on-chain tokenised form of a traditional asset
    token_standard       text,                           -- ERC-20 / ERC-1400 / ERC-3643 / BENJI / ...; only when tokenised
    credit_tranche       text,                           -- structured-credit seniority (facet)
    credit_quality       text,                           -- IG / HY / PRIME (facet)
    collateral_pool      text,                           -- ABS collateral pool (facet)
    agency_status        text,                           -- AGENCY / NON_AGENCY (facet)
    backing              text,                           -- securitisation backing (CASH/SYNTHETIC) or stablecoin backing (FIAT/CRYPTO/ALGORITHMIC) (facet)
    source_system        text,
    created_at           timestamp with time zone NOT NULL DEFAULT now(),
    created_by           text NOT NULL DEFAULT 'system',
    approved_by          text,                           -- 4-eyes approver
    CONSTRAINT sm_asset_class_fkey FOREIGN KEY (asset_class) REFERENCES asset_class_ref(asset_class),
    CONSTRAINT sm_type_fkey FOREIGN KEY (asset_class, security_type)
        REFERENCES security_type_ref(asset_class, security_type),
    CONSTRAINT sm_subtype_fkey FOREIGN KEY (asset_class, security_type, security_subtype)
        REFERENCES security_subtype_ref(asset_class, security_type, security_subtype),
    CONSTRAINT sm_currency_fkey FOREIGN KEY (currency) REFERENCES currency_ref(currency_code),
    CONSTRAINT sm_country_risk_fkey FOREIGN KEY (country_of_risk) REFERENCES country_ref(country_code),
    CONSTRAINT sm_country_issue_fkey FOREIGN KEY (country_of_issuance) REFERENCES country_ref(country_code),
    CONSTRAINT sm_processing_version_chk CHECK (processing_version >= 1),
    CONSTRAINT sm_status_chk CHECK (security_status IN ('ACTIVE','SUSPENDED','MATURED','DELISTED')),
    CONSTRAINT sm_token_standard_chk CHECK (token_standard IS NULL OR is_tokenised IS TRUE),
    CONSTRAINT sm_credit_tranche_chk CHECK (credit_tranche IS NULL OR credit_tranche IN ('AAA','AA','A','BBB','BB','B','EQUITY')),
    CONSTRAINT sm_credit_quality_chk CHECK (credit_quality IS NULL OR credit_quality IN ('INVESTMENT_GRADE','HIGH_YIELD','PRIME')),
    CONSTRAINT sm_collateral_pool_chk CHECK (collateral_pool IS NULL OR collateral_pool IN ('AUTO','CREDIT_CARD','STUDENT_LOAN','CONSUMER')),
    CONSTRAINT sm_agency_status_chk CHECK (agency_status IS NULL OR agency_status IN ('AGENCY','NON_AGENCY')),
    CONSTRAINT sm_backing_chk CHECK (backing IS NULL OR backing IN ('CASH','SYNTHETIC','FIAT','CRYPTO','ALGORITHMIC'))
);

-- Catalog metadata (downstream data-dictionary / schema_master tooling reads pg_catalog comments).
-- [Type] tag on the table, per-column Roles (PK / FK / Audit), per db/migrations/AGENTS.md.
COMMENT ON TABLE security_master IS '[Dimension] Append-only SCD2 instrument-classification master. One row per (security_id, processing_version); the current classification per security_id is security_master_current. Positions resolve to it through the natural key security_id (via security_instrument_bridge and security_master_current), not the per-version surrogate.';
COMMENT ON COLUMN security_master.security_sk IS 'PK. Per-version surrogate (GENERATED ALWAYS), unique per (security_id, processing_version). Do NOT stamp onto positions: it pins to one version and goes stale on the next SCD2 row. Positions resolve through security_id via security_master_current (VEC-420).';
COMMENT ON COLUMN security_master.security_id IS 'Natural key (sm-<code>); stable across all SCD2 versions. This is what positions resolve against.';
COMMENT ON COLUMN security_master.processing_version IS 'SCD2 dedup/version key. Monotonic per security_id (>=1), loader-assigned.';
COMMENT ON COLUMN security_master.valid_from IS 'Date this version became effective; only temporal field stored (valid_to derived in security_master_versions).';
COMMENT ON COLUMN security_master.change_reason IS 'Mandatory: why this version exists.';
COMMENT ON COLUMN security_master.security_name IS 'Full display name.';
COMMENT ON COLUMN security_master.ticker IS 'Short trading symbol; stable across renames.';
COMMENT ON COLUMN security_master.isin IS 'ISO 6166 identifier; n/a for on-chain instruments.';
COMMENT ON COLUMN security_master.cusip IS 'CUSIP identifier.';
COMMENT ON COLUMN security_master.sedol IS 'SEDOL identifier.';
COMMENT ON COLUMN security_master.figi IS 'Bloomberg FIGI.';
COMMENT ON COLUMN security_master.asset_class IS 'FK->asset_class_ref.asset_class. Top classification level.';
COMMENT ON COLUMN security_master.security_type IS 'FK->security_type_ref.(asset_class, security_type).';
COMMENT ON COLUMN security_master.security_subtype IS 'FK->security_subtype_ref.(asset_class, security_type, security_subtype), MATCH SIMPLE (skipped when NULL). Nullable.';
COMMENT ON COLUMN security_master.currency IS 'FK->currency_ref.currency_code. ISO 4217 denomination.';
COMMENT ON COLUMN security_master.country_of_issuance IS 'FK->country_ref.country_code. Where the instrument was issued.';
COMMENT ON COLUMN security_master.country_of_risk IS 'FK->country_ref.country_code. Underlying economic-exposure country.';
COMMENT ON COLUMN security_master.issuer_entity_id IS 'Soft ref to entity_master.entity_id (no FK: SCD2 key is non-unique; resolve via entity_master_current).';
COMMENT ON COLUMN security_master.security_status IS 'ACTIVE / SUSPENDED / MATURED / DELISTED.';
COMMENT ON COLUMN security_master.is_tokenised IS 'TRUE = on-chain tokenised form of a traditional asset.';
COMMENT ON COLUMN security_master.token_standard IS 'ERC-20 / ERC-1400 / ERC-3643 / BENJI / ...; only when is_tokenised.';
COMMENT ON COLUMN security_master.credit_tranche IS 'Structured-credit seniority facet (AAA..B, EQUITY).';
COMMENT ON COLUMN security_master.credit_quality IS 'Credit-quality facet (INVESTMENT_GRADE / HIGH_YIELD / PRIME).';
COMMENT ON COLUMN security_master.collateral_pool IS 'ABS collateral-pool facet (AUTO / CREDIT_CARD / STUDENT_LOAN / CONSUMER).';
COMMENT ON COLUMN security_master.agency_status IS 'Agency facet (AGENCY / NON_AGENCY).';
COMMENT ON COLUMN security_master.backing IS 'Backing facet: securitisation (CASH/SYNTHETIC) or stablecoin (FIAT/CRYPTO/ALGORITHMIC).';
COMMENT ON COLUMN security_master.source_system IS 'System of record.';
COMMENT ON COLUMN security_master.created_at IS 'Audit. Write timestamp.';
COMMENT ON COLUMN security_master.created_by IS 'Audit. User or service that wrote the row.';
COMMENT ON COLUMN security_master.approved_by IS 'Audit. 4-eyes approver.';

-- One version per (security_id, processing_version): the SCD2 dedup key. Two same-day corrections are
-- distinguished by processing_version, and the current view breaks ties on it. Unlike the pipeline
-- state tables (ADR-0002), there is no auto-increment trigger here: this is a curated master whose
-- inserts are deliberate loader operations (VEC-419), so the loader owns monotonic processing_version
-- assignment per security_id. A collision fails hard on this unique index rather than silently merging.
CREATE UNIQUE INDEX IF NOT EXISTS sm_id_version_uidx ON security_master (security_id, processing_version);
-- Current-version lookup (the ORDER BY of security_master_current).
CREATE INDEX IF NOT EXISTS sm_current_idx ON security_master (security_id, valid_from DESC, processing_version DESC);
-- Classification filter index for the risk master (a query shape, not FK support: the reference
-- parents are immutable, so a child-side FK index buys nothing for RI). Also serves lookups on
-- (asset_class) and (asset_class, security_type) as a prefix. The single-column currency /
-- country_of_risk / country_of_issuance indexes were dropped (Simon review #5): they were only ever
-- FK-support, which is pointless against immutable parents, and there is no query consumer yet. Add
-- them back in a one-line migration when a real query shape needs them.
CREATE INDEX IF NOT EXISTS sm_classification_idx ON security_master (asset_class, security_type, security_subtype);

-- Current view: the latest EFFECTIVE version per security_id. Bounded on CURRENT_DATE so a
-- future-dated version (a known maturity or announced status change inserted ahead of time) does
-- NOT become current until its valid_from arrives.
CREATE OR REPLACE VIEW security_master_current AS
SELECT DISTINCT ON (security_id) *
FROM security_master
WHERE valid_from <= CURRENT_DATE
ORDER BY security_id, valid_from DESC, processing_version DESC;

-- Versioned view: derive valid_to (exclusive) and is_current for full-history reads. The validity
-- window is half-open [valid_from, valid_to_exclusive): point-in-time reads use
-- valid_from <= d AND (valid_to_exclusive IS NULL OR d < valid_to_exclusive). is_current applies
-- that predicate at CURRENT_DATE, so a future-dated version is NOT current until effective, and a
-- same-day correction (two versions sharing valid_from) yields a zero-width window on the superseded
-- row so it is never current.
CREATE OR REPLACE VIEW security_master_versions AS
SELECT *,
    (valid_from <= CURRENT_DATE
        AND (valid_to_exclusive IS NULL OR CURRENT_DATE < valid_to_exclusive)) AS is_current
FROM (
    SELECT *,
        lead(valid_from) OVER (PARTITION BY security_id ORDER BY valid_from, processing_version) AS valid_to_exclusive
    FROM security_master
) v;

-- View catalogue metadata (per db/migrations/AGENTS.md).
COMMENT ON VIEW security_master_current IS '[Dimension] Latest effective version per security_id (valid_from <= today). The classification positions resolve to, via security_id.';
COMMENT ON VIEW security_master_versions IS '[Dimension] Full SCD2 history per security_id with derived valid_to_exclusive (half-open [valid_from, valid_to_exclusive)) and is_current (effective as of today).';

-- Reads for both roles; append-only writes for the indexer role (INSERT, never UPDATE/DELETE).
GRANT SELECT ON security_master, security_master_current, security_master_versions TO stl_readonly;
GRANT SELECT, INSERT ON security_master TO stl_readwrite;
GRANT SELECT ON security_master_current, security_master_versions TO stl_readwrite;

-- Append-only guard: revoke mutation on both the indexer role and the table owner so any
-- UPDATE / DELETE / TRUNCATE errors out. Guarded by role existence; mirrors the reference-table guard.
DO $$
DECLARE role text;
BEGIN
    FOREACH role IN ARRAY ARRAY['stl_readwrite','stl_migrator'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role) THEN
            EXECUTE format('REVOKE UPDATE, DELETE, TRUNCATE ON security_master FROM %I', role);
        END IF;
    END LOOP;
END $$;

INSERT INTO migrations (filename) VALUES ('20260713_140000_create_security_master.sql') ON CONFLICT (filename) DO NOTHING;
