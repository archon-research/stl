-- Morpho VaultV2 structured tracking (VEC-218): adapter membership log.
--
-- morpho_adapter (20260721_120000) is pure identity; this table carries every
-- OBSERVATION of whether that adapter is in its vault's adapter set. One row
-- per observation per block position — never a mutable state. "Is it a member
-- now" is the latest row per adapter by
-- (block_number, block_version, log_index, processing_version) DESC, exposed as
-- the morpho_adapter_current view; "which block was it added at" is
-- MIN(block_number) over its is_member rows with
-- observed_via = 'add_adapter_event'.
--
-- Modelling the column as a STATE ASSERTION (is_member) rather than a
-- transition ('added'/'removed') is the crux: an AddAdapter/RemoveAdapter log
-- witnesses a change, but a hash-pinned adapters(i) enumeration (vault
-- discovery, bootstrap seed) and the membership an Allocate log implies only
-- assert the set's CONTENTS at a block. observed_via records which of the two
-- an observation was, so a mid-life discovery states what it actually saw
-- instead of claiming an add it never witnessed.
--
-- Because every observation is its own row at its own (block_number,
-- block_version, log_index), a reorg-relocated or replayed observation needs no
-- convergence, no relocation bound and no orphan guard: the ordering tuple
-- selects, and nothing edits an earlier row. log_index in the key is also what
-- makes an add, a remove and a re-add inside ONE block three distinct
-- observations.
--
-- A plain table, not a hypertable, by deliberate maintainer carve-out (the same
-- carve-out morpho_vault_cap / morpho_vault_fee take): adapter membership
-- changes are governance events writing on the order of rows per day at most,
-- so chunking, compression and S3 tiering buy nothing — and unlike caps and
-- fees this table is on the READ hot path, where a hypertable would fan the
-- latest-row LIMIT 1 lookup across every chunk and hide history behind the
-- tiered-read horizon. Auditability follows ADR-0002: block_version,
-- processing_version + build_id, PK = natural key + processing_version
-- (processing_version LAST for a contiguous PK-index prefix), and a build-aware
-- advisory-locked BEFORE INSERT trigger (prefix: mam).
--
-- The PK deliberately deviates from morpho_vault_cap / morpho_vault_fee by
-- leaving `timestamp` OUT of the key. There it mirrors morpho_adapter_state,
-- where timestamp is the PARTITION column and TimescaleDB therefore requires it
-- in every unique index. This is a plain table, so that requirement does not
-- apply, and the key is kept identical to the latest-row ordering tuple — which
-- is what makes the lookup an index scan backward with no sort.

-- ============================================================================
-- morpho_adapter_membership: append-only observations of adapter-set membership.
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_adapter_membership
(
    morpho_adapter_id  BIGINT      NOT NULL REFERENCES morpho_adapter (id),
    block_number       BIGINT      NOT NULL,
    block_version      INT         NOT NULL DEFAULT 0,
    log_index          INT         NOT NULL,
    timestamp          TIMESTAMPTZ NOT NULL,
    is_member          BOOLEAN     NOT NULL,
    adapter_type       SMALLINT,
    observed_via       TEXT        NOT NULL,
    processing_version INT         NOT NULL DEFAULT 0,
    build_id           INT         NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Only a REMOVAL may be untyped: an adapter first seen by its own removal
    -- has no known classification and NULL is the honest record. Asserting
    -- MEMBERSHIP always carries the probe's answer (99 = probed and
    -- unclassifiable), so the type of a currently-active adapter is read
    -- straight off its latest row.
    CONSTRAINT morpho_adapter_membership_type_required_for_members
        CHECK (adapter_type IS NOT NULL OR NOT is_member),
    -- TEXT + CHECK, never VARCHAR + CHECK: a VARCHAR enum column is what broke
    -- OSM tiering before.
    CONSTRAINT morpho_adapter_membership_observed_via_known
        CHECK (observed_via IN ('add_adapter_event', 'remove_adapter_event',
                                'allocation_event', 'vault_discovery', 'bootstrap_seed')),
    -- processing_version last so the trigger's (morpho_adapter_id, block_number,
    -- block_version, log_index) lookup is a contiguous PK-index prefix
    -- (ADR-0002), and so the PK IS the latest-row ordering tuple.
    PRIMARY KEY (morpho_adapter_id, block_number, block_version, log_index, processing_version)
);

CREATE INDEX IF NOT EXISTS idx_morpho_adapter_membership_block
    ON morpho_adapter_membership (block_number);

-- Build-aware processing-version trigger with advisory lock (ADR-0002 §3).
-- Same (adapter, block, block_version, log_index, build_id) retry → reuse
-- version (idempotent); a new build_id at the same key → MAX+1. Every key
-- component is already an integer, so unlike the mas/mvc/mvf triggers this one
-- needs no EXTRACT(epoch FROM …) stabiliser (timestamp is not in the key).
-- Pinned to force_custom_plan, required of every assign_processing_version_*
-- function whether or not its table is a hypertable (VEC-541).
CREATE OR REPLACE FUNCTION assign_processing_version_morpho_adapter_membership()
RETURNS TRIGGER
SET plan_cache_mode = 'force_custom_plan'
AS $$
DECLARE
    existing_ver INT;
    max_ver      INT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        format('mam|%s|%s|%s|%s', NEW.morpho_adapter_id, NEW.block_number,
               NEW.block_version, NEW.log_index), 0));

    SELECT processing_version INTO existing_ver
    FROM morpho_adapter_membership
    WHERE morpho_adapter_id = NEW.morpho_adapter_id
      AND block_number      = NEW.block_number
      AND block_version     = NEW.block_version
      AND log_index         = NEW.log_index
      AND build_id          = NEW.build_id
    LIMIT 1;

    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM morpho_adapter_membership
        WHERE morpho_adapter_id = NEW.morpho_adapter_id
          AND block_number      = NEW.block_number
          AND block_version     = NEW.block_version
          AND log_index         = NEW.log_index;
        NEW.processing_version := max_ver + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_assign_processing_version ON morpho_adapter_membership;
CREATE TRIGGER trigger_assign_processing_version
    BEFORE INSERT ON morpho_adapter_membership
    FOR EACH ROW
EXECUTE FUNCTION assign_processing_version_morpho_adapter_membership();

-- ============================================================================
-- morpho_adapter_current: the vault's current adapter set, as a query rather
-- than a column. Same LEFT JOIN LATERAL … ORDER BY … LIMIT 1 + WHERE shape as
-- maple_loan_current / maple_sky_strategy_current (20260627_120000). Consumers
-- should read this rather than the log: a hand-rolled query that forgets the
-- ordering gets a wrong answer silently.
-- ============================================================================
CREATE OR REPLACE VIEW morpho_adapter_current AS
SELECT a.id, a.morpho_vault_id, a.address, a.asset_token_id,
       m.adapter_type, m.block_number AS as_of_block, m.block_version AS as_of_block_version,
       m.observed_via
FROM morpho_adapter a
LEFT JOIN LATERAL (
    SELECT is_member, adapter_type, block_number, block_version, observed_via
    FROM morpho_adapter_membership
    WHERE morpho_adapter_id = a.id
    ORDER BY block_number DESC, block_version DESC, log_index DESC, processing_version DESC
    LIMIT 1
) m ON TRUE
WHERE m.is_member;

-- ============================================================================
-- Catalogue metadata.
-- ============================================================================
COMMENT ON TABLE morpho_adapter_membership IS
  '[Configuration] Append-only observations of whether a VaultV2 liquidity adapter is in its vault''s adapter set. Each row is one observation at one block position, never a mutable state: the current set is the latest row per adapter by (block_number, block_version, log_index, processing_version) DESC, exposed as the morpho_adapter_current view; "the block it was added at" is MIN(block_number) over its is_member rows with observed_via = ''add_adapter_event''. A reorg-relocated or replayed observation is simply another row at its own block/version, so no convergence, relocation bound or orphan guard is needed. Plain table, not a hypertable, by deliberate maintainer carve-out: adapter membership changes are governance events writing on the order of rows per day at most, so chunking, compression and tiering buy nothing — and this table is on the read hot path, where a hypertable would fan the latest-row lookup across every chunk and hide history behind the tiered-read horizon. Append-only, ADR-0002 versioned; UPDATE and DELETE are revoked from the application role.';
COMMENT ON COLUMN morpho_adapter_membership.morpho_adapter_id IS 'FK→morpho_adapter.id. Part of PK.';
COMMENT ON COLUMN morpho_adapter_membership.block_number IS 'Block at which the observation was made. Part of PK.';
COMMENT ON COLUMN morpho_adapter_membership.block_version IS 'Block payload version the row was indexed from. Live rows: the reorg version carried by the block event. Replayed rows: the S3 object version, routinely 1 straight from the bulk downloader with no reorg behind it. A higher version is therefore never evidence of a reorg, only the tie-break that picks the canonical row. Part of PK; a new version inserts cleanly rather than overwriting.';
COMMENT ON COLUMN morpho_adapter_membership.log_index IS 'Position of the observation within its block: the emitting log''s index for an event-derived observation; 2147483647 (entity.EndOfBlockLogIndex) for a hash-pinned end-of-block state read (vault_discovery, bootstrap_seed), which is authoritative over every log in the block and therefore orders last. Part of PK — it is what lets an add, a remove and a re-add in ONE block be three distinct observations.';
COMMENT ON COLUMN morpho_adapter_membership.timestamp IS 'Block timestamp (UTC). Attribute, not part of the key: this is a plain table, so unlike morpho_adapter_state the partition column requirement does not apply, and the key is kept identical to the latest-row ordering tuple so the lookup is a backward index scan with no sort.';
COMMENT ON COLUMN morpho_adapter_membership.is_member IS 'Whether the adapter was in the vault''s adapter set as observed at this position. TRUE from an AddAdapter log, an Allocate log (the contract cannot allocate to an unregistered adapter), or a hash-pinned adapters(i) enumeration; FALSE from a RemoveAdapter log.';
COMMENT ON COLUMN morpho_adapter_membership.adapter_type IS 'Classification observed with this row: 1 = MorphoMarketV1AdapterV2 (Morpho Blue market), 2 = MorphoVaultV1Adapter (nested MetaMorpho V1 vault), 99 = probed and unclassifiable, NULL = this observation carried no probe. NOT NULL whenever is_member (CHECK), so the current type of an active adapter is read off its latest row. A better classification is recorded by appending, never by updating.';
COMMENT ON COLUMN morpho_adapter_membership.observed_via IS 'Provenance: add_adapter_event | remove_adapter_event (transitions — always recorded, because they are the evidence of WHEN the set changed) | allocation_event | vault_discovery | bootstrap_seed (assertions — recorded only when they change the answer at this position). Mirrors the observed_via label on morpho_v2_adapter_registrations_total.';
COMMENT ON COLUMN morpho_adapter_membership.processing_version IS 'Correction version: 0=original, N=Nth reprocess. Part of PK; order by block_number DESC, block_version DESC, log_index DESC, processing_version DESC for the latest observation.';
COMMENT ON COLUMN morpho_adapter_membership.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';
COMMENT ON COLUMN morpho_adapter_membership.created_at IS 'Audit. Processing time: wall-clock the row was inserted (DEFAULT NOW()), per the schema_master canonical semantics; NOT the block timestamp (`timestamp`). Never part of any key or latest-row ordering.';

COMMENT ON VIEW morpho_adapter_current IS '[Configuration] The adapters currently in each VaultV2''s adapter set: the latest morpho_adapter_membership observation per adapter, filtered to is_member. What consumers should read instead of the log.';

-- ============================================================================
-- Append-only enforcement: the application role may SELECT and INSERT but never
-- mutate or delete. (Table owner stl_migrator is unaffected, as Postgres owners
-- bypass these grants — that is the operator escape hatch for repairing a bad
-- row. TRUNCATE is never granted by ALTER DEFAULT PRIVILEGES, so it needs no
-- REVOKE.)
-- ============================================================================

REVOKE UPDATE, DELETE ON morpho_adapter_membership FROM stl_readwrite;

INSERT INTO migrations (filename)
VALUES ('20260721_125000_create_morpho_adapter_membership.sql')
ON CONFLICT (filename) DO NOTHING;
