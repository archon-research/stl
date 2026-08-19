-- Morpho VaultV2 structured tracking (VEC-218): adapter identity.
--
-- A VaultV2 (morpho-org/vault-v2) never allocates to Morpho Blue directly. It
-- holds a set of liquidity-adapter contracts, each wrapping one downstream
-- venue (a Morpho Blue market or a nested MetaMorpho V1 vault). morpho_adapter
-- is the IDENTITY of those adapters: exactly one row per (morpho_vault_id,
-- address), forever. Whether the adapter is currently in the vault's adapter
-- set, when it entered or left, and how it classifies are OBSERVATIONS, and
-- they live in morpho_adapter_membership (20260721_125000); its per-block realAssets()
-- readings live in the morpho_adapter_state hypertable (20260721_130000).
--
-- Every column here is a genuinely immutable identity fact, so the row is
-- written once and never converged (strict append-only rule, see
-- db/migrations/AGENTS.md). That is what makes the surrogate id stable: no
-- lifecycle decision can move the row a morpho_adapter_state snapshot hangs
-- off, so no snapshot can ever be stranded by one.
--
-- Deliberately NOT here: a first_seen_block / added_at_block column. "First
-- seen" is itself a converging observation, so a written-once column would
-- record whichever writer arrived first rather than the earliest block. It is
-- MIN(block_number) over the membership log, and the honest first-add is
-- MIN(block_number) FILTER (WHERE observed_via = 'add_adapter_event').
--
-- processing_version and build_id are carried for schema uniformity and audit
-- (ADR-0002 §3), not for versioning: an identity row is written once, so there
-- is no (key, version) series to version and both stay 0 in practice. That is
-- also why this table gets NO assign_processing_version_* BEFORE INSERT
-- trigger — the advisory-locked trigger pattern versions repeated observations
-- of one key, and this table has none; UNIQUE (morpho_vault_id, address) makes
-- a re-observation an ON CONFLICT DO NOTHING no-op instead of a new row.

-- ============================================================================
-- morpho_adapter: identity of a VaultV2 liquidity adapter (one row per adapter
-- per vault, forever). morpho_vault_id FKs the parent VaultV2; asset_token_id
-- FKs the vault's underlying asset (the unit of the adapter's realAssets()
-- reading), resolved from the token registry by natural key (chain_id, address).
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_adapter
(
    id                 BIGSERIAL PRIMARY KEY,
    morpho_vault_id    BIGINT NOT NULL REFERENCES morpho_vault (id),
    address            BYTEA  NOT NULL,
    asset_token_id     BIGINT NOT NULL REFERENCES token (id),
    processing_version INT    NOT NULL DEFAULT 0,
    build_id           INT    NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (morpho_vault_id, address)
);

CREATE INDEX IF NOT EXISTS idx_morpho_adapter_asset_token ON morpho_adapter (asset_token_id);

-- ============================================================================
-- Catalogue metadata (source of truth for column units/scale; see the
-- "Interpreting numeric columns" convention). Style matches
-- 20260609_120000_add_schema_comments / 20260626_120000_create_fluid_vault_tables.
-- ============================================================================
COMMENT ON TABLE morpho_adapter IS
  '[Dimension] Identity of a Morpho VaultV2 liquidity adapter: exactly one row per (morpho_vault_id, address), forever. Written once and never updated — whether the adapter is currently in the vault''s adapter set, when it entered or left, and its classification are observations in morpho_adapter_membership, and "the current set" is the morpho_adapter_current view. Its stable id is what morpho_adapter_state rows hang off, so no lifecycle observation can ever strand a snapshot. Append-only: UPDATE and DELETE are revoked from the application role.';
COMMENT ON COLUMN morpho_adapter.id IS 'PK. Stable surrogate id referenced by morpho_adapter_state and morpho_adapter_membership; never reassigned.';
COMMENT ON COLUMN morpho_adapter.morpho_vault_id IS 'FK→morpho_vault.id. The parent VaultV2. Part of the natural key (morpho_vault_id, address).';
COMMENT ON COLUMN morpho_adapter.address IS 'Adapter contract address (20 bytes). Part of the natural key (morpho_vault_id, address).';
COMMENT ON COLUMN morpho_adapter.asset_token_id IS 'FK→token.id. The vault''s underlying asset ERC-20 and the unit of the adapter''s realAssets() reading; invariantly equal to morpho_vault.asset_token_id, denormalised so a state reader needs one join rather than two. Immutable.';
COMMENT ON COLUMN morpho_adapter.processing_version IS 'Correction version: 0=original, N=Nth reprocess. Carried for schema uniformity and audit only — an identity row is written once, so it is 0 in practice, it is NOT part of the natural key UNIQUE (morpho_vault_id, address), and a re-observation reuses the existing row rather than minting a new version. Lifecycle versioning lives on morpho_adapter_membership.';
COMMENT ON COLUMN morpho_adapter.build_id IS 'Audit. Deployment build that wrote the row; never use to pick the latest row.';
COMMENT ON COLUMN morpho_adapter.created_at IS 'Audit. Processing time: wall-clock the identity row was first written (DEFAULT NOW()), per the schema_master canonical semantics; NOT a block time — this row carries none, every block-timed fact about the adapter lives in morpho_adapter_membership. Written once; never part of any key or ordering.';

INSERT INTO migrations (filename)
VALUES ('20260721_120000_create_morpho_adapter.sql')
ON CONFLICT (filename) DO NOTHING;
