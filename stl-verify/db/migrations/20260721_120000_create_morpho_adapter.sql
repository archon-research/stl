-- Morpho VaultV2 structured tracking, part 1 of 3 (VEC-218): adapter registry.
--
-- A VaultV2 (morpho-org/vault-v2) never allocates to Morpho Blue directly. It
-- holds a set of liquidity-adapter contracts, each wrapping one downstream
-- venue (a Morpho Blue market or a nested MetaMorpho V1 vault). morpho_adapter
-- is the registry of those adapters, one row per adapter per vault; its
-- per-block realAssets() readings live in the morpho_adapter_state hypertable
-- (part 2). This is a Dimension/registry table like morpho_vault / fluid_vault:
-- no hypertable, no processing_version.
--
-- Only (morpho_vault_id, address) is immutable per row. The lifetime bounds and
-- the classification are CONVERGING OBSERVATIONS, not values fixed at insert:
-- added_at_block and removed_at_block each converge to the earliest block we ever
-- observed (LEAST) as backfilled history and relocated (reorged) transactions
-- arrive, and adapter_type is upgraded from the Unknown sentinel the first time a
-- replay classifies the adapter. None of them is a time-varying on-chain VALUE
-- needing an audit trail (see the full-auditability rule in
-- db/migrations/AGENTS.md) — they are our best estimate of when ONE on-chain
-- lifetime started and ended, which only ever gets more accurate, so they stay
-- in-place columns.
--
-- added_at_block / removed_at_block bound each adapter's active lifetime
-- (removed_at_block NULL = active). A removed-then-re-added adapter is a new
-- row, so added_at_block is part of the UNIQUE key. added_at_block is the block
-- at which we first observed the adapter on-chain; for an adapter that predates
-- vault discovery it starts at the discovery / first-allocation block and
-- converges (LEAST) to the true AddAdapter block once the backfiller replays
-- history, so the active-row upsert never leaves a duplicate active incarnation.

-- ============================================================================
-- morpho_adapter: registry of VaultV2 liquidity adapters (one row per adapter).
-- morpho_vault_id FKs the parent VaultV2; asset_token_id FKs the vault's
-- underlying asset (the unit of the adapter's realAssets() reading), resolved
-- from the token registry by natural key (chain_id, address).
-- ============================================================================
CREATE TABLE IF NOT EXISTS morpho_adapter
(
    id              BIGSERIAL PRIMARY KEY,
    morpho_vault_id BIGINT   NOT NULL REFERENCES morpho_vault (id),
    address         BYTEA    NOT NULL,
    asset_token_id  BIGINT   NOT NULL REFERENCES token (id),
    adapter_type    SMALLINT NOT NULL,
    added_at_block  BIGINT   NOT NULL,
    removed_at_block BIGINT,
    UNIQUE (morpho_vault_id, address, added_at_block)
);

-- At most ONE active incarnation per (vault, adapter), enforced by the database.
-- Registrations and removals both serialize on a pg_advisory_xact_lock over
-- (vault, address), so this index is the structural backstop rather than the
-- primary guard: it aborts any writer that reaches the table without that lock,
-- instead of letting a de-registered adapter resurrect as a second ACTIVE row that
-- GetActiveAdaptersByVault would feed into realAssets forever. It also serves the
-- hot lookup (a vault's currently-active adapters on every allocation snapshot)
-- via its leading morpho_vault_id column.
CREATE UNIQUE INDEX IF NOT EXISTS uq_morpho_adapter_active
    ON morpho_adapter (morpho_vault_id, address) WHERE removed_at_block IS NULL;
CREATE INDEX IF NOT EXISTS idx_morpho_adapter_asset_token ON morpho_adapter (asset_token_id);

-- ============================================================================
-- Catalogue metadata (source of truth for column units/scale; see the
-- "Interpreting numeric columns" convention). Style matches
-- 20260609_120000_add_schema_comments / 20260626_120000_create_fluid_vault_tables.
-- ============================================================================
COMMENT ON TABLE morpho_adapter IS
  '[Dimension] Registry of Morpho VaultV2 liquidity adapters, one row per adapter per vault. Each adapter wraps one downstream venue; per-block realAssets() readings live in morpho_adapter_state. Only (morpho_vault_id, address) is immutable per row: added_at_block and removed_at_block converge to the earliest observed block (LEAST) as backfilled history and reorg-relocated transactions arrive, and adapter_type is curated once from the Unknown sentinel. Removed adapters keep their row; re-adds are new rows.';
COMMENT ON COLUMN morpho_adapter.id IS 'PK. Surrogate id referenced by morpho_adapter_state.';
COMMENT ON COLUMN morpho_adapter.morpho_vault_id IS 'FK→morpho_vault.id. The parent VaultV2 that registered this adapter.';
COMMENT ON COLUMN morpho_adapter.address IS 'Adapter contract address (20 bytes). Unique per (morpho_vault_id, address, added_at_block).';
COMMENT ON COLUMN morpho_adapter.asset_token_id IS 'FK→token.id. The vault''s underlying asset ERC-20; the unit of the adapter''s realAssets() reading.';
COMMENT ON COLUMN morpho_adapter.adapter_type IS 'Adapter kind: 1 = MorphoMarketV1AdapterV2 (Morpho Blue market), 2 = MorphoVaultV1Adapter (nested MetaMorpho V1 vault), 99 = Unknown (the on-chain type probe could not classify it). Curated by replay: a row recorded as 99 is upgraded the first time an observation supplies a real type; a real type is never overwritten.';
COMMENT ON COLUMN morpho_adapter.added_at_block IS 'Block at which the adapter was first observed on-chain by us: the AddAdapter block when witnessed live or replayed; the vault-discovery or first-allocation block for adapters predating discovery; converges (LEAST) to the true AddAdapter block once history is replayed. Part of the UNIQUE key so a re-added adapter is a distinct row.';
COMMENT ON COLUMN morpho_adapter.removed_at_block IS 'Block at which the adapter was de-registered; NULL while the adapter is active. Converges (LEAST) to the earliest observed removal block, so a reorg that re-lands the RemoveAdapter transaction in a neighbouring block settles deterministically regardless of arrival order.';

INSERT INTO migrations (filename)
VALUES ('20260721_120000_create_morpho_adapter.sql')
ON CONFLICT (filename) DO NOTHING;
