-- Morpho VaultV2 structured tracking, part 1 of 3 (VEC-218): adapter registry +
-- vault fee columns.
--
-- A VaultV2 (morpho-org/vault-v2) never allocates to Morpho Blue directly. It
-- holds a set of liquidity-adapter contracts, each wrapping one downstream
-- venue (a Morpho Blue market or a nested MetaMorpho V1 vault). morpho_adapter
-- is the registry of those adapters, one row per adapter per vault; its
-- per-block realAssets() readings live in the morpho_adapter_state hypertable
-- (part 2). This is a Dimension/registry table like morpho_vault / fluid_vault:
-- no hypertable, no processing_version. The identity fields (morpho_vault_id,
-- address, asset_token_id, adapter_type, added_at_block) are immutable per row;
-- removed_at_block is the one mutable column, set once when the adapter is
-- de-registered.
--
-- added_at_block / removed_at_block bound each adapter's active lifetime
-- (removed_at_block NULL = active). A removed-then-re-added adapter is a new
-- row, so added_at_block is part of the UNIQUE key.
--
-- This migration also adds the VaultV2 fee configuration to morpho_vault. These
-- columns are NULL on V1/V1.1 rows (MetaMorpho has no such config) and are
-- mutated in place on the vault's Set* fee events; the full change history is
-- preserved in protocol_event. Fees are raw on-chain uint96 WAD values, NOT
-- basis points: performanceFee is a WAD fraction of accrued interest
-- (1e18 = 100%), managementFee is a WAD per-second rate.

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

-- Active-adapters lookup: the hot query enumerates a vault's currently-active
-- adapters (removed_at_block IS NULL) on every allocation snapshot.
CREATE INDEX IF NOT EXISTS idx_morpho_adapter_active
    ON morpho_adapter (morpho_vault_id) WHERE removed_at_block IS NULL;
CREATE INDEX IF NOT EXISTS idx_morpho_adapter_asset_token ON morpho_adapter (asset_token_id);

-- ============================================================================
-- morpho_vault fee configuration (VaultV2 only; NULL on MetaMorpho V1/V1.1).
-- ============================================================================
ALTER TABLE morpho_vault
    ADD COLUMN IF NOT EXISTS performance_fee           NUMERIC(30, 0),
    ADD COLUMN IF NOT EXISTS management_fee            NUMERIC(30, 0),
    ADD COLUMN IF NOT EXISTS performance_fee_recipient BYTEA,
    ADD COLUMN IF NOT EXISTS management_fee_recipient  BYTEA;

-- ============================================================================
-- Catalogue metadata (source of truth for column units/scale; see the
-- "Interpreting numeric columns" convention). Style matches
-- 20260609_120000_add_schema_comments / 20260626_120000_create_fluid_vault_tables.
-- ============================================================================
COMMENT ON TABLE morpho_adapter IS
  '[Dimension] Registry of Morpho VaultV2 liquidity adapters, one row per adapter per vault. Each adapter wraps one downstream venue; per-block realAssets() readings live in morpho_adapter_state. Identity fields are immutable per row; removed_at_block is set once on de-registration (removed adapters keep their row; re-adds are new rows).';
COMMENT ON COLUMN morpho_adapter.id IS 'PK. Surrogate id referenced by morpho_adapter_state.';
COMMENT ON COLUMN morpho_adapter.morpho_vault_id IS 'FK→morpho_vault.id. The parent VaultV2 that registered this adapter.';
COMMENT ON COLUMN morpho_adapter.address IS 'Adapter contract address (20 bytes). Unique per (morpho_vault_id, address, added_at_block).';
COMMENT ON COLUMN morpho_adapter.asset_token_id IS 'FK→token.id. The vault''s underlying asset ERC-20; the unit of the adapter''s realAssets() reading.';
COMMENT ON COLUMN morpho_adapter.adapter_type IS 'Adapter kind: 1 = MorphoMarketV1AdapterV2 (Morpho Blue market), 2 = MorphoVaultV1Adapter (nested MetaMorpho V1 vault), 99 = Unknown (unrecognised type, recorded for later curation).';
COMMENT ON COLUMN morpho_adapter.added_at_block IS 'Block at which the adapter was registered on the vault (first-seen). Part of the UNIQUE key so a re-added adapter is a distinct row.';
COMMENT ON COLUMN morpho_adapter.removed_at_block IS 'Block at which the adapter was de-registered; NULL while the adapter is active.';

COMMENT ON COLUMN morpho_vault.performance_fee IS 'VaultV2 performance fee: raw on-chain uint96 WAD fraction of accrued interest (1e18 = 100%). NULL on MetaMorpho V1/V1.1 rows (no such config). Mutated in place on Set* fee events; full change history lives in protocol_event.';
COMMENT ON COLUMN morpho_vault.management_fee IS 'VaultV2 management fee: raw on-chain uint96 WAD per-second rate (not bps, not annualised). NULL on MetaMorpho V1/V1.1 rows. Mutated in place on Set* fee events; full change history lives in protocol_event.';
COMMENT ON COLUMN morpho_vault.performance_fee_recipient IS 'VaultV2 performance-fee recipient address (20 bytes). NULL on MetaMorpho V1/V1.1 rows. Mutated in place on Set* events; history in protocol_event.';
COMMENT ON COLUMN morpho_vault.management_fee_recipient IS 'VaultV2 management-fee recipient address (20 bytes). NULL on MetaMorpho V1/V1.1 rows. Mutated in place on Set* events; history in protocol_event.';

INSERT INTO migrations (filename)
VALUES ('20260721_120000_create_morpho_adapter.sql')
ON CONFLICT (filename) DO NOTHING;
