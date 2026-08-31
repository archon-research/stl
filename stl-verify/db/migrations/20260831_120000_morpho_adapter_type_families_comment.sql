-- Re-issue morpho_adapter_membership.adapter_type's catalogue comment for the
-- three VaultV2 adapter families the classifier now models (3 ERC4626Merkl,
-- 4 Box, 5 CompoundV3). Comment only: the column, its CHECK and its grants are
-- untouched, and the creating migration (20260721_125000) is immutable.

COMMENT ON COLUMN morpho_adapter_membership.adapter_type IS 'Classification observed with this row: 1 = MorphoMarketV1AdapterV2 (Morpho Blue market), 2 = MorphoVaultV1Adapter (nested MetaMorpho V1 vault), 3 = ERC4626MerklAdapter (external ERC-4626 vault, rewards claimable via Merkl), 4 = BoxAdapter (Morpho Box), 5 = CompoundV3Adapter (Compound V3 Comet), 99 = probed and unclassifiable, NULL = this observation carried no probe. 99 is a legitimate steady-state value: bespoke curator-written adapters share no marker getter with any family, so no probe can classify them. NOT NULL whenever is_member (CHECK), so the current type of an active adapter is read off its latest row. A better classification is recorded by appending, never by updating.';

INSERT INTO migrations (filename)
VALUES ('20260831_120000_morpho_adapter_type_families_comment.sql')
ON CONFLICT (filename) DO NOTHING;
