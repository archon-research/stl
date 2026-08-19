-- PSM3 share legs: the prime's stake in the pool, alongside the reserve columns.
--
-- PSM3 is also an LP pool with an internal, non-transferable share mapping. The
-- reserve columns say how big the pool is; these three say how much of it the
-- prime's ALM proxy owns and what that stake is worth at par.
--
-- Nullable because the table is append-only: rows written before this migration
-- have no share readings and are never backfilled in place.

ALTER TABLE psm3_reserves
    ADD COLUMN alm_shares      NUMERIC,   -- raw 1e18, PSM3.shares(alm)
    ADD COLUMN total_shares    NUMERIC,   -- raw 1e18, PSM3.totalShares()
    ADD COLUMN alm_asset_value NUMERIC;   -- raw 1e18, PSM3.convertToAssetValue(alm_shares)

COMMENT ON COLUMN psm3_reserves.alm_shares IS
  'PSM3.shares(alm) — the internal LP shares held by the prime''s ALM proxy, the only meaningful depositor. Raw on-chain integer at 1e18: shares are minted in the contract''s own 18-decimal USD value unit (the 1 USDC genesis deposit mints 1e18 shares), and the value per share drifts up from there, so this is NOT a token amount and must not be scaled by any token.decimals. Shares are internal accounting only — no share token, no transfers, no ERC-4626 surface — so the holder cannot be discovered from chain state; the ALM proxy address is pinned per chain in the indexer config and cross-checked against axis-synome at startup, and is not stored in this table. Use alm_shares / total_shares for the ownership fraction and alm_asset_value for the USD figure. NULL on rows written before the share legs were indexed.';
COMMENT ON COLUMN psm3_reserves.total_shares IS
  'PSM3.totalShares() — total LP shares outstanding. Raw on-chain integer at 1e18, same unit as alm_shares. Never zero on a live deployment: the deploy script seeds a first deposit to address(0), permanently locking about 1e18 share units, so alm_shares / total_shares is slightly below 1 rather than exactly 1. NULL on rows written before the share legs were indexed.';
COMMENT ON COLUMN psm3_reserves.alm_asset_value IS
  'PSM3.convertToAssetValue(alm_shares) — the ALM proxy''s stake valued at par, read on-chain rather than derived. Raw on-chain integer, 18-decimal USD, the same par scale as total_assets (not a market-priced value). Equals alm_shares x total_assets / total_shares with the contract''s truncating division, so recomputing it off-chain can differ in the last units; prefer this column. NULL on rows written before the share legs were indexed.';

INSERT INTO migrations (filename)
VALUES ('20260819_110000_psm3_reserves_share_legs.sql')
ON CONFLICT (filename) DO NOTHING;
