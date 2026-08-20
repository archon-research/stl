-- PSM3 share legs: Spark's stake in the pool, alongside the reserve columns.
--
-- PSM3 is also an LP pool with an internal, non-transferable share mapping. The
-- reserve columns say how big the pool is; these say how much of it
-- Spark's ALM proxy owns and what that stake is worth at par. The holder
-- columns are prefixed with the prime's name so another prime depositing later
-- gets its own {prime}_alm_shares / {prime}_alm_asset_value pair.
--
-- Nullable because the table is append-only: rows written before this migration
-- have no share readings and are never backfilled in place.

ALTER TABLE psm3_reserves
    ADD COLUMN spark_alm_address     BYTEA,     -- holder the share legs were read for
    ADD COLUMN spark_alm_shares      NUMERIC,   -- raw 1e18, PSM3.shares(spark_alm_address)
    ADD COLUMN total_shares          NUMERIC,   -- raw 1e18, PSM3.totalShares()
    ADD COLUMN spark_alm_asset_value NUMERIC;   -- raw 1e18, PSM3.convertToAssetValue(spark_alm_shares)

COMMENT ON COLUMN psm3_reserves.spark_alm_address IS
  'The Spark ALM proxy address whose share legs this row records. Config-sourced (pinned per chain, cross-checked against axis-synome at startup), so after a proxy rotation rows on either side name their holder and a step change in spark_alm_shares stays distinguishable from a real divestment. Shares are an internal mapping with no share token, so the holder is not recoverable from a later state read; holder history is reconstructable only by replaying Deposit/Withdraw logs. NULL on rows written before the share legs were indexed.';
COMMENT ON COLUMN psm3_reserves.spark_alm_shares IS
  'PSM3.shares(spark_alm_address) — the internal LP shares held by the configured Spark ALM proxy. deposit() is permissionless, so other depositors are possible and push spark_alm_shares / total_shares further below 1. Raw on-chain integer at 1e18: shares are minted in the contract''s own 18-decimal USD value unit (the 1 USDC genesis deposit mints 1e18 shares), and the value per share drifts up from there, so this is NOT a token amount and must not be scaled by any token.decimals. Shares are internal accounting only — no share token, no transfers, no ERC-4626 surface. Use spark_alm_shares / total_shares for the ownership fraction and spark_alm_asset_value for the USD figure. NULL on rows written before the share legs were indexed.';
COMMENT ON COLUMN psm3_reserves.total_shares IS
  'PSM3.totalShares() — total LP shares outstanding. Raw on-chain integer at 1e18, same unit as spark_alm_shares. Never zero on a live deployment: the deploy script seeds a first deposit to address(0), permanently locking about 1e18 share units, so spark_alm_shares / total_shares is slightly below 1 rather than exactly 1. NULL on rows written before the share legs were indexed.';
COMMENT ON COLUMN psm3_reserves.spark_alm_asset_value IS
  'PSM3.convertToAssetValue(spark_alm_shares) — the ALM proxy''s stake valued at par. Raw on-chain integer, 18-decimal USD, the same par scale as total_assets (not a market-priced value). Equals floor(spark_alm_shares x total_assets / total_shares) exactly in integer math over this row''s columns — derivable by construction, stored for auditability: a nonzero residual indicates a decode/ordering bug, not rounding. NULL on rows written before the share legs were indexed.';

INSERT INTO migrations (filename)
VALUES ('20260819_110000_psm3_reserves_share_legs.sql')
ON CONFLICT (filename) DO NOTHING;
