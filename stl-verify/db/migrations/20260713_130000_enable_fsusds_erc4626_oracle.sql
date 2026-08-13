-- Enable the erc4626_share oracle for fsUSDS (Fluid Savings USDS, VEC-435).
-- 20260626_120000_add_fsusds_erc4626_oracle.sql shipped it disabled because a binary
-- that predates erc4626_share errors on the unknown oracle type every block and
-- DLQ-loops the pipeline. That gate is long met: the erc4626_share dispatch merged
-- 2026-07-02 (a3273540, PR #491), and every build writing onchain_token_price rows
-- on 2026-07-13 (2ef803f7, 4e82d2d2, 594a03c3, 7ae7d2e1) contains it.
-- Cast-verified on mainnet 2026-07-13 (block 25523949):
--   vault 0x2BBE31d63E6813E3AC858C04dae43FB2a72B0D11: decimals() = 18,
--   asset() = sUSDS (0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD),
--   convertToAssets(1e18) = 1002057025096000000 (1.002057 sUSDS/share);
--   Chronicle sUSDS/USD feed 0x496470F4835186bF118545Bd76889F123D608E84:
--   decimals() = 18, latestRoundData answer = 1101850242953761113 (1.10185 USD,
--   updated 3.7h before the check); implied fsUSDS price 1.1041 USD.
-- The oracle-price-worker loads units once at startup; the deploy that applies this
-- migration also rolls the pods, so the unit loads on the same rollout.
-- Fluid owner (VEC-435): please confirm before merge.
UPDATE oracle
SET enabled = true
WHERE name = 'fluid_fsusds' AND oracle_type = 'erc4626_share';

DO $$
DECLARE
    enabled_oracles INT;
    enabled_assets INT;
    well_formed_assets INT;
BEGIN
    SELECT COUNT(*) INTO enabled_oracles FROM oracle
    WHERE name = 'fluid_fsusds' AND oracle_type = 'erc4626_share' AND enabled;
    IF enabled_oracles <> 1 THEN
        RAISE EXCEPTION 'expected exactly 1 enabled fluid_fsusds erc4626_share oracle, found %', enabled_oracles;
    END IF;

    -- buildERC4626Unit hard-fails worker startup on a malformed registry shape
    -- (zero feed_address, feed_decimals <= 0, non-USD quote, zero token decimals);
    -- assert that shape here so the migration, not the worker rollout, is what fails.
    SELECT
        COUNT(*),
        COUNT(*) FILTER (
            WHERE oa.feed_address IS NOT NULL
              AND oa.feed_address <> '\x0000000000000000000000000000000000000000'::bytea
              AND oa.feed_decimals > 0
              AND oa.quote_currency = 'USD'
              AND t.decimals > 0
        )
    INTO enabled_assets, well_formed_assets
    FROM oracle_asset oa
    JOIN oracle o ON o.id = oa.oracle_id
    JOIN token t ON t.id = oa.token_id
    WHERE o.name = 'fluid_fsusds' AND oa.enabled;
    IF enabled_assets <> 1 OR well_formed_assets <> 1 THEN
        RAISE EXCEPTION 'expected exactly 1 well-formed enabled fluid_fsusds oracle_asset, found % enabled (% well-formed)', enabled_assets, well_formed_assets;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260713_130000_enable_fsusds_erc4626_oracle.sql')
ON CONFLICT (filename) DO NOTHING;
