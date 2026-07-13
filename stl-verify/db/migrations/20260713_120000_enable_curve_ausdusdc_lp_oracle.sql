-- Enable the curve_lp_ng oracle for the AUSD/USDC StableSwap-NG pool.
-- 20260709_120000_add_er_missing_price_feeds.sql shipped it disabled because a binary
-- that predates curve_lp_ng errors on the unknown oracle type every block and
-- DLQ-loops the pipeline. That gate is met: staging and prod both run 4e82d2d2
-- (verified 2026-07-13), which dispatches curve_lp_ng in the worker and backfiller.
-- The oracle-price-worker loads units once at startup; the deploy that applies this
-- migration also rolls the pods, so the unit loads on the same rollout.
UPDATE oracle
SET enabled = true
WHERE name = 'curve_ausdusdc_lp' AND oracle_type = 'curve_lp_ng';

DO $$
DECLARE cnt INT;
BEGIN
    SELECT COUNT(*) INTO cnt FROM oracle
    WHERE name = 'curve_ausdusdc_lp' AND oracle_type = 'curve_lp_ng' AND enabled;
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'expected exactly 1 enabled curve_ausdusdc_lp curve_lp_ng oracle, found %', cnt;
    END IF;

    -- The unit builder hard-fails startup on a malformed registry shape; assert the
    -- shape here too so the migration, not the worker rollout, is what fails.
    SELECT COUNT(DISTINCT oa.feed_address) INTO cnt
    FROM oracle_asset oa
    JOIN oracle o ON o.id = oa.oracle_id
    WHERE o.name = 'curve_ausdusdc_lp' AND oa.enabled;
    IF cnt < 2 THEN
        RAISE EXCEPTION 'curve_ausdusdc_lp needs >= 2 enabled coin feeds, found %', cnt;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260713_120000_enable_curve_ausdusdc_lp_oracle.sql')
ON CONFLICT (filename) DO NOTHING;
