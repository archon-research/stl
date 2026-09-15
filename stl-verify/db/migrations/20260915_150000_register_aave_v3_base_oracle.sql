-- Register the Aave V3 Base AaveOracle (chain 8453) so aBasUSDC receipt tokens
-- are priced via the protocol's own oracle. The existing chainlink_base binding
-- (from 20260909_130000) stays as an extra row; the receipt lateral joins any
-- bound oracle and takes newest by block then oracle_id DESC, so aave_v3_base
-- is preferred while it writes newer rows, and chainlink_base takes over if
-- the Aave unit skips a block (it reverts wholesale when any asset is unpriceable).
-- Both are ~$1, so harmless.
--
-- AaveOracle address from the aave-address-book (AaveV3Base.sol):
--   0x2Cc0Fc26eD4563A5ce5e8bdcfe1A2878676Ae156
-- Pool address (protocol natural key): 0xA238Dd80C259a72e81d7e4664a9801593F98d1c5
-- Pool created at block 2357134 (protocols.go).
-- USDC on Base: 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913 (6 decimals)
-- aBasUSDC receipt token already exists (20260909_130000).
-- On-chain verification (BASE_CURRENCY_UNIT, getAssetsPrices) pending per PR checklist.

INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, price_decimals, enabled)
VALUES ('aave_v3_base', 'Aave V3 Base', 8453,
        '\x2Cc0Fc26eD4563A5ce5e8bdcfe1A2878676Ae156'::bytea,
        'aave_oracle', 2357134, 8, true)
ON CONFLICT (name) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, quote_currency, processing_version, change_reason)
SELECT o.id, t.id, true, 'USD', 0, 'VEC-519: initial Aave V3 Base oracle registration'
FROM oracle o
JOIN token t ON t.chain_id = 8453 AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea
WHERE o.name = 'aave_v3_base'
ON CONFLICT (oracle_id, token_id, feed_key, processing_version) DO NOTHING;

INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
SELECT p.id, o.id, 2357134
FROM protocol p, oracle o
WHERE p.chain_id = 8453 AND p.address = '\xA238Dd80C259a72e81d7e4664a9801593F98d1c5'::bytea
  AND o.name = 'aave_v3_base'
ON CONFLICT (protocol_id, oracle_id, from_block) DO NOTHING;

DO $$
DECLARE cnt INT;
BEGIN
    SELECT COUNT(*) INTO cnt
    FROM oracle o
    WHERE o.name = 'aave_v3_base' AND o.chain_id = 8453 AND o.enabled
      AND o.oracle_type = 'aave_oracle';
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'aave_v3_base oracle not found or misconfigured';
    END IF;

    SELECT COUNT(*) INTO cnt
    FROM oracle_asset oa
    JOIN oracle o ON o.id = oa.oracle_id AND o.name = 'aave_v3_base'
    JOIN token t ON t.id = oa.token_id AND t.chain_id = 8453
                AND t.address = '\x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'::bytea
    WHERE oa.enabled AND oa.quote_currency = 'USD' AND oa.feed_address IS NULL;
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'expected 1 enabled aave_v3_base USDC oracle_asset row, found %', cnt;
    END IF;

    SELECT COUNT(*) INTO cnt
    FROM protocol_oracle po
    JOIN protocol p ON p.id = po.protocol_id AND p.chain_id = 8453
                    AND p.address = '\xA238Dd80C259a72e81d7e4664a9801593F98d1c5'::bytea
    JOIN oracle o ON o.id = po.oracle_id AND o.name = 'aave_v3_base';
    IF cnt < 1 THEN
        RAISE EXCEPTION 'Aave V3 Base -> aave_v3_base protocol_oracle binding missing';
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260915_150000_register_aave_v3_base_oracle.sql')
ON CONFLICT (filename) DO NOTHING;
