-- Register the Aave V3 Arbitrum AaveOracle (chain 42161) so aArbUSDCn receipt
-- tokens are priced via the protocol's own oracle rather than a generic feed.
--
-- AaveOracle address from the aave-address-book (AaveV3Arbitrum.sol):
--   0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7
-- Pool address (protocol natural key): 0x794a61358D6845594F94dc1DB02A252b5b4814aD
-- PoolAddressesProvider: 0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb
-- Pool created at block 7742429 (protocols.go).
-- Only USDC is an Aave V3 Arbitrum reserve that we hold; USDS/sUSDS are not
-- reserves (getSourceOfAsset returns 0x0), so they must NOT be added to this
-- oracle unit (an aave unit reverts wholesale if any asset is unpriceable).
-- aArbUSDCn receipt token: 0x724dc807b04555b71ed48a6896b6f41593b8c637
-- On-chain verification (BASE_CURRENCY_UNIT, getAssetsPrices, getSourceOfAsset)
-- pending per the PR checklist.

INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, price_decimals, enabled)
VALUES ('aave_v3_arbitrum', 'Aave V3 Arbitrum', 42161,
        '\xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7'::bytea,
        'aave_oracle', 7742429, 8, true)
ON CONFLICT (name) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, quote_currency, processing_version, change_reason)
SELECT o.id, t.id, true, 'USD', 0, 'VEC-519: initial Aave V3 Arbitrum oracle registration'
FROM oracle o
JOIN token t ON t.chain_id = 42161 AND t.address = '\xaf88d065e77c8cc2239327c5edb3a432268e5831'::bytea
WHERE o.name = 'aave_v3_arbitrum'
ON CONFLICT (oracle_id, token_id, feed_key, processing_version) DO NOTHING;

INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
SELECT p.id, o.id, 7742429
FROM protocol p, oracle o
WHERE p.chain_id = 42161 AND p.address = '\x794a61358D6845594F94dc1DB02A252b5b4814aD'::bytea
  AND o.name = 'aave_v3_arbitrum'
ON CONFLICT (protocol_id, oracle_id, from_block) DO NOTHING;

INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol, created_at_block)
SELECT 42161, p.id, t.id, '\x724dc807b04555b71ed48a6896b6f41593b8c637'::bytea, 'aArbUSDCn', 7742429
FROM protocol p, token t
WHERE p.chain_id = 42161 AND p.address = '\x794a61358D6845594F94dc1DB02A252b5b4814aD'::bytea
  AND t.chain_id = 42161 AND t.address = '\xaf88d065e77c8cc2239327c5edb3a432268e5831'::bytea
ON CONFLICT (chain_id, receipt_token_address) DO NOTHING;

DO $$
DECLARE cnt INT;
BEGIN
    SELECT COUNT(*) INTO cnt
    FROM oracle o
    WHERE o.name = 'aave_v3_arbitrum' AND o.chain_id = 42161 AND o.enabled
      AND o.oracle_type = 'aave_oracle';
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'aave_v3_arbitrum oracle not found or misconfigured';
    END IF;

    SELECT COUNT(*) INTO cnt
    FROM oracle_asset oa
    JOIN oracle o ON o.id = oa.oracle_id AND o.name = 'aave_v3_arbitrum'
    JOIN token t ON t.id = oa.token_id AND t.chain_id = 42161
                AND t.address = '\xaf88d065e77c8cc2239327c5edb3a432268e5831'::bytea
    WHERE oa.enabled AND oa.quote_currency = 'USD' AND oa.feed_address IS NULL;
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'expected 1 enabled aave_v3_arbitrum USDC oracle_asset row, found %', cnt;
    END IF;

    SELECT COUNT(*) INTO cnt
    FROM protocol_oracle po
    JOIN protocol p ON p.id = po.protocol_id AND p.chain_id = 42161
                    AND p.address = '\x794a61358D6845594F94dc1DB02A252b5b4814aD'::bytea
    JOIN oracle o ON o.id = po.oracle_id AND o.name = 'aave_v3_arbitrum';
    IF cnt < 1 THEN
        RAISE EXCEPTION 'Aave V3 Arbitrum -> aave_v3_arbitrum protocol_oracle binding missing';
    END IF;

    SELECT COUNT(*) INTO cnt
    FROM receipt_token rt
    JOIN protocol p ON p.id = rt.protocol_id AND p.chain_id = 42161
                    AND p.address = '\x794a61358D6845594F94dc1DB02A252b5b4814aD'::bytea
    JOIN token t ON t.id = rt.underlying_token_id AND t.chain_id = 42161
                AND t.address = '\xaf88d065e77c8cc2239327c5edb3a432268e5831'::bytea
    WHERE rt.chain_id = 42161
      AND rt.receipt_token_address = '\x724dc807b04555b71ed48a6896b6f41593b8c637'::bytea;
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'expected 1 aArbUSDCn receipt_token row, found %', cnt;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260915_140000_register_aave_v3_arbitrum_oracle.sql')
ON CONFLICT (filename) DO NOTHING;
