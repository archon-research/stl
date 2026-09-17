-- Register Chainlink price feeds for Robinhood (chain 4663): USDG/USD.
-- Unlocks ~$15M of unpriced USDG held directly on Robinhood, plus groveUSDG
-- (~$102k) via the receipt-token path once USDG is priced.
--
-- Feed address from the Chainlink reference-data-directory for Robinhood, verified
-- on-chain at block 65178110 (description/decimals/latestRoundData):
--   USDG/USD  proxy 0x61B7e5650328764B076A108EFF5fa7282a1B9aD2  "USDG / USD", 8dp, deployed at 33322
-- deployment_block is that feed, as chainlink/chainlink_base carry.
--
-- groveUSDG is a Morpho Vault V2 (curator Steakhouse) on Robinhood. It is priced
-- through the receipt-token path: the tracker writes underlying_value in USDG
-- units via convertToAssets, and the receipt path multiplies by the USDG/USD
-- price from this oracle. No Go code change needed.
-- Verified on-chain at block 65178110: USDG 0x5fc5360d... symbol USDG, 6dp, deployed at 57;
-- groveUSDG receipt token 0xbEeFF039907422219FB367E525954ddC092854D9 asset() = USDG, 18dp,
-- convertToAssets(1e18) = 1001191, deployed at 47860; Morpho Blue on Robinhood
-- 0x9D53d5E3bd5E8d4Cbfa6DB1ca238AEA02E651010 deployed at 286; Multicall3 present from block 0.

INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    (4663, '\x5fc5360d0400a0fd4f2af552add042d716f1d168'::bytea, 'USDG', 6)
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, price_decimals, enabled)
VALUES ('chainlink_robinhood', 'Chainlink (Robinhood)', 4663, NULL, 'chainlink_feed', 0, 8, true)
ON CONFLICT (name) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency, processing_version, change_reason)
SELECT o.id, t.id, true, '\x61B7e5650328764B076A108EFF5fa7282a1B9aD2'::bytea, 8, 'USD', 0, 'VEC-519: initial Robinhood feed registration'
FROM oracle o
JOIN token t ON t.chain_id = 4663 AND t.address = '\x5fc5360d0400a0fd4f2af552add042d716f1d168'::bytea
WHERE o.name = 'chainlink_robinhood'
ON CONFLICT (oracle_id, token_id, feed_key, processing_version) DO NOTHING;

INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
VALUES (4663, '\x9D53d5E3bd5E8d4Cbfa6DB1ca238AEA02E651010'::bytea,
        'Morpho Blue', 'lending', 286, NOW(), '{}'::jsonb)
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
SELECT p.id, o.id, 286
FROM protocol p, oracle o
WHERE p.chain_id = 4663 AND p.address = '\x9D53d5E3bd5E8d4Cbfa6DB1ca238AEA02E651010'::bytea
  AND o.name = 'chainlink_robinhood'
ON CONFLICT (protocol_id, oracle_id, from_block) DO NOTHING;

INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol, created_at_block)
SELECT 4663, p.id, t.id, '\xbEeFF039907422219FB367E525954ddC092854D9'::bytea, 'groveUSDG', 47860
FROM protocol p, token t
WHERE p.chain_id = 4663 AND p.address = '\x9D53d5E3bd5E8d4Cbfa6DB1ca238AEA02E651010'::bytea
  AND t.chain_id = 4663 AND t.address = '\x5fc5360d0400a0fd4f2af552add042d716f1d168'::bytea
ON CONFLICT (chain_id, receipt_token_address) DO NOTHING;

DO $$
DECLARE cnt INT;
BEGIN
    SELECT COUNT(*) INTO cnt
    FROM oracle o
    JOIN token t ON t.chain_id = 4663 AND t.address = '\x5fc5360d0400a0fd4f2af552add042d716f1d168'::bytea
    JOIN oracle_asset oa ON oa.oracle_id = o.id AND oa.token_id = t.id
    WHERE o.name = 'chainlink_robinhood' AND o.enabled AND o.chain_id = 4663
      AND oa.enabled AND oa.feed_decimals = 8 AND oa.quote_currency = 'USD'
      AND oa.feed_address = '\x61B7e5650328764B076A108EFF5fa7282a1B9aD2'::bytea;
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'expected 1 enabled chainlink_robinhood USDG oracle_asset row, found %', cnt;
    END IF;

    SELECT COUNT(*) INTO cnt
    FROM protocol_oracle po
    JOIN protocol p ON p.id = po.protocol_id AND p.chain_id = 4663
                    AND p.address = '\x9D53d5E3bd5E8d4Cbfa6DB1ca238AEA02E651010'::bytea
    JOIN oracle o ON o.id = po.oracle_id AND o.name = 'chainlink_robinhood';
    IF cnt < 1 THEN
        RAISE EXCEPTION 'Morpho Blue Robinhood -> chainlink_robinhood protocol_oracle binding missing';
    END IF;

    SELECT COUNT(*) INTO cnt
    FROM receipt_token rt
    JOIN protocol p ON p.id = rt.protocol_id AND p.chain_id = 4663
                    AND p.address = '\x9D53d5E3bd5E8d4Cbfa6DB1ca238AEA02E651010'::bytea
    JOIN token t ON t.id = rt.underlying_token_id AND t.chain_id = 4663
                AND t.address = '\x5fc5360d0400a0fd4f2af552add042d716f1d168'::bytea
    WHERE rt.chain_id = 4663
      AND rt.receipt_token_address = '\xbEeFF039907422219FB367E525954ddC092854D9'::bytea;
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'expected 1 groveUSDG receipt_token row under Morpho Blue Robinhood with USDG underlying, found %', cnt;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260915_130000_register_chainlink_robinhood_feeds.sql')
ON CONFLICT (filename) DO NOTHING;
