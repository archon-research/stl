-- Register Chainlink price feeds for Arbitrum (chain 42161): USDS/USD and USDC/USD.
-- Unlocks ~$90M of unpriced USDS held directly on Arbitrum.
--
-- Feed addresses from the Chainlink reference-data-directory (RDD) for Arbitrum,
-- verified at https://reference-data-directory.vercel.app/feeds-ethereum-mainnet-arbitrum-1.json:
--   USDS/USD  proxy 0x37833E5b3fbbEd4D613a3e0C354eF91A42B81eeB  (8 decimals, path usds-usd)
--   USDC/USD  proxy 0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3  (8 decimals, path usdc-usd)
-- The USDS/USD feed is a dex_state_price feed (24h heartbeat, 0.5% deviation), weaker
-- than mainnet's Chainlink USDS/USD. PSM3 itself values USDS at exactly $1.

INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    (42161, '\x6491c05a82219b8d1479057361ff1654749b876b'::bytea, 'USDS', 18),
    (42161, '\xaf88d065e77c8cc2239327c5edb3a432268e5831'::bytea, 'USDC', 6)
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, price_decimals, enabled)
VALUES ('chainlink_arbitrum', 'Chainlink (Arbitrum)', 42161, NULL, 'chainlink_feed', 0, 8, true)
ON CONFLICT (name) DO NOTHING;

INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency, processing_version, change_reason)
SELECT o.id, t.id, true, v.feed_address, 8, 'USD', 0, 'VEC-519: initial Arbitrum feed registration'
FROM (VALUES
    ('\x6491c05a82219b8d1479057361ff1654749b876b'::bytea, '\x37833E5b3fbbEd4D613a3e0C354eF91A42B81eeB'::bytea),
    ('\xaf88d065e77c8cc2239327c5edb3a432268e5831'::bytea, '\x50834F3163758fcC1Df9973b6e91f0F0F0434aD3'::bytea)
) AS v(token_address, feed_address)
JOIN token t ON t.chain_id = 42161 AND t.address = v.token_address
JOIN oracle o ON o.name = 'chainlink_arbitrum'
ON CONFLICT (oracle_id, token_id, feed_key, processing_version) DO NOTHING;

DO $$
DECLARE cnt INT;
BEGIN
    SELECT COUNT(DISTINCT t.id) INTO cnt
    FROM (VALUES
        ('\x6491c05a82219b8d1479057361ff1654749b876b'::bytea, '\x37833E5b3fbbEd4D613a3e0C354eF91A42B81eeB'::bytea),
        ('\xaf88d065e77c8cc2239327c5edb3a432268e5831'::bytea, '\x50834F3163758fcC1Df9973b6e91f0F0F0434aD3'::bytea)
    ) AS v(token_address, feed_address)
    JOIN token t ON t.chain_id = 42161 AND t.address = v.token_address
    JOIN oracle o ON o.name = 'chainlink_arbitrum' AND o.enabled AND o.chain_id = 42161
    JOIN oracle_asset oa ON oa.oracle_id = o.id AND oa.token_id = t.id
                        AND oa.feed_address = v.feed_address
                        AND oa.enabled AND oa.feed_decimals = 8 AND oa.quote_currency = 'USD';
    IF cnt <> 2 THEN
        RAISE EXCEPTION 'expected 2 enabled chainlink_arbitrum oracle_asset rows (USDS, USDC), found %', cnt;
    END IF;

    SELECT COUNT(*) INTO cnt
    FROM oracle o
    WHERE o.name = 'chainlink_arbitrum' AND o.chain_id = 42161 AND o.enabled;
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'chainlink_arbitrum oracle not found or not on chain 42161';
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260915_120000_register_chainlink_arbitrum_feeds.sql')
ON CONFLICT (filename) DO NOTHING;
