-- Seed the SparkLend spUSDT receipt token (Ethereum mainnet, chain_id = 1).
-- It is referenced by the suraf asset->rating mapping
-- (python/suraf/mappings/asset_to_rating.json: "1:0xe7df...": "sparklend_spusdt"),
-- which python-api resolves against receipt_token at startup. In live
-- environments the sparklend indexer writes this row during ingestion; seeding
-- it makes that startup resolution deterministic and unblocks local dev, where
-- the mock chain never ingests it.
--
-- Looks up the SparkLend protocol and underlying USDT token so it stays
-- FK-correct across environments (ids differ per env), inserts nothing if those
-- are absent, and no-ops if the receipt token already exists.

INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol)
SELECT 1, p.id, t.id, '\xe7df13b8e3d6740fe17cbe928c7334243d86c92f'::bytea, 'spUSDT'
FROM protocol p, token t
WHERE p.chain_id = 1 AND p.name = 'SparkLend'
  AND t.chain_id = 1 AND t.address = '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
ON CONFLICT (chain_id, receipt_token_address) DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260604_120000_seed_sparklend_spusdt_receipt_token.sql')
ON CONFLICT (filename) DO NOTHING;
