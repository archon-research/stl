-- Seed the remaining SparkLend receipt tokens referenced by the CORE model
-- asset->market_key mapping (Ethereum mainnet, chain_id = 1): spDAI, spUSDC,
-- spUSDS. spUSDT was already seeded by
-- 20260604_120000_seed_sparklend_spusdt_receipt_token.sql for the SURAF
-- mapping; this completes the set the same way and for the same reason:
-- python-api resolves the mapping against receipt_token at startup, and in
-- local dev the mock chain never ingests these rows. In live environments the
-- sparklend indexer writes them during ingestion; ON CONFLICT makes the seed
-- a no-op there.
--
-- Addresses verified against the SparkLend Pool contract on mainnet
-- (Pool.getReserveData(underlying).aTokenAddress on
-- 0xC13e21B648A5Ee794902342038FF3aDAB66BE987, each round-tripped through the
-- spToken's UNDERLYING_ASSET_ADDRESS()).
--
-- Looks up the SparkLend protocol and each underlying token by natural key so
-- it stays FK-correct across environments (ids differ per env), and inserts
-- nothing if those are absent.

INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol)
SELECT 1, p.id, t.id, v.receipt_address, v.symbol
FROM (VALUES
    ('\x4dedf26112b3ec8ec46e7e31ea5e123490b05b8b'::bytea, 'spDAI',  '\x6b175474e89094c44da98b954eedeac495271d0f'::bytea),
    ('\x377c3bd93f2a2984e1e7be6a5c22c525ed4a4815'::bytea, 'spUSDC', '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea),
    ('\xc02ab1a5eaa8d1b114ef786d9bde108cd4364359'::bytea, 'spUSDS', '\xdc035d45d973e3ec169d2276ddab16f1e407384f'::bytea)
) AS v(receipt_address, symbol, underlying_address)
JOIN protocol p ON p.chain_id = 1 AND p.name = 'SparkLend'
JOIN token t ON t.chain_id = 1 AND t.address = v.underlying_address
ON CONFLICT (chain_id, receipt_token_address) DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260814_120000_seed_sparklend_core_model_receipt_tokens.sql')
ON CONFLICT (filename) DO NOTHING;
