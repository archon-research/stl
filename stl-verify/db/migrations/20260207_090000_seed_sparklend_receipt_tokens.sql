-- Seed canonical SparkLend receipt tokens for Ethereum mainnet (chain_id = 1).
--
-- This migration materializes deterministic receipt_token rows keyed by
-- (protocol_id, underlying_token_id) using the SparkLend protocol row and the
-- underlying token rows seeded in 20260204_110000_seed_sparklend_tokens.sql.

WITH spark_protocol AS (
    SELECT id
    FROM protocol
    WHERE chain_id = 1
      AND name = 'SparkLend'
), canonical_receipt_tokens(
    underlying_symbol,
    receipt_token_address,
    receipt_token_symbol,
    created_at_block
) AS (
    VALUES
        ('DAI',     '\x4DEDf26112B3Ec8eC46e7E31EA5e123490B05B8B'::bytea, 'spDAI',     16776401),
        ('sDAI',    '\x78f897F0fE2d3B5690EbAe7f19862DEacedF10a7'::bytea, 'spsDAI',    16776401),
        ('USDC',    '\x377C3bd93f2a2984E1E7bE6A5C22c525eD4A4815'::bytea, 'spUSDC',    16776401),
        ('WETH',    '\x59cD1C87501baa753d0B5B5Ab5D8416A45cD71DB'::bytea, 'spWETH',    16776401),
        ('wstETH',  '\x12B54025C112Aa61fAce2CDB7118740875A566E9'::bytea, 'spwstETH',  16776401),
        ('WBTC',    '\x4197ba364AE6698015AE5c1468f54087602715b2'::bytea, 'spWBTC',    16776401),
        ('GNO',     '\x7b481aCC9fDADDc9af2cBEA1Ff2342CB1733E50F'::bytea, 'spGNO',     16776401),
        ('rETH',    '\x9985dF20D7e9103ECBCeb16a84956434B6f06ae8'::bytea, 'sprETH',    16776401),
        ('USDT',    '\xe7dF13b8e3d6740fe17CBE928C7334243d86c92f'::bytea, 'spUSDT',    16776401),
        ('weETH',   '\x3CFd5C0D4acAA8Faee335842e4f31159fc76B008'::bytea, 'spweETH',   16776401),
        ('cbBTC',   '\xb3973D459df38ae57797811F2A1fd061DA1BC123'::bytea, 'spcbBTC',   16776401),
        ('sUSDS',   '\x6715bc100A183cc65502F05845b589c1919ca3d3'::bytea, 'spsUSDS',   16776401),
        ('USDS',    '\xC02aB1A5eaA8d1B114EF786D9bde108cD4364359'::bytea, 'spUSDS',    16776401),
        ('LBTC',    '\xa9d4EcEBd48C282a70CfD3c469d6C8F178a5738E'::bytea, 'spLBTC',    16776401),
        ('tBTC',    '\xce6Ca9cDce00a2b0c0d1dAC93894f4Bd2c960567'::bytea, 'sptBTC',    16776401),
        ('ezETH',   '\xB131cD463d83782d4DE33e00e35EF034F0869bA1'::bytea, 'spezETH',   16776401),
        ('rsETH',   '\x856f1Ea78361140834FDCd0dB0b08079e4A45062'::bytea, 'sprsETH',   16776401),
        ('PYUSD',   '\x779224df1c756b4EDD899854F32a53E8c2B2ce5d'::bytea, 'spPYUSD',   16776401)
)
INSERT INTO receipt_token (
    protocol_id,
    underlying_token_id,
    receipt_token_address,
    symbol,
    created_at_block,
    updated_at
)
SELECT
    p.id,
    underlying_token.id,
    canonical.receipt_token_address,
    canonical.receipt_token_symbol,
    canonical.created_at_block,
    NOW()
FROM canonical_receipt_tokens canonical
CROSS JOIN spark_protocol p
JOIN token underlying_token
  ON underlying_token.chain_id = 1
 AND underlying_token.symbol = canonical.underlying_symbol
ON CONFLICT (protocol_id, underlying_token_id) DO UPDATE SET
    receipt_token_address = EXCLUDED.receipt_token_address,
    symbol = EXCLUDED.symbol,
    created_at_block = EXCLUDED.created_at_block,
    updated_at = NOW();

INSERT INTO migrations (filename)
VALUES ('20260207_090000_seed_sparklend_receipt_tokens.sql')
ON CONFLICT (filename) DO NOTHING;
