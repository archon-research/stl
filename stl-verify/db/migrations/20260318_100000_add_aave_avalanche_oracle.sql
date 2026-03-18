-- Seed Aave V3 Avalanche reserve tokens (chain_id=43114)
-- Addresses from AaveV3AvalancheAssets in the official aave-address-book

INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    (43114, '\xd586E7F844cEa2F87f50152665BCbc2C279D8d70'::bytea, 'DAI.e',  18),
    (43114, '\x5947BB275c521040051D82396192181b413227A3'::bytea, 'LINK.e', 18),
    (43114, '\xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E'::bytea, 'USDC',   6),
    (43114, '\x50b7545627a5162F82A992c33b87aDc75187B218'::bytea, 'WBTC.e', 8),
    (43114, '\x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB'::bytea, 'WETH.e', 18),
    (43114, '\x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7'::bytea, 'USDt',   6),
    (43114, '\x63a72806098Bd3D9520cC43356dD78afe5D386D9'::bytea, 'AAVE.e', 18),
    (43114, '\xB31f66AA3C1e785363F0875A1B74E27b85FD66c7'::bytea, 'WAVAX',  18),
    (43114, '\x2b2C81e08f1Af8835a78Bb2A90AE924ACE0eA4bE'::bytea, 'sAVAX',  18),
    (43114, '\xD24C2Ad096400B6FBcd2ad8B24E7acBc21A1da64'::bytea, 'FRAX',   18),
    (43114, '\x5c49b268c9841AFF1Cc3B0a418ff5c3442eE3F3b'::bytea, 'MAI',    18),
    (43114, '\x152b9d0FdC40C096757F570A51E494bd4b943E50'::bytea, 'BTC.b',  8),
    (43114, '\x00000000eFE302BEAA2b3e6e1b18d08D69a9012a'::bytea, 'AUSD',   6),
    (43114, '\xfc421aD3C883Bf9E7C4f42dE845C4e4405799e73'::bytea, 'GHO',    18),
    (43114, '\xC891EB4cbdEFf6e073e859e987815Ed1505c2ACD'::bytea, 'EURC',   6)
ON CONFLICT (chain_id, address) DO UPDATE SET
    symbol     = EXCLUDED.symbol,
    decimals   = EXCLUDED.decimals,
    updated_at = NOW();

-- Insert Aave V3 Avalanche oracle
-- AaveV3Avalanche.ORACLE = 0xEBd36016B3eD09D4693Ed4251c67Bd858c3c7C9C
INSERT INTO oracle (name, display_name, oracle_type, chain_id, address, deployment_block, price_decimals, enabled)
VALUES (
    'aave_v3_avax',
    'Aave V3 Avalanche',
    'aave_oracle',
    43114,
    '\xEBd36016B3eD09D4693Ed4251c67Bd858c3c7C9C'::bytea,
    11970506,
    8,
    true
) ON CONFLICT (name) DO NOTHING;

-- Bind oracle to Aave V3 Avalanche protocol (protocol_id=100446)
INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
SELECT 100446, o.id, 11970506
FROM oracle o
WHERE o.name = 'aave_v3_avax'
ON CONFLICT (protocol_id, oracle_id, from_block) DO NOTHING;

-- Seed oracle_asset rows — INNER JOIN ensures migration fails loudly if any token is missing
INSERT INTO oracle_asset (oracle_id, token_id, enabled, quote_currency)
SELECT o.id, t.id, true, 'USD'
FROM oracle o
JOIN token t ON t.chain_id = 43114 AND t.symbol IN (
    'DAI.e', 'LINK.e', 'USDC', 'WBTC.e', 'WETH.e', 'USDt',
    'AAVE.e', 'WAVAX', 'sAVAX', 'FRAX', 'MAI', 'BTC.b', 'AUSD', 'GHO', 'EURC'
)
WHERE o.name = 'aave_v3_avax'
ON CONFLICT (oracle_id, token_id) WHERE feed_address IS NULL DO NOTHING;

INSERT INTO migrations (filename)
VALUES ('20260318_100000_add_aave_avalanche_oracle.sql')
ON CONFLICT (filename) DO NOTHING;
