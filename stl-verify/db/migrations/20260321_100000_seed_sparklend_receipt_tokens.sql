-- Seed SparkLend receipt tokens (spTokens) for Ethereum mainnet.
-- Maps each spToken address to its underlying token and the SparkLend protocol.
--
-- Source: https://github.com/sparkdotfi/spark-address-registry/blob/master/src/SparkLend.sol
--
-- The subquery resolves protocol_id and underlying_token_id dynamically so this
-- migration is resilient to ID differences across environments.

INSERT INTO receipt_token (protocol_id, underlying_token_id, receipt_token_address, symbol, created_at_block)
SELECT p.id, t.id, v.sp_address, v.sp_symbol, p.created_at_block
FROM (VALUES
    ('\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea, '\x4DEDf26112B3Ec8eC46e7E31EA5e123490B05B8B'::bytea, 'spDAI'),
    ('\x83F20F44975D03b1b09e64809B757c47f942BEeA'::bytea, '\x78f897F0fE2d3B5690EbAe7f19862DEacedF10a7'::bytea, 'spsDAI'),
    ('\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea, '\x377C3bd93f2a2984E1E7bE6A5C22c525eD4A4815'::bytea, 'spUSDC'),
    ('\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea, '\x59cD1C87501baa753d0B5B5Ab5D8416A45cD71DB'::bytea, 'spWETH'),
    ('\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea, '\x12B54025C112Aa61fAce2CDB7118740875A566E9'::bytea, 'spwstETH'),
    ('\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'::bytea, '\x4197ba364AE6698015AE5c1468f54087602715b2'::bytea, 'spWBTC'),
    ('\x6810e776880C02933D47DB1b9fc05908e5386b96'::bytea, '\x7b481aCC9fDADDc9af2cBEA1Ff2342CB1733E50F'::bytea, 'spGNO'),
    ('\xae78736Cd615f374D3085123A210448E74Fc6393'::bytea, '\x9985dF20D7e9103ECBCeb16a84956434B6f06ae8'::bytea, 'sprETH'),
    ('\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea, '\xe7dF13b8e3d6740fe17CBE928C7334243d86c92f'::bytea, 'spUSDT'),
    ('\xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee'::bytea, '\x3CFd5C0D4acAA8Faee335842e4f31159fc76B008'::bytea, 'spweETH'),
    ('\xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf'::bytea, '\xb3973D459df38ae57797811F2A1fd061DA1BC123'::bytea, 'spcbBTC'),
    ('\xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD'::bytea, '\x6715bc100A183cc65502F05845b589c1919ca3d3'::bytea, 'spsUSDS'),
    ('\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea, '\xC02aB1A5eaA8d1B114EF786D9bde108cD4364359'::bytea, 'spUSDS'),
    ('\x8236a87084f8B84306f72007F36F2618A5634494'::bytea, '\xa9d4EcEBd48C282a70CfD3c469d6C8F178a5738E'::bytea, 'spLBTC'),
    ('\x18084fbA666a33d37592fA2633fD49a74DD93a88'::bytea, '\xce6Ca9cDce00a2b0c0d1dAC93894f4Bd2c960567'::bytea, 'sptBTC'),
    ('\xbf5495Efe5DB9ce00f80364C8B423567e58d2110'::bytea, '\xB131cD463d83782d4DE33e00e35EF034F0869bA1'::bytea, 'spezETH'),
    ('\xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7'::bytea, '\x856f1Ea78361140834FDCd0dB0b08079e4A45062'::bytea, 'sprsETH'),
    ('\x6c3ea9036406852006290770BEdFcAbA0e23A0e8'::bytea, '\x779224df1c756b4EDD899854F32a53E8c2B2ce5d'::bytea, 'spPYUSD')
) AS v(underlying_address, sp_address, sp_symbol)
JOIN token t ON t.address = v.underlying_address AND t.chain_id = 1
JOIN protocol p ON p.name = 'SparkLend' AND p.chain_id = 1
ON CONFLICT ON CONSTRAINT receipt_token_protocol_underlying_unique
DO UPDATE SET
    updated_at = NOW();

INSERT INTO migrations (filename)
VALUES ('20260321_100000_seed_sparklend_receipt_tokens.sql')
ON CONFLICT (filename) DO NOTHING;
