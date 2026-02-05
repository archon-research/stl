-- Seed SparkLend reserve tokens for Ethereum mainnet (chain_id = 1)
-- Token addresses and symbols from getAllReservesTokens method

INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    (1, '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea, 'DAI', 18),
    (1, '\x83F20F44975D03b1b09e64809B757c47f942BEeA'::bytea, 'sDAI', 18),
    (1, '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea, 'USDC', 6),
    (1, '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'::bytea, 'WETH', 18),
    (1, '\x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0'::bytea, 'wstETH', 18),
    (1, '\x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599'::bytea, 'WBTC', 8),
    (1, '\x6810e776880C02933D47DB1b9fc05908e5386b96'::bytea, 'GNO', 18),
    (1, '\xae78736Cd615f374D3085123A210448E74Fc6393'::bytea, 'rETH', 18),
    (1, '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea, 'USDT', 6),
    (1, '\xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee'::bytea, 'weETH', 18),
    (1, '\xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf'::bytea, 'cbBTC', 8),
    (1, '\xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD'::bytea, 'sUSDS', 18),
    (1, '\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea, 'USDS', 18),
    (1, '\x8236a87084f8B84306f72007F36F2618A5634494'::bytea, 'LBTC', 8),
    (1, '\x18084fbA666a33d37592fA2633fD49a74DD93a88'::bytea, 'tBTC', 18),
    (1, '\xbf5495Efe5DB9ce00f80364C8B423567e58d2110'::bytea, 'ezETH', 18),
    (1, '\xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7'::bytea, 'rsETH', 18),
    (1, '\x6c3ea9036406852006290770BEdFcAbA0e23A0e8'::bytea, 'PYUSD', 6)
ON CONFLICT (chain_id, address) DO UPDATE SET
    symbol = EXCLUDED.symbol,
    decimals = EXCLUDED.decimals,
    updated_at = NOW();

INSERT INTO migrations (filename)
VALUES ('20260204_110000_seed_sparklend_tokens.sql')
ON CONFLICT (filename) DO NOTHING;
