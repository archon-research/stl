/**
 * The reference registries: chains, protocols, primes, tokens, data sources.
 *
 * Verbatim from staging apart from the token list, which is trimmed from 500
 * rows to the ones the rest of the fixtures reference (see `TOKENS`).
 */
import { FIXTURE_ANCHOR_ISO } from '../clock.ts';
import type { Chain, DataSource, Prime, Protocol, Token } from '../schema.ts';

export const CHAINS: readonly Chain[] = [
  { chain_id: 1, name: 'Ethereum Mainnet' },
  { chain_id: 10, name: 'Optimism' },
  { chain_id: 130, name: 'Unichain' },
  { chain_id: 8453, name: 'Base' },
  { chain_id: 42161, name: 'Arbitrum One' },
  { chain_id: 43114, name: 'Avalanche C-Chain' },
];

export const PROTOCOLS: readonly Protocol[] = [
  {
    id: 1,
    chain_id: 1,
    encode: 'c13e21b648a5ee794902342038ff3adab66be987',
    name: 'SparkLend',
  },
  {
    id: 2,
    chain_id: 1,
    encode: '7d2768de32b0b80b7a3454c06bdac94a69ddc7a9',
    name: 'Aave V2',
  },
  {
    id: 3,
    chain_id: 1,
    encode: '87870bca3f3fd6335c3f4ce8392d69350b4fa4e2',
    name: 'Aave V3',
  },
  {
    id: 4,
    chain_id: 1,
    encode: '4e033931ad43597d96d6bcc25c280717730b58b1',
    name: 'Aave V3 Lido',
  },
  {
    id: 6,
    chain_id: 1,
    encode: 'bbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb',
    name: 'Morpho Blue',
  },
  {
    id: 6330502,
    chain_id: 1,
    encode: '804a6f5f667170f545bf14e5ddb48c70b788390c',
    name: 'maple',
  },
  {
    id: 7343805,
    chain_id: 1,
    encode: '6a8cbed756804b16e05e741edabd5cb544ae21bf',
    name: 'Curve',
  },
  {
    id: 7343806,
    chain_id: 1,
    encode: '1f98431c8ad98523631ae4a59f267346ea31f984',
    name: 'UniswapV3',
  },
  {
    id: 7909060,
    chain_id: 1,
    encode: '52aa899454998be5b000ad077a46bbe360f4e497',
    name: 'fluid',
  },
  {
    id: 10530259,
    chain_id: 8453,
    encode: 'bbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb',
    name: 'Morpho Blue',
  },
  {
    id: 100446,
    chain_id: 43114,
    encode: '794a61358d6845594f94dc1db02a252b5b4814ad',
    name: 'Aave V3 Avalanche',
  },
];

export const SPARK_VAULT = '0x691a6c29e9e96dd897718305427ad5d534db16ba';
export const GROVE_VAULT = '0x26512a41c8406800f21094a7a7a0f980f6e25d43';

/** The proxy the dashboard lands on: the one carrying every non-empty fixture. */
export const SPARK_MAINNET_PROXY = '0x1601843c5e9bc251a3272907010afa41fa18347e';
export const SPARK_BASE_PROXY = '0x2917956eff0b5eaf030abdb4ef4296df775009ca';
export const SPARK_AVALANCHE_PROXY =
  '0xece6b0e8a54c2f44e066fbb9234e7157b15b7fec';
export const GROVE_MAINNET_PROXY = '0x491edfb0b8b608044e227225c715981a30f3a44e';
export const GROVE_BASE_PROXY = '0x9b746dbc5269e1df6e4193bcb441c0fbbf1cecee';
export const GROVE_AVALANCHE_PROXY =
  '0x7107dd8f56642327945294a18a4280c78e153644';

export type PrimeName = 'spark' | 'grove';

/**
 * Narrower than `Prime` in two places the handlers depend on: `name` keys the
 * per-prime lookup tables, and a vault address is guaranteed, so nothing has to
 * fall back to a proxy address in a field the UI groups primes by.
 */
export type SeededPrime = Prime & {
  name: PrimeName;
  prime_vault_address: string;
};

/**
 * Two primes, six ALM proxies. Kept whole: `name` repeating across proxies and
 * grouping by `prime_vault_address` is exactly what the UI's prime grouping is
 * built on, so a one-prime fixture would leave that untested.
 */
export const PRIMES: readonly SeededPrime[] = [
  {
    id: SPARK_MAINNET_PROXY,
    name: 'spark',
    address: SPARK_MAINNET_PROXY,
    chain_id: 1,
    chain: 'mainnet',
    role: 'alm',
    prime_vault_address: SPARK_VAULT,
  },
  {
    id: SPARK_BASE_PROXY,
    name: 'spark',
    address: SPARK_BASE_PROXY,
    chain_id: 8453,
    chain: 'base',
    role: 'alm',
    prime_vault_address: SPARK_VAULT,
  },
  {
    id: GROVE_MAINNET_PROXY,
    name: 'grove',
    address: GROVE_MAINNET_PROXY,
    chain_id: 1,
    chain: 'mainnet',
    role: 'alm',
    prime_vault_address: GROVE_VAULT,
  },
  {
    id: GROVE_AVALANCHE_PROXY,
    name: 'grove',
    address: GROVE_AVALANCHE_PROXY,
    chain_id: 43114,
    chain: 'avalanche-c',
    role: 'alm',
    prime_vault_address: GROVE_VAULT,
  },
  {
    id: GROVE_BASE_PROXY,
    name: 'grove',
    address: GROVE_BASE_PROXY,
    chain_id: 8453,
    chain: 'base',
    role: 'alm',
    prime_vault_address: GROVE_VAULT,
  },
  {
    id: SPARK_AVALANCHE_PROXY,
    name: 'spark',
    address: SPARK_AVALANCHE_PROXY,
    chain_id: 43114,
    chain: 'avalanche-c',
    role: 'alm',
    prime_vault_address: SPARK_VAULT,
  },
];

export const USDS = '0xdc035d45d973e3ec169d2276ddab16f1e407384f';
export const USDT = '0xdac17f958d2ee523a2206206994597c13d831ec7';
export const DAI = '0x6b175474e89094c44da98b954eedeac495271d0f';
export const USDC = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48';
export const PYUSD = '0x6c3ea9036406852006290770bedfcaba0e23a0e8';
export const WETH = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2';
export const SUSDS = '0xa3931d71877c0e7a3148cb7eb4463524fec27fbd';
export const SPUSDS = '0xc02ab1a5eaa8d1b114ef786d9bde108cd4364359';
export const USDC_AVALANCHE = '0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e';
export const AAVAUSDC = '0x625e7708f30ca75bfd92586e17077590c60eb4cd';

/**
 * 500 staging rows trimmed to 24: every id the allocation, activity, risk and
 * price fixtures reference, plus the zero-address row, which is the only one in
 * staging with a null `symbol` and a zero `decimals` — the shape the UI's token
 * formatting has to survive. `check-mock-api.mjs` asserts that "every id
 * resolves" half, across all proxies.
 *
 * `updated_at` is the only timestamp in the fixture set that is deliberately
 * absolute: registry rows are months old by design and sit in no window.
 *
 * Staging carries two rows for `spUSDS` (ids 736 and 858954, one address).
 * Normalised here to 736, the id the allocation rows point at, because a fixture
 * exists to be coherent rather than to reproduce an upstream duplicate.
 */
export const TOKENS: readonly Token[] = [
  {
    id: 1,
    chain_id: 1,
    address: DAI,
    symbol: 'DAI',
    decimals: 18,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: null,
  },
  {
    id: 3,
    chain_id: 1,
    address: USDC,
    symbol: 'USDC',
    decimals: 6,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: null,
  },
  {
    id: 4,
    chain_id: 1,
    address: WETH,
    symbol: 'WETH',
    decimals: 18,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: null,
  },
  {
    id: 6,
    chain_id: 1,
    address: '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599',
    symbol: 'WBTC',
    decimals: 8,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: null,
  },
  {
    id: 9,
    chain_id: 1,
    address: USDT,
    symbol: 'USDT',
    decimals: 6,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: null,
  },
  {
    id: 11,
    chain_id: 1,
    address: '0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf',
    symbol: 'cbBTC',
    decimals: 8,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: null,
  },
  {
    id: 12,
    chain_id: 1,
    address: SUSDS,
    symbol: 'sUSDS',
    decimals: 18,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: null,
  },
  {
    id: 13,
    chain_id: 1,
    address: USDS,
    symbol: 'USDS',
    decimals: 18,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: null,
  },
  {
    id: 18,
    chain_id: 1,
    address: PYUSD,
    symbol: 'PYUSD',
    decimals: 6,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: null,
  },
  {
    id: 34,
    chain_id: 1,
    address: '0x23878914efe38d27c4d67ab83ed1b93a74d4086a',
    symbol: 'aEthUSDT',
    decimals: 6,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: {},
  },
  {
    id: 269,
    chain_id: 1,
    address: '0x59cd1c87501baa753d0b5b5ab5d8416a45cd71db',
    symbol: 'spWETH',
    decimals: 18,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: {},
  },
  {
    id: 302,
    chain_id: 1,
    address: '0x0000000000000000000000000000000000000000',
    symbol: null,
    decimals: 0,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: {},
  },
  {
    id: 338,
    chain_id: 1,
    address: '0xe7df13b8e3d6740fe17cbe928c7334243d86c92f',
    symbol: 'spUSDT',
    decimals: 6,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: {},
  },
  {
    id: 723,
    chain_id: 1,
    address: '0x4dedf26112b3ec8ec46e7e31ea5e123490b05b8b',
    symbol: 'spDAI',
    decimals: 18,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: {},
  },
  {
    id: 735,
    chain_id: 1,
    address: '0x779224df1c756b4edd899854f32a53e8c2b2ce5d',
    symbol: 'spPYUSD',
    decimals: 6,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: {},
  },
  {
    id: 736,
    chain_id: 1,
    address: SPUSDS,
    symbol: 'spUSDS',
    decimals: 18,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: {},
  },
  {
    id: 850711,
    chain_id: 1,
    address: '0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b',
    symbol: 'syrupUSDC',
    decimals: 6,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: {},
  },
  {
    id: 885660,
    chain_id: 1,
    address: '0x56a76b428244a50513ec81e225a293d128fd581d',
    symbol: 'sparkUSDCbc',
    decimals: 6,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: {},
  },
  {
    id: 892750,
    chain_id: 1,
    address: '0x73e65dbd630f90604062f6e02fab9138e713edd9',
    symbol: 'spDAI',
    decimals: 18,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: {},
  },
  {
    id: 5,
    chain_id: 1,
    address: '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0',
    symbol: 'wstETH',
    decimals: 18,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: null,
  },
  {
    id: 10,
    chain_id: 1,
    address: '0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee',
    symbol: 'weETH',
    decimals: 18,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: null,
  },
  {
    id: 2,
    chain_id: 1,
    address: '0x83f20f44975d03b1b09e64809b757c47f942beea',
    symbol: 'sDAI',
    decimals: 18,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: null,
  },
  {
    id: 1301,
    chain_id: 43114,
    address: USDC_AVALANCHE,
    symbol: 'USDC',
    decimals: 6,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: null,
  },
  {
    id: 1302,
    chain_id: 43114,
    address: AAVAUSDC,
    symbol: 'aAvaUSDC',
    decimals: 6,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata: {},
  },
];

/**
 * Oracle prices by token id. USDS is the one real capture; the rest are the
 * prices the risk-breakdown and allocation fixtures were valued at, so a screen
 * that re-prices a position agrees with the row it came from.
 *
 * A token missing from this map answers `200` with the documented
 * `missing_quote` shape rather than a price, which is the branch the UI's stale
 * and unpriced states hang off.
 */
export const TOKEN_PRICES_USD: Readonly<Record<number, string>> = {
  1: '1.000271690000000000',
  3: '1.000318970000000000',
  4: '1897.710000000000000000',
  6: '64227.070592560000000000',
  9: '0.999151400000000000',
  11: '64200.425000000000000000',
  12: '1.106634890000000000',
  13: '1.000000000000000000',
  18: '1.000093900000000000',
  736: '0.999851540000000000',
  1301: '1.000318970000000000',
  1302: '1.000318970000000000',
  5: '2356.736518580000000000',
  10: '2090.600501825325400000',
  2: '1.179214890000000000',
};

export const DATA_SOURCES: readonly DataSource[] = [
  {
    name: 'STL Allocation Index',
    host: 'Same app (internal API)',
    access_model: 'closed',
    role: 'Internal allocation snapshots, price feeds, risk calculations',
    caveat: 'Internal-only backend',
    attribution_required: false,
  },
  {
    name: 'Chainlink Price Feeds',
    host: 'onchain (mainnet)',
    access_model: 'open',
    role: 'Token oracle prices from onchain contracts',
    caveat: null,
    attribution_required: false,
  },
  {
    name: 'Pyth Network',
    host: 'onchain + API',
    access_model: 'open',
    role: 'Multi-chain token oracle prices and confidence intervals',
    caveat: null,
    attribution_required: false,
  },
  {
    name: 'Self-computed Risk Capital (gap_sweep)',
    host: 'onchain + model',
    access_model: 'open',
    role: 'Required and Total Risk Capital and encumbrance shown on the dashboard, computed from on-chain allocations (gap_sweep stress) and the on-chain SubProxy treasury',
    caveat:
      'Model-derived and partial; covers on-chain lending positions only.',
    attribution_required: false,
  },
  {
    name: 'Star Agents Risk Capital & Requirements Monitor',
    host: 'https://info.skyeco.com/required-risk-capital',
    access_model: 'public',
    role: 'Risk capital requirements and monitor metrics by star (reference/parity)',
    caveat:
      "Kept for parity checks; no longer the source of the dashboard's risk-capital figures.",
    attribution_required: false,
  },
  {
    name: 'Anchorage Custody API',
    host: 'closed backend (Anchorage Digital)',
    access_model: 'closed',
    role: 'Off-chain BTC custody package snapshots (collateral, loan exposure, LTV)',
    caveat:
      "Polled every 15 minutes; surfaced with the snapshot's own timestamp so a frozen upstream feed reads as honestly stale rather than current.",
    attribution_required: false,
  },
];
