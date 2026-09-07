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

/**
 * Column types come off `Protocol` itself rather than being spelled `number` and
 * `string`, so a renamed or retyped field breaks the table, not just the row
 * bodies. Every row table below is typed the same way.
 */
type ProtocolRow = readonly [
  id: Protocol['id'],
  chain_id: Protocol['chain_id'],
  encode: Protocol['encode'],
  name: Protocol['name'],
];

const PROTOCOL_ROWS: readonly ProtocolRow[] = [
  [1, 1, 'c13e21b648a5ee794902342038ff3adab66be987', 'SparkLend'],
  [2, 1, '7d2768de32b0b80b7a3454c06bdac94a69ddc7a9', 'Aave V2'],
  [3, 1, '87870bca3f3fd6335c3f4ce8392d69350b4fa4e2', 'Aave V3'],
  [4, 1, '4e033931ad43597d96d6bcc25c280717730b58b1', 'Aave V3 Lido'],
  [6, 1, 'bbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb', 'Morpho Blue'],
  [6330502, 1, '804a6f5f667170f545bf14e5ddb48c70b788390c', 'maple'],
  [7343805, 1, '6a8cbed756804b16e05e741edabd5cb544ae21bf', 'Curve'],
  [7343806, 1, '1f98431c8ad98523631ae4a59f267346ea31f984', 'UniswapV3'],
  [7909060, 1, '52aa899454998be5b000ad077a46bbe360f4e497', 'fluid'],
  [10530259, 8453, 'bbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb', 'Morpho Blue'],
  [
    100446,
    43114,
    '794a61358d6845594f94dc1db02a252b5b4814ad',
    'Aave V3 Avalanche',
  ],
];

export const PROTOCOLS: readonly Protocol[] = PROTOCOL_ROWS.map(
  ([id, chain_id, encode, name]): Protocol => ({ id, chain_id, encode, name }),
);

export const SPARK_VAULT = '0x691a6c29e9e96dd897718305427ad5d534db16ba';
const GROVE_VAULT = '0x26512a41c8406800f21094a7a7a0f980f6e25d43';

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

/** Identical across a prime's proxies, which is what the UI groups rows by. */
const PRIME_VAULTS: Readonly<Record<PrimeName, string>> = {
  spark: SPARK_VAULT,
  grove: GROVE_VAULT,
};

type PrimeRow = readonly [
  address: SeededPrime['address'],
  name: PrimeName,
  chain_id: SeededPrime['chain_id'],
  chain: SeededPrime['chain'],
];

/**
 * Two primes, six ALM proxies. Kept whole: `name` repeating across proxies and
 * grouping by `prime_vault_address` is exactly what the UI's prime grouping is
 * built on, so a one-prime fixture would leave that untested.
 */
const PRIME_ROWS: readonly PrimeRow[] = [
  [SPARK_MAINNET_PROXY, 'spark', 1, 'mainnet'],
  [SPARK_BASE_PROXY, 'spark', 8453, 'base'],
  [GROVE_MAINNET_PROXY, 'grove', 1, 'mainnet'],
  [GROVE_AVALANCHE_PROXY, 'grove', 43114, 'avalanche-c'],
  [GROVE_BASE_PROXY, 'grove', 8453, 'base'],
  [SPARK_AVALANCHE_PROXY, 'spark', 43114, 'avalanche-c'],
];

export const PRIMES: readonly SeededPrime[] = PRIME_ROWS.map(
  ([address, name, chain_id, chain]): SeededPrime => ({
    // The document deprecates `id` as byte-identical to `address` in the same
    // row; carrying it as a column would let the fixture contradict that.
    id: address,
    name,
    address,
    chain_id,
    chain,
    // The endpoint lists allocation venues only, so every row is an ALM proxy.
    role: 'alm',
    prime_vault_address: PRIME_VAULTS[name],
  }),
);

export const USDS = '0xdc035d45d973e3ec169d2276ddab16f1e407384f';
const USDT = '0xdac17f958d2ee523a2206206994597c13d831ec7';
const DAI = '0x6b175474e89094c44da98b954eedeac495271d0f';
const USDC = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48';
const PYUSD = '0x6c3ea9036406852006290770bedfcaba0e23a0e8';
const WETH = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2';
const SUSDS = '0xa3931d71877c0e7a3148cb7eb4463524fec27fbd';
export const SPUSDS = '0xc02ab1a5eaa8d1b114ef786d9bde108cd4364359';
const USDC_AVALANCHE = '0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e';
const AAVAUSDC = '0x625e7708f30ca75bfd92586e17077590c60eb4cd';

/**
 * 500 staging rows trimmed to 24: every id the allocation, activity, risk and
 * price fixtures reference, plus the zero-address row below.
 * `check-mock-api.ts` asserts that "every id resolves" half, across all
 * proxies.
 *
 * `updated_at` is the only timestamp in the fixture set that is deliberately
 * absolute: registry rows are months old by design and sit in no window, which
 * is why the row tables below carry no column for it.
 *
 * Staging carries two rows for `spUSDS` (ids 736 and 858954, one address).
 * Normalised here to 736, the id the allocation rows point at, because a fixture
 * exists to be coherent rather than to reproduce an upstream duplicate.
 */
type TokenRow = readonly [
  id: Token['id'],
  chain_id: Token['chain_id'],
  address: Token['address'],
  symbol: Token['symbol'],
  decimals: Token['decimals'],
  metadata: Token['metadata'],
];

const TOKEN_ROWS: readonly TokenRow[] = [
  [1, 1, DAI, 'DAI', 18, null],
  [3, 1, USDC, 'USDC', 6, null],
  [4, 1, WETH, 'WETH', 18, null],
  [6, 1, '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599', 'WBTC', 8, null],
  [9, 1, USDT, 'USDT', 6, null],
  [11, 1, '0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf', 'cbBTC', 8, null],
  [12, 1, SUSDS, 'sUSDS', 18, null],
  [13, 1, USDS, 'USDS', 18, null],
  [18, 1, PYUSD, 'PYUSD', 6, null],
  [34, 1, '0x23878914efe38d27c4d67ab83ed1b93a74d4086a', 'aEthUSDT', 6, {}],
  [269, 1, '0x59cd1c87501baa753d0b5b5ab5d8416a45cd71db', 'spWETH', 18, {}],
  // Staging's only null `symbol` and zero `decimals` — the shape the UI's token
  // formatting has to survive.
  [302, 1, '0x0000000000000000000000000000000000000000', null, 0, {}],
  [338, 1, '0xe7df13b8e3d6740fe17cbe928c7334243d86c92f', 'spUSDT', 6, {}],
  [723, 1, '0x4dedf26112b3ec8ec46e7e31ea5e123490b05b8b', 'spDAI', 18, {}],
  [735, 1, '0x779224df1c756b4edd899854f32a53e8c2b2ce5d', 'spPYUSD', 6, {}],
  [736, 1, SPUSDS, 'spUSDS', 18, {}],
  [850711, 1, '0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b', 'syrupUSDC', 6, {}],
  [
    885660,
    1,
    '0x56a76b428244a50513ec81e225a293d128fd581d',
    'sparkUSDCbc',
    6,
    {},
  ],
  [892750, 1, '0x73e65dbd630f90604062f6e02fab9138e713edd9', 'spDAI', 18, {}],
  [5, 1, '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0', 'wstETH', 18, null],
  [10, 1, '0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee', 'weETH', 18, null],
  [2, 1, '0x83f20f44975d03b1b09e64809b757c47f942beea', 'sDAI', 18, null],
  [1301, 43114, USDC_AVALANCHE, 'USDC', 6, null],
  [1302, 43114, AAVAUSDC, 'aAvaUSDC', 6, {}],
];

export const TOKENS: readonly Token[] = TOKEN_ROWS.map(
  ([id, chain_id, address, symbol, decimals, metadata]): Token => ({
    id,
    chain_id,
    address,
    symbol,
    decimals,
    updated_at: FIXTURE_ANCHOR_ISO,
    metadata,
  }),
);

const TOKENS_BY_ID: ReadonlyMap<number, Token> = new Map(
  TOKENS.map((row): [number, Token] => [row.id, row]),
);

/**
 * The registry row an id names. Throws instead of answering `undefined`: the
 * position fixtures read symbol, address and chain off a token id the way the
 * responses denormalise them, so a dangling id has to fail while the fixture is
 * being built rather than reach a response body as a hole.
 */
export function tokenById(id: number): Token {
  const token = TOKENS_BY_ID.get(id);
  if (token === undefined) {
    throw new Error(`no TOKENS row for token id ${id}`);
  }

  return token;
}

/** The responses type a position's symbol non-null; only id 302 has none. */
export function tokenSymbol(id: number): string {
  const symbol = tokenById(id).symbol ?? null;
  if (symbol === null) {
    throw new Error(`TOKENS row ${id} carries no symbol`);
  }

  return symbol;
}

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

/**
 * Which provenances each prime can be answered from.
 *
 * `grove` is deliberately indexed-only: with every prime covered by Sky the
 * greyed-out option and the URL rewrite would never be reachable offline, and
 * those are the two things the coverage endpoint exists to drive.
 */
export function provenanceAvailability(): {
  primes: { name: string; available: ('indexed' | 'reference' | 'both')[] }[];
  reference_upstream_reachable: boolean;
} {
  return {
    primes: [
      { name: 'spark', available: ['indexed', 'reference', 'both'] },
      { name: 'grove', available: ['indexed'] },
    ],
    reference_upstream_reachable: true,
  };
}
