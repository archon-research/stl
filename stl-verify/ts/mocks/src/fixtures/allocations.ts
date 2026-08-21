/**
 * Allocation rows per ALM proxy, and the activity feed that explains them.
 *
 * Trimmed from staging's 24 rows on the spark mainnet proxy to 13, chosen to
 * keep one of every shape the UI branches on rather than the 13 largest:
 * priced receipt-token positions across four protocols, a dust position, an
 * unpriceable one (`maple`/`syrupUSDC`, which risk-capital reports
 * `applied: false` for), three `asset` rows with `receipt_token_id: null`, and
 * the off-chain Anchorage custody leg on `chain_id: 0` with `scope: 'prime'`.
 *
 * The cross-references are load-bearing: every `receipt_token_id` and
 * `underlying_token_id` resolves in `TOKENS` on its own chain, every priced
 * allocation has a `per_allocation` entry in the risk-capital fixture, and three
 * activity rows carry transactions whose events the tx endpoint answers for.
 * `check-mock-api.mjs` asserts each of those; break one and it fails rather than
 * the dashboard quietly rendering holes.
 */
import { DAY_MS, MINUTE_MS, SECOND_MS, offsetIsoAgo } from '../clock.ts';
import type { Allocation, AllocationActivity } from '../schema.ts';
import type { PrimeName } from './registry.ts';
import {
  AAVAUSDC,
  DAI,
  GROVE_MAINNET_PROXY,
  PYUSD,
  SPARK_AVALANCHE_PROXY,
  SPARK_MAINNET_PROXY,
  SPUSDS,
  SUSDS,
  USDC,
  USDC_AVALANCHE,
  USDS,
  USDT,
  WETH,
} from './registry.ts';

const LAST_SWEEP_AGO = 5 * MINUTE_MS + 13 * SECOND_MS;
const LAST_TRANSFER_AGO = 85 * SECOND_MS;
/** The custody snapshot is two months stale on purpose; see DATA_SOURCES. */
const CUSTODY_SNAPSHOT_AGO = 63 * DAY_MS;

export const SPARK_TX_HASH =
  '0x4e5c1fe085a5268abad4016609435ccc07a45527d9cf54b654d9a07cf324d932';

function sparkMainnetAllocations(nowMs: number): Allocation[] {
  const sweptAt = offsetIsoAgo(nowMs, LAST_SWEEP_AGO);
  const sweep = {
    latest_activity_at: sweptAt,
    latest_activity_action: 'sweep',
    scope: 'proxy',
  } as const;

  return [
    {
      ...sweep,
      chain_id: 1,
      receipt_token_id: 736,
      receipt_token_address: SPUSDS,
      underlying_token_id: 13,
      underlying_token_address: USDS,
      symbol: 'spUSDS',
      underlying_symbol: 'USDS',
      protocol_name: 'SparkLend',
      balance: '842029895.945548368376556578',
      amount_usd: '841904871.346598373354820026',
      latest_activity_amount: '0.000000000000000000',
      category: 'allocation',
    },
    {
      ...sweep,
      chain_id: 1,
      receipt_token_id: 338,
      receipt_token_address: '0xe7df13b8e3d6740fe17cbe928c7334243d86c92f',
      underlying_token_id: 9,
      underlying_token_address: USDT,
      symbol: 'spUSDT',
      underlying_symbol: 'USDT',
      protocol_name: 'SparkLend',
      balance: '347003142.672431',
      amount_usd: '346708318.392322222449470000',
      latest_activity_amount: '0.000000',
      category: 'allocation',
    },
    {
      ...sweep,
      chain_id: 1,
      receipt_token_id: 723,
      receipt_token_address: '0x4dedf26112b3ec8ec46e7e31ea5e123490b05b8b',
      underlying_token_id: 1,
      underlying_token_address: DAI,
      symbol: 'spDAI',
      underlying_symbol: 'DAI',
      protocol_name: 'SparkLend',
      balance: '296139496.545005544597463815',
      amount_usd: '296086123.323543238248161014',
      latest_activity_amount: '0.000000000000000000',
      category: 'allocation',
    },
    {
      ...sweep,
      chain_id: 1,
      receipt_token_id: 735,
      receipt_token_address: '0x779224df1c756b4edd899854f32a53e8c2b2ce5d',
      underlying_token_id: 18,
      underlying_token_address: PYUSD,
      symbol: 'spPYUSD',
      underlying_symbol: 'PYUSD',
      protocol_name: 'SparkLend',
      balance: '100000478.973363',
      amount_usd: '100009872.018352967986590000',
      latest_activity_amount: '0.000000',
      category: 'allocation',
    },
    {
      ...sweep,
      chain_id: 1,
      receipt_token_id: 269,
      receipt_token_address: '0x59cd1c87501baa753d0b5b5ab5d8416a45cd71db',
      underlying_token_id: 4,
      underlying_token_address: WETH,
      symbol: 'spWETH',
      underlying_symbol: 'WETH',
      protocol_name: 'SparkLend',
      balance: '32065.742209491845604541',
      amount_usd: '60851479.648374770322193501',
      latest_activity_amount: '0.000000000000000000',
      category: 'allocation',
    },
    {
      ...sweep,
      chain_id: 1,
      receipt_token_id: 885660,
      receipt_token_address: '0x56a76b428244a50513ec81e225a293d128fd581d',
      underlying_token_id: 3,
      underlying_token_address: USDC,
      symbol: 'sparkUSDCbc',
      underlying_symbol: 'USDC',
      protocol_name: 'Morpho Blue',
      balance: '8758007.042993599249382474',
      amount_usd: '9057828.812411176980000000',
      latest_activity_amount: '0.000000000000000000',
      category: 'allocation',
    },
    {
      ...sweep,
      chain_id: 1,
      receipt_token_id: 892750,
      receipt_token_address: '0x73e65dbd630f90604062f6e02fab9138e713edd9',
      underlying_token_id: 1,
      underlying_token_address: DAI,
      symbol: 'spDAI',
      underlying_symbol: 'DAI',
      protocol_name: 'Morpho Blue',
      balance: '401.044942988323179297',
      amount_usd: '839.944852318554772032',
      latest_activity_amount: '0.000000000000000000',
      category: 'allocation',
    },
    {
      ...sweep,
      chain_id: 1,
      receipt_token_id: 34,
      receipt_token_address: '0x23878914efe38d27c4d67ab83ed1b93a74d4086a',
      underlying_token_id: 9,
      underlying_token_address: USDT,
      symbol: 'aEthUSDT',
      underlying_symbol: 'USDT',
      protocol_name: 'Aave V3',
      balance: '5.946339',
      amount_usd: '5.941286811995430000',
      latest_activity_amount: '0.000000',
      category: 'allocation',
    },
    {
      ...sweep,
      chain_id: 1,
      receipt_token_id: 850711,
      receipt_token_address: '0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b',
      underlying_token_id: 3,
      underlying_token_address: USDC,
      symbol: 'syrupUSDC',
      underlying_symbol: 'USDC',
      protocol_name: 'maple',
      balance: '0.000001',
      amount_usd: '0.000000999870000000',
      latest_activity_amount: '0.000000',
      category: 'allocation',
    },
    {
      ...sweep,
      chain_id: 1,
      receipt_token_id: null,
      receipt_token_address: null,
      underlying_token_id: 12,
      underlying_token_address: SUSDS,
      symbol: 'sUSDS',
      underlying_symbol: 'sUSDS',
      protocol_name: null,
      balance: '617652055.510293017784331115',
      amount_usd: '683515036.564482027971673304',
      latest_activity_amount: '0.000000000000000000',
      category: 'asset',
    },
    {
      ...sweep,
      chain_id: 1,
      receipt_token_id: null,
      receipt_token_address: null,
      underlying_token_id: 18,
      underlying_token_address: PYUSD,
      symbol: 'PYUSD',
      underlying_symbol: 'PYUSD',
      protocol_name: null,
      balance: '238780734.151812',
      amount_usd: '238687485.499511034377760000',
      latest_activity_amount: '0.000000',
      category: 'asset',
    },
    {
      chain_id: 1,
      receipt_token_id: null,
      receipt_token_address: null,
      underlying_token_id: 9,
      underlying_token_address: USDT,
      symbol: 'USDT',
      underlying_symbol: 'USDT',
      protocol_name: null,
      balance: '4640.274995',
      amount_usd: '4635.170692505500000000',
      latest_activity_at: offsetIsoAgo(nowMs, LAST_TRANSFER_AGO),
      latest_activity_action: 'in',
      latest_activity_amount: '4640.274995',
      category: 'asset',
      scope: 'proxy',
    },
    {
      chain_id: 0,
      receipt_token_id: null,
      receipt_token_address: null,
      underlying_token_id: null,
      underlying_token_address: null,
      symbol: 'BTC',
      underlying_symbol: 'BTC',
      protocol_name: 'anchorage',
      balance: '4722.61',
      amount_usd: '250000000',
      latest_activity_at: offsetIsoAgo(nowMs, CUSTODY_SNAPSHOT_AGO),
      latest_activity_action: null,
      latest_activity_amount: null,
      category: 'custody',
      scope: 'prime',
    },
  ];
}

/**
 * Staging's avalanche spark proxy holds one dust position; the base proxy holds
 * nothing. Both are kept because they are the only fixtures that exercise the
 * near-zero and empty branches of the allocation table.
 */
function sparkAvalancheAllocations(nowMs: number): Allocation[] {
  return [
    {
      chain_id: 43114,
      receipt_token_id: 1302,
      receipt_token_address: AAVAUSDC,
      underlying_token_id: 1301,
      underlying_token_address: USDC_AVALANCHE,
      symbol: 'aAvaUSDC',
      underlying_symbol: 'USDC',
      protocol_name: 'Aave V3 Avalanche',
      balance: '0.156275',
      amount_usd: '0.156324898675310000',
      latest_activity_at: offsetIsoAgo(nowMs, LAST_SWEEP_AGO),
      latest_activity_action: 'sweep',
      latest_activity_amount: '0.000000',
      category: 'allocation',
      scope: 'proxy',
    },
  ];
}

function groveMainnetAllocations(nowMs: number): Allocation[] {
  const sweptAt = offsetIsoAgo(nowMs, LAST_SWEEP_AGO);

  return [
    {
      chain_id: 1,
      receipt_token_id: 736,
      receipt_token_address: SPUSDS,
      underlying_token_id: 13,
      underlying_token_address: USDS,
      symbol: 'spUSDS',
      underlying_symbol: 'USDS',
      protocol_name: 'SparkLend',
      balance: '124500000.000000000000000000',
      amount_usd: '124481521.310000000000000000',
      latest_activity_at: sweptAt,
      latest_activity_action: 'sweep',
      latest_activity_amount: '0.000000000000000000',
      category: 'allocation',
      scope: 'proxy',
    },
    {
      chain_id: 1,
      receipt_token_id: null,
      receipt_token_address: null,
      underlying_token_id: 12,
      underlying_token_address: SUSDS,
      symbol: 'sUSDS',
      underlying_symbol: 'sUSDS',
      protocol_name: null,
      balance: '38119004.221100000000000000',
      amount_usd: '42184992.760900000000000000',
      latest_activity_at: sweptAt,
      latest_activity_action: 'sweep',
      latest_activity_amount: '0.000000000000000000',
      category: 'asset',
      scope: 'proxy',
    },
  ];
}

/**
 * USD per unit of each receipt token a position holds, read off that position:
 * `amount_usd / balance` is the row's share ratio times its underlying's oracle
 * price, which is the product the real endpoint multiplies a flow by. Reading it
 * off the position is what keeps a valued flow agreeing with the allocation row
 * it moved, rather than with a price the fixture priced nothing at.
 *
 * Directly-held underlyings are absent by construction, matching the endpoint:
 * it values only receipt-token flows, because a treasury token's outflows are
 * recorded as sweeps and pricing its inflows alone reports gross throughput as
 * net flow.
 */
export function receiptTokenUsdPerUnit(
  nowMs: number,
): ReadonlyMap<number, number> {
  const priced = new Map<number, number>();

  for (const row of Object.values(seedAllocations(nowMs)).flat()) {
    const receiptTokenId = row.receipt_token_id ?? null;
    const balance = Number(row.balance);
    if (receiptTokenId === null || balance <= 0) {
      continue;
    }
    priced.set(receiptTokenId, Number(row.amount_usd) / balance);
  }

  return priced;
}

/** Keyed by lower-cased proxy address; a proxy with no entry holds nothing. */
export function seedAllocations(
  nowMs: number,
): Record<string, readonly Allocation[]> {
  return {
    [SPARK_MAINNET_PROXY]: sparkMainnetAllocations(nowMs),
    [SPARK_AVALANCHE_PROXY]: sparkAvalancheAllocations(nowMs),
    [GROVE_MAINNET_PROXY]: groveMainnetAllocations(nowMs),
  };
}

/**
 * Sky's Star monitor answers the same shape prime-scoped and USD-only, so
 * `?reference` in the UI's comparison harness has something to compare against.
 *
 * Keyed by prime, not shared: serving spark's positions for a grove proxy would
 * make the comparison the flag exists for silently compare two different primes.
 * `undefined` is "the monitor does not track this prime", which the endpoint
 * answers as a 404.
 */
export function seedReferenceAllocations(
  nowMs: number,
  primeName: PrimeName,
): Allocation[] | undefined {
  const selfRows =
    primeName === 'spark'
      ? sparkMainnetAllocations(nowMs)
      : groveMainnetAllocations(nowMs);

  return selfRows
    .filter((allocation) => allocation.category === 'allocation')
    .map((allocation): Allocation => ({
      ...allocation,
      balance: null,
      scope: 'prime',
    }));
}

/** Spread so 16 rows cover the whole default 24h window, newest first. */
const ACTIVITY_ROW_SPACING = 85 * MINUTE_MS;

/** Everything but the three fields the clock owns. */
type ActivityRowSeed = Omit<
  AllocationActivity,
  'block_number' | 'block_version' | 'created_at'
>;

/**
 * Sixteen rows across the protocols, chains and action types the activity
 * filters offer. Staging's 50-row capture was one token on one protocol, which
 * would leave every filter in the UI untestable.
 *
 * Row 3 carries {@link SPARK_TX_HASH}, the transaction the protocol-event
 * fixtures decode — that pair is what makes "click a row, see its events" work
 * offline.
 */
const ACTIVITY_ROWS: readonly ActivityRowSeed[] = [
  {
    chain_id: 1,
    prime_address: SPARK_MAINNET_PROXY,
    prime_name: 'spark',
    protocol_name: 'SparkLend',
    token_id: 736,
    token_symbol: 'spUSDS',
    action_type: 'sweep',
    tx_amount: '0.000000000000000000',
    balance: '842029895.945548368376556578',
    tx_hash: null,
    log_index: 0,
  },
  {
    chain_id: 1,
    prime_address: SPARK_MAINNET_PROXY,
    prime_name: 'spark',
    protocol_name: null,
    token_id: 9,
    token_symbol: 'USDT',
    action_type: 'in',
    tx_amount: '4640.274995',
    balance: '4640.274995',
    tx_hash:
      '0x5be036b81b1c79e4acdfd63ceafdc25e1a00f5cb4d0e87120665c4927764fbfc',
    log_index: 118,
  },
  {
    chain_id: 1,
    prime_address: SPARK_MAINNET_PROXY,
    prime_name: 'spark',
    protocol_name: 'SparkLend',
    token_id: 736,
    token_symbol: 'spUSDS',
    action_type: 'in',
    tx_amount: '23882.033557750140214353',
    balance: '842024608.502433178884234581',
    tx_hash: SPARK_TX_HASH,
    log_index: 242,
  },
  {
    chain_id: 1,
    prime_address: SPARK_MAINNET_PROXY,
    prime_name: 'spark',
    protocol_name: 'Morpho Blue',
    token_id: 885660,
    token_symbol: 'sparkUSDCbc',
    action_type: 'in',
    tx_amount: '1500000.000000',
    balance: '8758007.042993599249382474',
    tx_hash:
      '0x6ee15ae58c284dd3827ce7924e9e6fede5fc76d756e852441a32e3673f813a95',
    log_index: 57,
  },
  {
    chain_id: 1,
    prime_address: SPARK_MAINNET_PROXY,
    prime_name: 'spark',
    protocol_name: 'SparkLend',
    token_id: 338,
    token_symbol: 'spUSDT',
    action_type: 'out',
    tx_amount: '750000.000000',
    balance: '347003142.672431',
    tx_hash:
      '0x43019395d99015a53120b8dea9aa964ff4ff6c4ac2437c3ed00a13eae61b227b',
    log_index: 91,
  },
  {
    chain_id: 1,
    prime_address: SPARK_MAINNET_PROXY,
    prime_name: 'spark',
    protocol_name: 'SparkLend',
    token_id: 723,
    token_symbol: 'spDAI',
    action_type: 'in',
    tx_amount: '2250000.000000000000000000',
    balance: '296139496.545005544597463815',
    tx_hash:
      '0xe3bc0428879bec38409a0b0552592a2ffb40dd6ea7352e7ad980c901bf760380',
    log_index: 12,
  },
  {
    chain_id: 1,
    prime_address: SPARK_MAINNET_PROXY,
    prime_name: 'spark',
    protocol_name: 'Aave V3',
    token_id: 34,
    token_symbol: 'aEthUSDT',
    action_type: 'out',
    tx_amount: '5.946339',
    balance: '5.946339',
    tx_hash:
      '0x7df714df26c05d09bea46cbd0c843b625abfcae9d235f76ffaf7a629d1e979c8',
    log_index: 204,
  },
  {
    chain_id: 1,
    prime_address: SPARK_MAINNET_PROXY,
    prime_name: 'spark',
    protocol_name: 'SparkLend',
    token_id: 269,
    token_symbol: 'spWETH',
    action_type: 'in',
    tx_amount: '412.008881230000000000',
    balance: '32065.742209491845604541',
    tx_hash:
      '0x126e169d6397d685442941aba1b450910b347346f271505d5c5b514227548602',
    log_index: 33,
  },
  {
    chain_id: 1,
    prime_address: SPARK_MAINNET_PROXY,
    prime_name: 'spark',
    protocol_name: 'SparkLend',
    token_id: 735,
    token_symbol: 'spPYUSD',
    action_type: 'sweep',
    tx_amount: '0.000000',
    balance: '100000478.973363',
    tx_hash: null,
    log_index: 0,
  },
  {
    chain_id: 1,
    prime_address: GROVE_MAINNET_PROXY,
    prime_name: 'grove',
    protocol_name: 'SparkLend',
    token_id: 736,
    token_symbol: 'spUSDS',
    action_type: 'in',
    tx_amount: '4500000.000000000000000000',
    balance: '124500000.000000000000000000',
    tx_hash:
      '0x9b32ceb4acb469179f5893e82e089b09d2ddd9d782d1198e2a71518b7a393edb',
    log_index: 66,
  },
  {
    chain_id: 1,
    prime_address: SPARK_MAINNET_PROXY,
    prime_name: 'spark',
    protocol_name: 'maple',
    token_id: 850711,
    token_symbol: 'syrupUSDC',
    action_type: 'in',
    tx_amount: '0.000001',
    balance: '0.000001',
    tx_hash:
      '0xfede11d43e04f6582391f9fffac25287d6efde8ff99492353396d174746df83a',
    log_index: 147,
  },
  {
    chain_id: 1,
    prime_address: SPARK_MAINNET_PROXY,
    prime_name: 'spark',
    protocol_name: 'Morpho Blue',
    token_id: 892750,
    token_symbol: 'spDAI',
    action_type: 'out',
    tx_amount: '120.000000000000000000',
    balance: '401.044942988323179297',
    tx_hash:
      '0x2072429a964b809fece82a2128b317fc36178999359307daecb6cd5e7dcd9ca6',
    log_index: 8,
  },
  {
    chain_id: 43114,
    prime_address: SPARK_AVALANCHE_PROXY,
    prime_name: 'spark',
    protocol_name: 'Aave V3 Avalanche',
    token_id: 1302,
    token_symbol: 'aAvaUSDC',
    action_type: 'sweep',
    tx_amount: '0.000000',
    balance: '0.156275',
    tx_hash: null,
    log_index: 0,
  },
  {
    chain_id: 1,
    prime_address: SPARK_MAINNET_PROXY,
    prime_name: 'spark',
    protocol_name: 'SparkLend',
    token_id: 736,
    token_symbol: 'spUSDS',
    action_type: 'out',
    tx_amount: '8750000.000000000000000000',
    balance: '833274608.502433178884234581',
    tx_hash:
      '0xd5533f09130e82832b270e38378184de68cff2ef1c4acaa8563cb48366cbee14',
    log_index: 175,
  },
  {
    chain_id: 1,
    prime_address: GROVE_MAINNET_PROXY,
    prime_name: 'grove',
    protocol_name: null,
    token_id: 12,
    token_symbol: 'sUSDS',
    action_type: 'in',
    tx_amount: '1000000.000000000000000000',
    balance: '38119004.221100000000000000',
    tx_hash:
      '0x7b742cd15a4bcad89d4755af0096949018db4597a5e6a2e9c66f299ce1e1314f',
    log_index: 24,
  },
  {
    chain_id: 1,
    prime_address: SPARK_MAINNET_PROXY,
    prime_name: 'spark',
    protocol_name: 'SparkLend',
    token_id: 338,
    token_symbol: 'spUSDT',
    action_type: 'sweep',
    tx_amount: '0.000000',
    balance: '346253142.672431',
    tx_hash: null,
    log_index: 0,
  },
];

export function seedActivity(nowMs: number): AllocationActivity[] {
  // Annotated on the callback, not just on the function: a `.map()` result is
  // checked for assignability, which lets a field the document dropped stay in
  // the row. See the same annotation on every response-row `.map()` here.
  return ACTIVITY_ROWS.map((row, index): AllocationActivity => ({
    ...row,
    block_number: 25780912 - index * 110,
    block_version: 0,
    created_at: offsetIsoAgo(
      nowMs,
      LAST_SWEEP_AGO + index * ACTIVITY_ROW_SPACING,
    ),
  }));
}
