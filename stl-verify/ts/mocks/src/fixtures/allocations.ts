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
 * the dashboard quietly rendering holes. The first of them is structural here:
 * the addresses, symbols and chain a row would otherwise repeat beside its ids
 * are read off `TOKENS` by id, so a row cannot name one token and label itself
 * with another's symbol.
 */
import {
  DAY_MS,
  MINUTE_MS,
  REFERENCE_SYNCED_AGO_MS,
  SECOND_MS,
  iso,
  offsetIsoAgo,
} from '../clock.ts';
import { positionKeys } from '../identity.ts';
import type { Allocation, AllocationActivity } from '../schema.ts';
import type { PrimeName } from './registry.ts';
import {
  GROVE_MAINNET_PROXY,
  PRIMES,
  SPARK_AVALANCHE_PROXY,
  SPARK_MAINNET_PROXY,
  tokenById,
  tokenSymbol,
} from './registry.ts';

const LAST_SWEEP_AGO = 5 * MINUTE_MS + 13 * SECOND_MS;

const LAST_TRANSFER_AGO = 85 * SECOND_MS;
/** The custody snapshot is two months stale on purpose; see DATA_SOURCES. */
const CUSTODY_SNAPSHOT_AGO = 63 * DAY_MS;

export const SPARK_TX_HASH =
  '0x4e5c1fe085a5268abad4016609435ccc07a45527d9cf54b654d9a07cf324d932';

/** A position row minus everything `position` reads off its token ids. */
type PositionSeed = Omit<
  Allocation,
  | 'chain_id'
  | 'receipt_token_address'
  | 'source'
  | 'symbol'
  | 'underlying_symbol'
  | 'underlying_token_address'
  | 'underlying_token_id'
> & {
  /** Required: a row's chain and its `underlying_symbol` are read off it. */
  underlying_token_id: NonNullable<Allocation['underlying_token_id']>;
  /** Defaults to `indexed`: these are STL's own rows unless a case says otherwise. */
  source?: Allocation['source'];
};

function position(seed: PositionSeed): Allocation {
  const underlying = tokenById(seed.underlying_token_id);
  const receiptTokenId = seed.receipt_token_id ?? null;

  const receiptTokenAddress =
    receiptTokenId === null ? null : tokenById(receiptTokenId).address;

  return {
    ...seed,
    chain_id: underlying.chain_id,
    source: seed.source ?? 'indexed',
    position_keys: positionKeys({
      chain_id: underlying.chain_id,
      position_address: receiptTokenAddress ?? underlying.address,
      receipt_token_id: receiptTokenId,
      protocol_name: seed.protocol_name,
      symbol: tokenSymbol(receiptTokenId ?? seed.underlying_token_id),
    }),
    receipt_token_address: receiptTokenAddress,
    // A wrapped position is labelled by its receipt token, a direct holding by
    // the asset itself.
    symbol: tokenSymbol(receiptTokenId ?? seed.underlying_token_id),
    underlying_symbol: tokenSymbol(seed.underlying_token_id),
    underlying_token_address: underlying.address,
  };
}

/** Rows one sweep left behind: a single instant, a single action, proxy scope. */
type SweptSeed = Omit<
  PositionSeed,
  'latest_activity_at' | 'latest_activity_action' | 'scope'
>;

function swept(sweptAt: string, rows: readonly SweptSeed[]): Allocation[] {
  return rows.map((row) =>
    position({
      ...row,
      latest_activity_at: sweptAt,
      latest_activity_action: 'sweep',
      scope: 'proxy',
    }),
  );
}

function sparkMainnetAllocations(nowMs: number): Allocation[] {
  return [
    ...swept(offsetIsoAgo(nowMs, LAST_SWEEP_AGO), [
      {
        receipt_token_id: 736,
        underlying_token_id: 13,
        protocol_name: 'SparkLend',
        balance: '842029895.945548368376556578',
        amount_usd: '841904871.346598373354820026',
        latest_activity_amount: '0.000000000000000000',
        category: 'allocation',
      },
      {
        receipt_token_id: 338,
        underlying_token_id: 9,
        protocol_name: 'SparkLend',
        balance: '347003142.672431',
        amount_usd: '346708318.392322222449470000',
        latest_activity_amount: '0.000000',
        category: 'allocation',
      },
      {
        receipt_token_id: 723,
        underlying_token_id: 1,
        protocol_name: 'SparkLend',
        balance: '296139496.545005544597463815',
        amount_usd: '296086123.323543238248161014',
        latest_activity_amount: '0.000000000000000000',
        category: 'allocation',
      },
      {
        receipt_token_id: 735,
        underlying_token_id: 18,
        protocol_name: 'SparkLend',
        balance: '100000478.973363',
        amount_usd: '100009872.018352967986590000',
        latest_activity_amount: '0.000000',
        category: 'allocation',
      },
      {
        receipt_token_id: 269,
        underlying_token_id: 4,
        protocol_name: 'SparkLend',
        balance: '32065.742209491845604541',
        amount_usd: '60851479.648374770322193501',
        latest_activity_amount: '0.000000000000000000',
        category: 'allocation',
      },
      {
        receipt_token_id: 885660,
        underlying_token_id: 3,
        protocol_name: 'Morpho Blue',
        balance: '8758007.042993599249382474',
        amount_usd: '9057828.812411176980000000',
        latest_activity_amount: '0.000000000000000000',
        category: 'allocation',
      },
      // The dust position: four figures of USD against eight-figure siblings.
      {
        receipt_token_id: 892750,
        underlying_token_id: 1,
        protocol_name: 'Morpho Blue',
        balance: '401.044942988323179297',
        amount_usd: '839.944852318554772032',
        latest_activity_amount: '0.000000000000000000',
        category: 'allocation',
      },
      {
        receipt_token_id: 34,
        underlying_token_id: 9,
        protocol_name: 'Aave V3',
        balance: '5.946339',
        amount_usd: '5.941286811995430000',
        latest_activity_amount: '0.000000',
        category: 'allocation',
      },
      // The unpriceable one: risk capital reports `applied: false` for it.
      {
        receipt_token_id: 850711,
        underlying_token_id: 3,
        protocol_name: 'maple',
        balance: '0.000001',
        amount_usd: '0.000000999870000000',
        latest_activity_amount: '0.000000',
        category: 'allocation',
      },
      {
        receipt_token_id: null,
        underlying_token_id: 12,
        protocol_name: null,
        balance: '617652055.510293017784331115',
        amount_usd: '683515036.564482027971673304',
        latest_activity_amount: '0.000000000000000000',
        category: 'asset',
      },
      {
        receipt_token_id: null,
        underlying_token_id: 18,
        protocol_name: null,
        balance: '238780734.151812',
        amount_usd: '238687485.499511034377760000',
        latest_activity_amount: '0.000000',
        category: 'asset',
      },
    ]),
    // The one row a transfer touched rather than the sweep.
    position({
      receipt_token_id: null,
      underlying_token_id: 9,
      protocol_name: null,
      balance: '4640.274995',
      amount_usd: '4635.170692505500000000',
      latest_activity_at: offsetIsoAgo(nowMs, LAST_TRANSFER_AGO),
      latest_activity_action: 'in',
      latest_activity_amount: '4640.274995',
      category: 'asset',
      scope: 'proxy',
    }),
    // The custody leg stays a literal: off-chain BTC has no token-registry row
    // to read a chain, an address or a symbol off.
    {
      chain_id: 0,
      source: 'indexed',
      receipt_token_id: null,
      receipt_token_address: null,
      underlying_token_id: null,
      underlying_token_address: null,
      symbol: 'BTC',
      underlying_symbol: 'BTC',
      protocol_name: 'anchorage',
      // Its protocol is the only thing the two provenances describe the same
      // way: Sky reports the leg on ethereum under its own symbol, with an
      // address.
      position_keys: ['custody:anchorage'],
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
  return swept(offsetIsoAgo(nowMs, LAST_SWEEP_AGO), [
    {
      receipt_token_id: 1302,
      underlying_token_id: 1301,
      protocol_name: 'Aave V3 Avalanche',
      balance: '0.156275',
      amount_usd: '0.156324898675310000',
      latest_activity_amount: '0.000000',
      category: 'allocation',
    },
  ]);
}

function groveMainnetAllocations(nowMs: number): Allocation[] {
  return swept(offsetIsoAgo(nowMs, LAST_SWEEP_AGO), [
    {
      receipt_token_id: 736,
      underlying_token_id: 13,
      protocol_name: 'SparkLend',
      balance: '124500000.000000000000000000',
      amount_usd: '124481521.310000000000000000',
      latest_activity_amount: '0.000000000000000000',
      category: 'allocation',
    },
    {
      receipt_token_id: null,
      underlying_token_id: 12,
      protocol_name: null,
      balance: '38119004.221100000000000000',
      amount_usd: '42184992.760900000000000000',
      latest_activity_amount: '0.000000000000000000',
      category: 'asset',
    },
  ]);
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
 *
 * `selfRows` echo STL's own indexed positions, so their `underlying_*` already
 * matches what the real API resolves through its registry for a row it
 * indexes — no separate enrichment needed here. `skyOnlyAllocations` carries
 * the other half of the contract: rows STL does not index at all, whose
 * `underlying_*` stay null/`''`.
 */
export function seedReferenceAllocations(
  nowMs: number,
  primeName: PrimeName,
): Allocation[] | undefined {
  const selfRows =
    primeName === 'spark'
      ? sparkMainnetAllocations(nowMs)
      : groveMainnetAllocations(nowMs);

  return [
    ...selfRows
      .filter((allocation) => allocation.category === 'allocation')
      .map((allocation): Allocation => ({
        ...allocation,
        balance: null,
        scope: 'prime',
        source: 'reference',
        reference_synced_at: iso(nowMs - REFERENCE_SYNCED_AGO_MS),
      })),
    ...skyOnlyAllocations(nowMs, primeName),
  ];
}

/**
 * Positions Sky reports and STL does not index at all.
 *
 * The union is only interesting if one side has rows the other lacks, and these
 * carry the three properties that make them awkward: no receipt token (so no
 * `receipt_token_id` to join a risk row by), no token quantity, and — for the
 * Arkis vault — an exposure large enough to matter against STL's own totals.
 * `underlying_symbol` stays `''` here, matching the real API: these positions
 * do not resolve against STL's registry, so nothing is there to name them with.
 */
/** The Arkis vault Sky reports and STL does not index. */
const ARKIS_VAULT = '0x38464507e02c983f20428a6e8566693fe9e422a9';

function skyOnlyAllocations(nowMs: number, primeName: PrimeName): Allocation[] {
  if (primeName !== 'spark') return [];

  const observedAt = iso(nowMs - 13 * MINUTE_MS);
  const syncedAt = iso(nowMs - REFERENCE_SYNCED_AGO_MS);
  return [
    {
      chain_id: 1,
      network: 'ethereum',
      receipt_token_id: null,
      receipt_token_address: null,
      underlying_token_id: null,
      underlying_token_address: null,
      symbol: 'sparkPrimeUSDC1',
      // Unresolved against STL's receipt-token registry, like the id/address
      // above: this feed names no underlying of its own.
      underlying_symbol: '',
      protocol_name: 'Arkis',
      position_keys: [`position:1:${ARKIS_VAULT}`],
      balance: null,
      amount_usd: '20286862.977',
      latest_activity_at: observedAt,
      latest_activity_action: null,
      latest_activity_amount: null,
      category: 'allocation',
      scope: 'prime',
      source: 'reference',
      reference_synced_at: syncedAt,
    },
    {
      chain_id: 1,
      network: 'ethereum',
      receipt_token_id: null,
      receipt_token_address: null,
      underlying_token_id: null,
      underlying_token_address: null,
      symbol: 'UNI-V4-PYUSD-USDS',
      underlying_symbol: '',
      protocol_name: 'uniswap',
      // A pool id is not an address, so this one can only key on its symbol.
      position_keys: ['symbol:1:uniswap:uni-v4-pyusd-usds'],
      balance: null,
      amount_usd: '100118500.444',
      latest_activity_at: observedAt,
      latest_activity_action: null,
      latest_activity_amount: null,
      category: 'allocation',
      scope: 'prime',
      source: 'reference',
      reference_synced_at: syncedAt,
    },
  ];
}

/**
 * Every position either provenance reports, each named once.
 *
 * STL's row wins where both describe the same position — it is computed from the
 * chain rather than reported — and says `both` so the reader knows the other
 * agrees it exists. Rows only Sky reports keep their own provenance, which is
 * what the grid badges.
 */
export function seedCompositeAllocations(
  nowMs: number,
  primeName: PrimeName,
  proxyAddress: string,
): Allocation[] {
  const indexed = seedAllocations(nowMs)[proxyAddress] ?? [];
  const referenceRows = seedReferenceAllocations(nowMs, primeName) ?? [];

  const indexedIds = new Set(
    indexed
      .map((allocation) => allocation.receipt_token_id)
      // `!= null` covers undefined too: the field is optional in the document,
      // so a row that omits it reads as absent rather than as id 0.
      .filter((id): id is number => id != null),
  );

  return [
    ...indexed.map((allocation): Allocation => {
      const alsoReported =
        allocation.receipt_token_id != null &&
        referenceRows.some(
          (row) => row.receipt_token_id === allocation.receipt_token_id,
        );
      const reported = alsoReported
        ? referenceRows.find(
            (row) => row.receipt_token_id === allocation.receipt_token_id,
          )
        : undefined;
      return {
        ...allocation,
        source: reported ? 'both' : 'indexed',
        // Sky's figure and the cycle it was observed at travel together: the
        // API sets both in one copy, so a stamp beside an empty comparison
        // cell is a state staging cannot produce.
        reference_amount_usd: reported?.amount_usd ?? null,
        reference_synced_at: reported?.reference_synced_at ?? null,
      };
    }),
    ...referenceRows.filter(
      (row) =>
        row.receipt_token_id == null || !indexedIds.has(row.receipt_token_id),
    ),
  ];
}

/** Spread so 16 rows cover the whole default 24h window, newest first. */
const ACTIVITY_ROW_SPACING = 85 * MINUTE_MS;

/** Everything but the three fields the clock owns and the three the registry does. */
type ActivityRowSeed = Omit<
  AllocationActivity,
  | 'block_number'
  | 'block_version'
  | 'chain_id'
  | 'created_at'
  | 'prime_name'
  | 'token_symbol'
>;

/** A proxy names its prime, so a row cannot label itself with the other's. */
function primeNameOf(address: AllocationActivity['prime_address']): PrimeName {
  const prime = PRIMES.find((row) => row.address === address);
  if (prime === undefined) {
    throw new Error(`no PRIMES row for proxy ${address}`);
  }

  return prime.name;
}

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
    prime_address: SPARK_MAINNET_PROXY,
    protocol_name: 'SparkLend',
    token_id: 736,
    action_type: 'sweep',
    tx_amount: '0.000000000000000000',
    balance: '842029895.945548368376556578',
    tx_hash: null,
    log_index: 0,
  },
  {
    prime_address: SPARK_MAINNET_PROXY,
    protocol_name: null,
    token_id: 9,
    action_type: 'in',
    tx_amount: '4640.274995',
    balance: '4640.274995',
    tx_hash:
      '0x5be036b81b1c79e4acdfd63ceafdc25e1a00f5cb4d0e87120665c4927764fbfc',
    log_index: 118,
  },
  {
    prime_address: SPARK_MAINNET_PROXY,
    protocol_name: 'SparkLend',
    token_id: 736,
    action_type: 'in',
    tx_amount: '23882.033557750140214353',
    balance: '842024608.502433178884234581',
    tx_hash: SPARK_TX_HASH,
    log_index: 242,
  },
  {
    prime_address: SPARK_MAINNET_PROXY,
    protocol_name: 'Morpho Blue',
    token_id: 885660,
    action_type: 'in',
    tx_amount: '1500000.000000',
    balance: '8758007.042993599249382474',
    tx_hash:
      '0x6ee15ae58c284dd3827ce7924e9e6fede5fc76d756e852441a32e3673f813a95',
    log_index: 57,
  },
  {
    prime_address: SPARK_MAINNET_PROXY,
    protocol_name: 'SparkLend',
    token_id: 338,
    action_type: 'out',
    tx_amount: '750000.000000',
    balance: '347003142.672431',
    tx_hash:
      '0x43019395d99015a53120b8dea9aa964ff4ff6c4ac2437c3ed00a13eae61b227b',
    log_index: 91,
  },
  {
    prime_address: SPARK_MAINNET_PROXY,
    protocol_name: 'SparkLend',
    token_id: 723,
    action_type: 'in',
    tx_amount: '2250000.000000000000000000',
    balance: '296139496.545005544597463815',
    tx_hash:
      '0xe3bc0428879bec38409a0b0552592a2ffb40dd6ea7352e7ad980c901bf760380',
    log_index: 12,
  },
  {
    prime_address: SPARK_MAINNET_PROXY,
    protocol_name: 'Aave V3',
    token_id: 34,
    action_type: 'out',
    tx_amount: '5.946339',
    balance: '5.946339',
    tx_hash:
      '0x7df714df26c05d09bea46cbd0c843b625abfcae9d235f76ffaf7a629d1e979c8',
    log_index: 204,
  },
  {
    prime_address: SPARK_MAINNET_PROXY,
    protocol_name: 'SparkLend',
    token_id: 269,
    action_type: 'in',
    tx_amount: '412.008881230000000000',
    balance: '32065.742209491845604541',
    tx_hash:
      '0x126e169d6397d685442941aba1b450910b347346f271505d5c5b514227548602',
    log_index: 33,
  },
  {
    prime_address: SPARK_MAINNET_PROXY,
    protocol_name: 'SparkLend',
    token_id: 735,
    action_type: 'sweep',
    tx_amount: '0.000000',
    balance: '100000478.973363',
    tx_hash: null,
    log_index: 0,
  },
  {
    prime_address: GROVE_MAINNET_PROXY,
    protocol_name: 'SparkLend',
    token_id: 736,
    action_type: 'in',
    tx_amount: '4500000.000000000000000000',
    balance: '124500000.000000000000000000',
    tx_hash:
      '0x9b32ceb4acb469179f5893e82e089b09d2ddd9d782d1198e2a71518b7a393edb',
    log_index: 66,
  },
  {
    prime_address: SPARK_MAINNET_PROXY,
    protocol_name: 'maple',
    token_id: 850711,
    action_type: 'in',
    tx_amount: '0.000001',
    balance: '0.000001',
    tx_hash:
      '0xfede11d43e04f6582391f9fffac25287d6efde8ff99492353396d174746df83a',
    log_index: 147,
  },
  {
    prime_address: SPARK_MAINNET_PROXY,
    protocol_name: 'Morpho Blue',
    token_id: 892750,
    action_type: 'out',
    tx_amount: '120.000000000000000000',
    balance: '401.044942988323179297',
    tx_hash:
      '0x2072429a964b809fece82a2128b317fc36178999359307daecb6cd5e7dcd9ca6',
    log_index: 8,
  },
  {
    prime_address: SPARK_AVALANCHE_PROXY,
    protocol_name: 'Aave V3 Avalanche',
    token_id: 1302,
    action_type: 'sweep',
    tx_amount: '0.000000',
    balance: '0.156275',
    tx_hash: null,
    log_index: 0,
  },
  {
    prime_address: SPARK_MAINNET_PROXY,
    protocol_name: 'SparkLend',
    token_id: 736,
    action_type: 'out',
    tx_amount: '8750000.000000000000000000',
    balance: '833274608.502433178884234581',
    tx_hash:
      '0xd5533f09130e82832b270e38378184de68cff2ef1c4acaa8563cb48366cbee14',
    log_index: 175,
  },
  {
    prime_address: GROVE_MAINNET_PROXY,
    protocol_name: null,
    token_id: 12,
    action_type: 'in',
    tx_amount: '1000000.000000000000000000',
    balance: '38119004.221100000000000000',
    tx_hash:
      '0x7b742cd15a4bcad89d4755af0096949018db4597a5e6a2e9c66f299ce1e1314f',
    log_index: 24,
  },
  {
    prime_address: SPARK_MAINNET_PROXY,
    protocol_name: 'SparkLend',
    token_id: 338,
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
    chain_id: tokenById(row.token_id).chain_id,
    prime_name: primeNameOf(row.prime_address),
    token_symbol: tokenSymbol(row.token_id),
    block_number: 25780912 - index * 110,
    block_version: 0,
    created_at: offsetIsoAgo(
      nowMs,
      LAST_SWEEP_AGO + index * ACTIVITY_ROW_SPACING,
    ),
  }));
}
