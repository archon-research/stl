/**
 * Risk capital, the collateral breakdown behind a receipt token, and RRC.
 *
 * Every figure is verbatim from staging's spark mainnet capture except where a
 * row was dropped; `per_allocation` is trimmed to the nine allocations the
 * allocation fixture keeps, so the two tables reconcile on screen. The `prime_*`
 * totals are staging's and therefore still describe the full 14-allocation
 * prime — the same "totals exceed the visible rows" relationship the real API
 * has whenever a chain is unindexed, which is what `prime_unserved_chains` is
 * for.
 */
import { MINUTE_MS, isoAgo } from '../clock.ts';
import { positionKeys } from '../identity.ts';
import { ownEntry } from '../lookup.ts';
import type {
  AllocationRiskCapital,
  CapitalMetrics,
  ChainRiskCapital,
  PrimeRiskCapital,
  RiskBreakdown,
  RiskBreakdownItem,
  RrcEnvelope,
} from '../schema.ts';
import {
  GROVE_AVALANCHE_PROXY,
  GROVE_BASE_PROXY,
  GROVE_MAINNET_PROXY,
  GROVE_VAULT,
  SPARK_AVALANCHE_PROXY,
  SPARK_BASE_PROXY,
  SPARK_MAINNET_PROXY,
  SPARK_VAULT,
  SPUSDS,
  TOKENS,
  tokenSymbol,
} from './registry.ts';

const SPARK_PROXIES = [
  SPARK_MAINNET_PROXY,
  SPARK_BASE_PROXY,
  '0x345e368fccd62266b3f5f37c9a131fd1c39f5869',
  '0x876664f0c9ff24d1aa355ce9f1680ae1a5bf36fb',
  '0x92afd6f2385a90e44da3a8b60fe36f6cbe1d8709',
  SPARK_AVALANCHE_PROXY,
];

/**
 * A priced row: `applied`, `model` and `unpriced_reason` are the same on all of
 * them, and `symbol` is the receipt token's, so the builder fills those four and
 * the table carries only the five columns a row varies in. The unpriced row
 * below cannot go through it and stays a literal.
 */
type PerAllocationRow = readonly [
  receipt_token_id: NonNullable<AllocationRiskCapital['receipt_token_id']>,
  protocol_name: AllocationRiskCapital['protocol_name'],
  exposure_usd: AllocationRiskCapital['exposure_usd'],
  required_risk_capital_usd: NonNullable<
    AllocationRiskCapital['required_risk_capital_usd']
  >,
  crr_pct: NonNullable<AllocationRiskCapital['crr_pct']>,
];

function pricedAllocation(row: PerAllocationRow): AllocationRiskCapital {
  const [receiptTokenId, protocolName, exposureUsd, rrcUsd, crrPct] = row;

  return {
    receipt_token_id: receiptTokenId,
    position_keys: positionKeys({
      chain_id: null,
      position_address: null,
      receipt_token_id: receiptTokenId,
      protocol_name: protocolName,
      symbol: tokenSymbol(receiptTokenId),
    }),
    // STL's own model; a merged-mode fixture would say otherwise.
    source: 'indexed',
    symbol: tokenSymbol(receiptTokenId),
    protocol_name: protocolName,
    exposure_usd: exposureUsd,
    applied: true,
    required_risk_capital_usd: rrcUsd,
    crr_pct: crrPct,
    model: 'gap_sweep',
    unpriced_reason: null,
  };
}

const SPARK_PRICED_ROWS: readonly PerAllocationRow[] = [
  [736, 'SparkLend', '841904871.346598373354820026', '23308466.81', '4.47'],
  [338, 'SparkLend', '346708318.392322222449470000', '10442084.54', '3.63'],
  [723, 'SparkLend', '296086123.323543238248161014', '10123665.27', '5.40'],
  [735, 'SparkLend', '100009872.018352967986590000', '312255.16', '3.43'],
  [885660, 'Morpho Blue', '9057828.812411176980000000', '435169.05', '5.31'],
  [269, 'SparkLend', '60851479.648374770322193501', '1469.59', '5.35'],
  [892750, 'Morpho Blue', '839.944852318554772032', '0.00', '0.00'],
  [34, 'Aave V3', '5.941286811995430000', '0.06', '1.13'],
];

const SPARK_PER_ALLOCATION: readonly AllocationRiskCapital[] = [
  ...SPARK_PRICED_ROWS.map(pricedAllocation),
  // The one unpriced row, and the reason the UI needs an `applied: false` state.
  {
    receipt_token_id: 850711,
    source: 'indexed',
    symbol: 'syrupUSDC',
    protocol_name: 'maple',
    exposure_usd: '0.000000999870000000',
    applied: false,
    required_risk_capital_usd: null,
    crr_pct: null,
    model: 'gap_sweep',
    unpriced_reason: 'no_model',
  },
];

const GROVE_PRICED_ROWS: readonly PerAllocationRow[] = [
  [736, 'SparkLend', '124481521.310000000000000000', '5564324.20', '4.47'],
];

const GROVE_PER_ALLOCATION: readonly AllocationRiskCapital[] =
  GROVE_PRICED_ROWS.map(pricedAllocation);

type PerChainRow = readonly [
  proxy_address: ChainRiskCapital['proxy_address'],
  chain: ChainRiskCapital['chain'],
  exposure_usd: ChainRiskCapital['exposure_usd'],
  required_risk_capital_usd: ChainRiskCapital['required_risk_capital_usd'],
  allocation_count: ChainRiskCapital['allocation_count'],
];

function perChain(rows: readonly PerChainRow[]): ChainRiskCapital[] {
  return rows.map(([proxy, chain, exposure, rrc, count]): ChainRiskCapital => ({
    proxy_address: proxy,
    chain,
    exposure_usd: exposure,
    required_risk_capital_usd: rrc,
    allocation_count: count,
  }));
}

/**
 * Three shapes in one table: indexed with exposure, indexed and empty, and
 * unindexed (`null`, not `'0'`) — the distinction the UI must not flatten.
 */
const SPARK_PER_CHAIN_ROWS: readonly PerChainRow[] = [
  [
    SPARK_MAINNET_PROXY,
    'mainnet',
    '1656538061.841276418798473974',
    '44692696.19',
    14,
  ],
  [SPARK_BASE_PROXY, 'base', '0', '0', 0],
  ['0x345e368fccd62266b3f5f37c9a131fd1c39f5869', 'unichain', null, null, null],
  ['0x876664f0c9ff24d1aa355ce9f1680ae1a5bf36fb', 'optimism', null, null, null],
  ['0x92afd6f2385a90e44da3a8b60fe36f6cbe1d8709', 'arbitrum', null, null, null],
  [SPARK_AVALANCHE_PROXY, 'avalanche-c', '0.156324898675310000', '0', 1],
];

const GROVE_PER_CHAIN_ROWS: readonly PerChainRow[] = [
  [
    GROVE_MAINNET_PROXY,
    'mainnet',
    '124481521.310000000000000000',
    '5564324.20',
    1,
  ],
  [GROVE_BASE_PROXY, 'base', '0', '0', 0],
  [GROVE_AVALANCHE_PROXY, 'avalanche-c', '0', '0', 0],
];

const SPARK_RISK_CAPITAL: PrimeRiskCapital = {
  prime_id: SPARK_MAINNET_PROXY,
  proxy_address: SPARK_MAINNET_PROXY,
  model: 'gap_sweep',
  source: 'indexed',
  exposure_usd: '1656538061.841276418798473974',
  total_risk_capital_usd: '48142491.085806286854722044',
  required_risk_capital_usd: '44692696.19',
  encumbrance_ratio: '0.9283',
  modeled_exposure_usd: '1656538061.841275418928473974',
  modeled_pct: '1.0000',
  per_allocation: [...SPARK_PER_ALLOCATION],
  prime_name: 'spark',
  prime_exposure_usd: '1656538061.997601317473783974',
  prime_required_risk_capital_usd: '44692696.19',
  prime_modeled_exposure_usd: '1656538061.841275418928473974',
  prime_modeled_pct: '1.0000',
  prime_encumbrance_ratio: '0.9283',
  prime_proxies: SPARK_PROXIES,
  prime_per_chain: perChain(SPARK_PER_CHAIN_ROWS),
  prime_unserved_chains: ['unichain', 'optimism', 'arbitrum'],
};

const GROVE_RISK_CAPITAL: PrimeRiskCapital = {
  prime_id: GROVE_MAINNET_PROXY,
  proxy_address: GROVE_MAINNET_PROXY,
  model: 'gap_sweep',
  source: 'indexed',
  exposure_usd: '124481521.310000000000000000',
  total_risk_capital_usd: '9204118.400000000000000000',
  required_risk_capital_usd: '5564324.20',
  encumbrance_ratio: '0.6045',
  modeled_exposure_usd: '124481521.310000000000000000',
  modeled_pct: '1.0000',
  per_allocation: [...GROVE_PER_ALLOCATION],
  prime_name: 'grove',
  prime_exposure_usd: '124481521.310000000000000000',
  prime_required_risk_capital_usd: '5564324.20',
  prime_modeled_exposure_usd: '124481521.310000000000000000',
  prime_modeled_pct: '1.0000',
  prime_encumbrance_ratio: '0.6045',
  prime_proxies: [GROVE_MAINNET_PROXY, GROVE_BASE_PROXY, GROVE_AVALANCHE_PROXY],
  prime_per_chain: perChain(GROVE_PER_CHAIN_ROWS),
  prime_unserved_chains: [],
};

/**
 * Every ALM proxy of a prime answers with that prime's figures, per the
 * endpoint's `prime_*` contract. Keyed by lower-cased proxy address.
 */
export const RISK_CAPITAL_BY_PROXY: Readonly<Record<string, PrimeRiskCapital>> =
  {
    [SPARK_MAINNET_PROXY]: SPARK_RISK_CAPITAL,
    [SPARK_BASE_PROXY]: {
      ...SPARK_RISK_CAPITAL,
      prime_id: SPARK_BASE_PROXY,
      proxy_address: SPARK_BASE_PROXY,
      exposure_usd: '0',
      required_risk_capital_usd: '0',
      modeled_exposure_usd: '0',
      per_allocation: [],
    },
    [SPARK_AVALANCHE_PROXY]: {
      ...SPARK_RISK_CAPITAL,
      prime_id: SPARK_AVALANCHE_PROXY,
      proxy_address: SPARK_AVALANCHE_PROXY,
      exposure_usd: '0.156324898675310000',
      required_risk_capital_usd: '0',
      modeled_exposure_usd: '0.156324898675310000',
      per_allocation: [],
    },
    [GROVE_MAINNET_PROXY]: GROVE_RISK_CAPITAL,
    [GROVE_BASE_PROXY]: {
      ...GROVE_RISK_CAPITAL,
      prime_id: GROVE_BASE_PROXY,
      proxy_address: GROVE_BASE_PROXY,
      exposure_usd: '0',
      required_risk_capital_usd: '0',
      modeled_exposure_usd: '0',
      per_allocation: [],
    },
    [GROVE_AVALANCHE_PROXY]: {
      ...GROVE_RISK_CAPITAL,
      prime_id: GROVE_AVALANCHE_PROXY,
      proxy_address: GROVE_AVALANCHE_PROXY,
      exposure_usd: '0',
      required_risk_capital_usd: '0',
      modeled_exposure_usd: '0',
      per_allocation: [],
    },
  };

/**
 * The tranche split as fractions of Total Risk Capital, not as figures.
 *
 * Spark's captured shares, applied to whichever prime is asked. Held as ratios
 * because the absolute figures are the one thing that cannot be shared: grove's
 * Total Risk Capital is 9.2M, so spark's 48.1M of tranches would have described
 * a prime holding five times its own capital in its first-loss layer.
 */
const JUNIOR_SHARE = 0.2596;
const JUNIOR_INTERNAL_SHARE = 0.6;
const JUNIOR_EXTERNAL_SHARE = 0.3;
const SENIOR_INTERNAL_SHARE = 0.6;

/**
 * Split in cents, at the precision these figures are published at: shares
 * rounded independently leave the parts a cent or two off the whole they split,
 * which is the same kind of body-that-cannot-be-true the shares exist to avoid.
 */
function centsOf(usd: string | null | undefined): number {
  return Math.round(Number(usd ?? '0') * 100);
}

function usdFigure(cents: number): string {
  return (cents / 100).toFixed(2);
}

/**
 * `reference=true` moves every figure to prime scope and fills the
 * reference-only fields. Derived rather than captured: the reference-mode
 * contract is "unprefixed equals prefixed", so expressing it as a transform
 * keeps the two halves from drifting apart in the fixture.
 */
/**
 * Sky's own per-allocation figures, captured from staging's spark composite
 * response.
 *
 * They exist to be *unlike* STL's, because that is the case the composite view
 * is for: Sky prices spUSDS at nothing where STL's model asks $23.3M, and the
 * two rows it prices highest — off-chain custody and the Arkis vault — are ones
 * STL resolves no receipt token for, so they carry a null id and cannot join to
 * a row on the allocations grid. A fixture where the two provenances agreed, or
 * where every Sky row had an id, would leave both facts untested.
 */
type ReferenceAllocationRow = readonly [
  receipt_token_id: AllocationRiskCapital['receipt_token_id'],
  symbol: AllocationRiskCapital['symbol'],
  protocol_name: AllocationRiskCapital['protocol_name'],
  exposure_usd: AllocationRiskCapital['exposure_usd'],
  required_risk_capital_usd: NonNullable<
    AllocationRiskCapital['required_risk_capital_usd']
  >,
  crr_pct: NonNullable<AllocationRiskCapital['crr_pct']>,
];

const SPARK_REFERENCE_ROWS: readonly ReferenceAllocationRow[] = [
  [736, 'spUSDS', 'SparkLend', '869783405.762', '0', '0'],
  [
    338,
    'spUSDT',
    'SparkLend',
    '354681648.458',
    '2369391.55517',
    '0.66778919177',
  ],
  [
    723,
    'spDAI',
    'SparkLend',
    '295574431.958',
    '0.09367882851',
    '0.00000003168',
  ],
  [735, 'spPYUSD', 'SparkLend', '100023505.149', '0', '0'],
  // Sky-only, and the two largest requirements in its model.
  [null, 'ANCHORAGE', 'anchorage', '210000001.360', '6300000.0408', '3.0'],
  [null, 'sparkPrimeUSDC1', 'Arkis', '20286862.977', '10143431.4886', '50.0'],
  [null, 'UNI-V4-PYUSD-USDS', 'uniswap', '100118500.444', '0', '0'],
];

const GROVE_REFERENCE_ROWS: readonly ReferenceAllocationRow[] = [
  [736, 'spUSDS', 'SparkLend', '124481521.310', '4102881.44', '3.29'],
  [null, 'ANCHORAGE', 'anchorage', '41000000.000', '1230000.00', '3.0'],
];

function referenceAllocation(
  row: ReferenceAllocationRow,
): AllocationRiskCapital {
  const [receiptTokenId, symbol, protocolName, exposureUsd, rrcUsd, crrPct] =
    row;

  return {
    receipt_token_id: receiptTokenId,
    position_keys: positionKeys({
      chain_id: 1,
      position_address: referenceAddressOf(receiptTokenId, symbol),
      receipt_token_id: receiptTokenId,
      protocol_name: protocolName,
      symbol,
    }),
    source: 'reference',
    symbol,
    protocol_name: protocolName,
    exposure_usd: exposureUsd,
    // Upstream publishes only positions it has priced, so every row is applied
    // and none carries a model name.
    applied: true,
    required_risk_capital_usd: rrcUsd,
    crr_pct: crrPct,
    model: null,
    unpriced_reason: null,
  };
}

/** Sum of decimal-string figures, kept in cents so the total is exact. */
function sumFigures(values: readonly string[]): string {
  return usdFigure(values.reduce((total, value) => total + centsOf(value), 0));
}

/**
 * Addresses for the positions Sky reports that STL resolves no receipt token
 * for. Without one they could only key on their symbol, and the Arkis vault is
 * the row whose $10.1M requirement makes the address join worth testing.
 */
const SKY_ONLY_ADDRESSES: Record<string, string> = {
  sparkPrimeUSDC1: '0x38464507e02c983f20428a6e8566693fe9e422a9',
};

/** The address a Sky row keys on: its receipt token's, else its own. */
function referenceAddressOf(
  receiptTokenId: number | null | undefined,
  symbol: string,
): string | null {
  if (receiptTokenId == null) return SKY_ONLY_ADDRESSES[symbol] ?? null;

  const token = TOKENS.find((row) => row.id === receiptTokenId);
  if (token === undefined) {
    throw new Error(`no TOKENS row for ${receiptTokenId}`);
  }
  return token.address;
}

function referenceRowsFor(
  self: PrimeRiskCapital,
): readonly ReferenceAllocationRow[] {
  return self.prime_name === 'grove'
    ? GROVE_REFERENCE_ROWS
    : SPARK_REFERENCE_ROWS;
}

/**
 * The merged breakdown: STL's row where both provenances have one, carrying
 * Sky's figures in its `reference_*` fields, plus the rows Sky alone reports.
 *
 * Ordered by exposure, largest first, like the endpoint — the two halves arrive
 * in their own orders and concatenating them would publish neither.
 */
function compositePerAllocation(
  self: PrimeRiskCapital,
): AllocationRiskCapital[] {
  const skyById = new Map<number, ReferenceAllocationRow>();
  const skyOnly: ReferenceAllocationRow[] = [];
  for (const row of referenceRowsFor(self)) {
    const [receiptTokenId] = row;
    if (receiptTokenId === null) {
      skyOnly.push(row);
    } else {
      skyById.set(receiptTokenId, row);
    }
  }

  const merged: AllocationRiskCapital[] = self.per_allocation.map((entry) => {
    const sky =
      entry.receipt_token_id === null
        ? undefined
        : skyById.get(entry.receipt_token_id);
    if (sky === undefined) return entry;

    const [, , , , rrcUsd, crrPct] = sky;
    return {
      ...entry,
      source: 'both',
      reference_exposure_usd: sky[3],
      reference_required_risk_capital_usd: rrcUsd,
      reference_crr_pct: crrPct,
    };
  });

  return [...merged, ...skyOnly.map(referenceAllocation)].sort(
    (left, right) => Number(right.exposure_usd) - Number(left.exposure_usd),
  );
}

/**
 * Both provenances at once: STL's totals in the bare fields, Sky's in the
 * `reference_`-prefixed ones, and one row per position.
 */
export function toCompositeRiskCapital(
  self: PrimeRiskCapital,
): PrimeRiskCapital {
  const reference = toReferenceRiskCapital(self);

  return {
    ...self,
    source: 'both',
    per_allocation: compositePerAllocation(self),
    reference_prime_exposure_usd: reference.prime_exposure_usd,
    reference_prime_required_risk_capital_usd:
      reference.prime_required_risk_capital_usd,
    reference_total_risk_capital_usd: reference.total_risk_capital_usd,
    reference_prime_encumbrance_ratio: reference.prime_encumbrance_ratio,
    // Sky reports these and STL models none of them, so the merged answer
    // carries them whole.
    junior_risk_capital_usd: reference.junior_risk_capital_usd,
    senior_risk_capital_usd: reference.senior_risk_capital_usd,
    exposure_share: reference.exposure_share,
  };
}

/** Sky's answer alone: its own totals, and its own breakdown. */
export function toReferenceRiskCapital(
  self: PrimeRiskCapital,
): PrimeRiskCapital {
  const total = centsOf(self.total_risk_capital_usd);
  const junior = Math.round(total * JUNIOR_SHARE);
  // Every residual layer takes what the layers above it leave, rather than its
  // own share, so each split adds up to the figure it splits.
  const senior = total - junior;
  const juniorInternal = Math.round(junior * JUNIOR_INTERNAL_SHARE);
  const juniorExternal = Math.round(junior * JUNIOR_EXTERNAL_SHARE);
  const seniorInternal = Math.round(senior * SENIOR_INTERNAL_SHARE);

  // Sky's totals are Sky's own rows summed, not STL's totals relabelled. Copying
  // STL's would make every share a position takes of the requirement wrong by
  // the ratio between the two models — 5% where Sky publishes 12%.
  const rows = referenceRowsFor(self);
  const primeExposureUsd = sumFigures(rows.map((row) => row[3]));
  const primeRequiredRiskCapitalUsd = sumFigures(rows.map((row) => row[4]));

  return {
    ...self,
    source: 'reference',
    model: null,
    per_allocation: rows.map(referenceAllocation),
    prime_exposure_usd: primeExposureUsd,
    prime_required_risk_capital_usd: primeRequiredRiskCapitalUsd,
    prime_modeled_exposure_usd: primeExposureUsd,
    exposure_usd: primeExposureUsd,
    required_risk_capital_usd: primeRequiredRiskCapitalUsd,
    modeled_exposure_usd: self.prime_modeled_exposure_usd,
    encumbrance_ratio: self.prime_encumbrance_ratio,
    junior_risk_capital_usd: usdFigure(junior),
    senior_risk_capital_usd: usdFigure(senior),
    internal_junior_risk_capital_usd: usdFigure(juniorInternal),
    internal_senior_risk_capital_usd: usdFigure(seniorInternal),
    external_junior_risk_capital_usd: usdFigure(juniorExternal),
    external_senior_risk_capital_usd: usdFigure(senior - seniorInternal),
    tokenized_junior_risk_capital_usd: usdFigure(
      junior - juniorInternal - juniorExternal,
    ),
    // Utilizations and the exposure share are ratios, so they carry no scale to
    // contradict and stay the captured values.
    epi_utilization: '0.8712',
    spj_utilization: '0.6431',
    exposure_share: '0.9302',
  };
}

/**
 * The collateral backing `spUSDS`, trimmed from 11 items to 6: the five that
 * carry visible weight plus one dust row, because `backing_pct: '0.0000'` on a
 * non-zero amount is its own formatting case.
 */
const SPUSDS_BREAKDOWN_ITEMS: readonly RiskBreakdownItem[] = [
  {
    token_id: 5,
    symbol: 'wstETH',
    amount: '110641.089909500359539079',
    backing_pct: '50.0614',
    amount_usd: '260751897.045212644607424526',
    price_usd: '2356.736518580000000000',
    liquidation_threshold: '0.84000000000000000000',
    liquidation_bonus: '1.07000000000000000000',
  },
  {
    token_id: 4,
    symbol: 'WETH',
    amount: '56794.346851094720177213',
    backing_pct: '20.6924',
    amount_usd: '107779199.962790961427500720',
    price_usd: '1897.710000000000000000',
    liquidation_threshold: '0.86000000000000000000',
    liquidation_bonus: '1.05000000000000000000',
  },
  {
    token_id: 11,
    symbol: 'cbBTC',
    amount: '942.144395696123479534',
    backing_pct: '11.6126',
    amount_usd: '60486070.615059298238572246',
    price_usd: '64200.425000000000000000',
    liquidation_threshold: '0.82000000000000000000',
    liquidation_bonus: '1.08000000000000000000',
  },
  {
    token_id: 6,
    symbol: 'WBTC',
    amount: '831.786171761002994193',
    backing_pct: '10.2567',
    amount_usd: '53423189.171609176516948958',
    price_usd: '64227.070592560000000000',
    liquidation_threshold: '0.78000000000000000000',
    liquidation_bonus: '1.07000000000000000000',
  },
  {
    token_id: 10,
    symbol: 'weETH',
    amount: '11621.046741519946224708',
    backing_pct: '4.6644',
    amount_usd: '24294966.149557162129232309',
    price_usd: '2090.600501825325400000',
    liquidation_threshold: '0.80000000000000000000',
    liquidation_bonus: '1.08000000000000000000',
  },
  {
    token_id: 2,
    symbol: 'sDAI',
    amount: '178.421357886855380116',
    backing_pct: '0.0000',
    amount_usd: '210.397121914198799509',
    price_usd: '1.179214890000000000',
    liquidation_threshold: '0.80000000000000000000',
    liquidation_bonus: '1.05000000000000000000',
  },
];

/**
 * Morpho Blue markets are isolated, so a receipt token there is backed by its
 * one market collateral rather than a shared reserve pool. Two items so the
 * drawer's list still looks like a list.
 */
const MORPHO_BREAKDOWN_ITEMS: readonly RiskBreakdownItem[] = [
  {
    token_id: 11,
    symbol: 'cbBTC',
    amount: '96.412580301994820000',
    backing_pct: '74.8120',
    amount_usd: '6190.741980000000000000',
    price_usd: '64200.425000000000000000',
    liquidation_threshold: '0.86000000000000000000',
    liquidation_bonus: '1.05000000000000000000',
  },
  {
    token_id: 12,
    symbol: 'sUSDS',
    amount: '1859402.940120000000000000',
    backing_pct: '25.1880',
    amount_usd: '2057870.221300000000000000',
    price_usd: '1.106634890000000000',
    liquidation_threshold: '0.91500000000000000000',
    liquidation_bonus: '1.04000000000000000000',
  },
];

/**
 * Keyed by lower-cased receipt-token address, as the endpoint's path is.
 *
 * The SparkLend receipt tokens share one reserve pool, so they share one
 * collateral composition — scaled to each token's own exposure rather than
 * copied, so the drawer's totals agree with the allocation row that opened it.
 * `aEthUSDT` and `syrupUSDC` are deliberately absent: the real API has nothing
 * to decompose for them, and the UI's not-available state needs a fixture too.
 */
export const RISK_BREAKDOWN_BY_TOKEN: Readonly<Record<string, RiskBreakdown>> =
  {
    [SPUSDS]: sharedPoolBreakdown(736, 1),
    // Exposure relative to spUSDS, from the allocation fixture's amount_usd.
    ['0xe7df13b8e3d6740fe17cbe928c7334243d86c92f']: sharedPoolBreakdown(
      338,
      0.41181,
    ),
    ['0x4dedf26112b3ec8ec46e7e31ea5e123490b05b8b']: sharedPoolBreakdown(
      723,
      0.35169,
    ),
    ['0x779224df1c756b4edd899854f32a53e8c2b2ce5d']: sharedPoolBreakdown(
      735,
      0.11879,
    ),
    ['0x59cd1c87501baa753d0b5b5ab5d8416a45cd71db']: sharedPoolBreakdown(
      269,
      0.07228,
    ),
    ['0x56a76b428244a50513ec81e225a293d128fd581d']: {
      receipt_token_id: 885660,
      items: [...MORPHO_BREAKDOWN_ITEMS],
    },
    ['0x73e65dbd630f90604062f6e02fab9138e713edd9']: {
      receipt_token_id: 892750,
      items: [...MORPHO_BREAKDOWN_ITEMS],
    },
  };

function sharedPoolBreakdown(
  receiptTokenId: number,
  exposureShare: number,
): RiskBreakdown {
  return {
    receipt_token_id: receiptTokenId,
    items: SPUSDS_BREAKDOWN_ITEMS.map((item) => scaleItem(item, exposureShare)),
  };
}

/** `backing_pct` is a share of the whole, so scaling amounts leaves it alone. */
function scaleItem(item: RiskBreakdownItem, share: number): RiskBreakdownItem {
  if (share === 1) {
    return item;
  }

  return {
    ...item,
    amount: (Number(item.amount) * share).toFixed(18),
    amount_usd: (Number(item.amount_usd) * share).toFixed(18),
  };
}

/**
 * `prime_id` scales the breakdown to that prime's pro-rata pool share. Spark
 * holds essentially all of the spUSDS pool, so its share is the identity.
 */
const PRIME_POOL_SHARE: Readonly<Record<string, number | undefined>> = {
  [SPARK_MAINNET_PROXY]: 1,
  [GROVE_MAINNET_PROXY]: 0.1288,
};

/**
 * `undefined` means no share data — which the endpoint answers as a 503 rather
 * than falling back to `1`, because `1` is the pool-level breakdown presented as
 * one prime's position: a confident answer that is wrong by the pool's size.
 */
export function poolShareFor(primeId: string | null): number | undefined {
  return primeId === null
    ? 1
    : ownEntry(PRIME_POOL_SHARE, primeId.toLowerCase());
}

// Any registry token gets a breakdown so the drawer works for every
// allocation row; the mapped entries above keep their curated exposures and
// the fallback reuses the shared pool composition at a mid-size share. A token
// outside the registry still misses, keeping the 404 branch reachable.
export function breakdownFor(tokenAddress: string): RiskBreakdown | undefined {
  const mapped = ownEntry(RISK_BREAKDOWN_BY_TOKEN, tokenAddress.toLowerCase());
  if (mapped !== undefined) {
    return mapped;
  }
  const token = TOKENS.find(
    (row) => row.address?.toLowerCase() === tokenAddress.toLowerCase(),
  );
  return token === undefined
    ? undefined
    : sharedPoolBreakdown(token.id, 0.24513);
}

export function scaleBreakdown(
  breakdown: RiskBreakdown,
  share: number,
): RiskBreakdown {
  if (share === 1) {
    return breakdown;
  }

  return {
    ...breakdown,
    items: breakdown.items.map((item) => scaleItem(item, share)),
  };
}

/** suraf's capital against gap_sweep's expected loss on the same asset. */
const SURAF_RATIO = 0.809;
/** The rating penalty suraf's adjusted CRR carries over its unadjusted one. */
const SURAF_PENALTY_PP = 0.5;

/** `max_rrc_usd` and `max_crr_pct` are each the largest across `results`. */
function largerDecimal(left: string, right: string): string {
  return Number(left) >= Number(right) ? left : right;
}

/**
 * RRC at default stress. No staging capture existed for this endpoint, so the
 * bodies are built from the schema: both registered models, `max_*` taken from
 * the larger of them, and `gap_sweep`'s `loss_usd` equal to its `rrc_usd` as
 * `GapSweepDetails` documents.
 *
 * `undefined` when the address is not a receipt token this fixture can decompose
 * — which includes `syrupUSDC`, carried elsewhere as `unpriced_reason:
 * 'no_model'`. Pricing an asset the risk-capital fixture reports as unpriceable
 * would have the two contradict each other.
 */
export function rrcEnvelope(
  primeId: string,
  chainId: number | null,
  tokenAddress: string | null,
): RrcEnvelope | undefined {
  if (chainId === null || tokenAddress === null) {
    return undefined;
  }
  const breakdown = breakdownFor(tokenAddress);
  if (breakdown === undefined) {
    return undefined;
  }
  const assetId = breakdown.receipt_token_id;
  // gap_sweep is the model risk-capital reports per allocation, so the two
  // endpoints agree on the same asset; suraf is scaled off it at a fixed ratio.
  // Assets outside the curated per-allocation rows get fixed mid-size numbers,
  // so every allocation's drawer resolves.
  const applied = SPARK_PER_ALLOCATION.find(
    (entry) => entry.receipt_token_id === assetId,
  );
  const gapSweepUsd = applied?.required_risk_capital_usd ?? '1834200.55';
  const gapSweepCrrPct = applied?.crr_pct ?? '2.31';
  const rrc = {
    gapSweepUsd,
    gapSweepCrrPct,
    surafUsd: (Number(gapSweepUsd) * SURAF_RATIO).toFixed(2),
    surafCrrPct: (Number(gapSweepCrrPct) * SURAF_RATIO).toFixed(2),
  };
  // `crr_pct == unadjusted_crr_pct + penalty_pp`, per `SurafDetails`. The
  // penalty is capped at the CRR itself so a zero-CRR asset keeps that identity
  // without reporting a negative unadjusted ratio.
  const penaltyPp = Math.min(SURAF_PENALTY_PP, Number(rrc.surafCrrPct));

  return {
    asset_id: assetId,
    chain_id: chainId,
    prime_id: primeId,
    receipt_token_address: tokenAddress,
    // Collapsed from this asset's own two results, not carried over from
    // spUSDS: a `max_` larger than every result it summarizes is a body no
    // response could have produced.
    max_rrc_usd: largerDecimal(rrc.gapSweepUsd, rrc.surafUsd),
    max_crr_pct: largerDecimal(rrc.gapSweepCrrPct, rrc.surafCrrPct),
    results: [
      {
        asset_id: assetId,
        prime_id: primeId,
        risk_model: 'gap_sweep',
        rrc_usd: rrc.gapSweepUsd,
        comparable_crr_pct: rrc.gapSweepCrrPct,
        details: {
          risk_model: 'gap_sweep',
          gap_pct: '0.15',
          loss_usd: rrc.gapSweepUsd,
        },
      },
      {
        asset_id: assetId,
        prime_id: primeId,
        risk_model: 'suraf',
        rrc_usd: rrc.surafUsd,
        comparable_crr_pct: rrc.surafCrrPct,
        details: {
          risk_model: 'suraf',
          crr_pct: rrc.surafCrrPct,
          unadjusted_crr_pct: (Number(rrc.surafCrrPct) - penaltyPp).toFixed(2),
          penalty_pp: penaltyPp.toFixed(2),
          // Scoped to the asset: a fixed `sparklend-usds` rating id answered a
          // WETH or a Morpho position with a rating of a different asset.
          rating_id: `stl-fixture-asset-${assetId}`,
          rating_version: '2026.07',
          // Synthetic, not a commit of any repo: a real sha here reads as a
          // provenance claim the fixture cannot make.
          source_commit_sha: 'f1e2d3c4b5a60978877665544332211009fedcba',
        },
      },
    ],
  };
}

const BENCHMARK_SOURCE = 'https://info.skyeco.com/required-risk-capital';
/** The upstream monitor is polled every 15 minutes. */
const CAPITAL_SNAPSHOT_AGO = 11 * MINUTE_MS;

const SPARK_CAPITAL_FIGURES = {
  prime_name: 'spark',
  prime_vault_address: SPARK_VAULT,
  exposure: '1656538061.997601317473783974',
  total_risk_capital: '48142491.085806286854722044',
  required_risk_capital: '44692696.19',
  capital_buffer: '3449794.895806286854722044',
  encumbrance_ratio: '0.9283',
  is_validated: true,
  benchmark_source: BENCHMARK_SOURCE,
  validation_note: null,
  scope: 'prime',
} as const;

const GROVE_CAPITAL_FIGURES = {
  prime_name: 'grove',
  prime_vault_address: GROVE_VAULT,
  exposure: '124481521.310000000000000000',
  total_risk_capital: '9204118.400000000000000000',
  required_risk_capital: '5564324.20',
  capital_buffer: '3639794.200000000000000000',
  encumbrance_ratio: '0.6045',
  is_validated: true,
  benchmark_source: BENCHMARK_SOURCE,
  validation_note: null,
  scope: 'prime',
} as const;

/**
 * One row per ALM proxy carrying prime-level figures — the
 * `prime_id`-is-really-a-proxy trap the endpoint's own description warns about,
 * reproduced rather than tidied up so the UI's dedupe-by-vault is exercised.
 */
export function seedCapitalMetrics(nowMs: number): CapitalMetrics[] {
  const timestamp = isoAgo(nowMs, CAPITAL_SNAPSHOT_AGO);

  return [
    { ...SPARK_CAPITAL_FIGURES, prime_id: SPARK_MAINNET_PROXY, timestamp },
    { ...SPARK_CAPITAL_FIGURES, prime_id: SPARK_BASE_PROXY, timestamp },
    { ...SPARK_CAPITAL_FIGURES, prime_id: SPARK_AVALANCHE_PROXY, timestamp },
    { ...GROVE_CAPITAL_FIGURES, prime_id: GROVE_MAINNET_PROXY, timestamp },
    { ...GROVE_CAPITAL_FIGURES, prime_id: GROVE_BASE_PROXY, timestamp },
    // The unvalidated branch: no upstream row, so zeroed figures and a note.
    {
      prime_id: GROVE_AVALANCHE_PROXY,
      prime_name: 'grove',
      prime_vault_address: GROVE_VAULT,
      scope: 'prime',
      exposure: '0',
      total_risk_capital: '0',
      required_risk_capital: '0',
      capital_buffer: '0',
      encumbrance_ratio: null,
      is_validated: false,
      benchmark_source: null,
      validation_note:
        'No matching row in the Star Agents monitor for this prime.',
      timestamp,
    },
  ];
}
