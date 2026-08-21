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
import type {
  AllocationRiskCapital,
  CapitalMetrics,
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
} from './registry.ts';

const SPARK_PROXIES = [
  SPARK_MAINNET_PROXY,
  SPARK_BASE_PROXY,
  '0x345e368fccd62266b3f5f37c9a131fd1c39f5869',
  '0x876664f0c9ff24d1aa355ce9f1680ae1a5bf36fb',
  '0x92afd6f2385a90e44da3a8b60fe36f6cbe1d8709',
  SPARK_AVALANCHE_PROXY,
];

const SPARK_PER_ALLOCATION: readonly AllocationRiskCapital[] = [
  {
    receipt_token_id: 736,
    symbol: 'spUSDS',
    protocol_name: 'SparkLend',
    exposure_usd: '841904871.346598373354820026',
    applied: true,
    required_risk_capital_usd: '23308466.81',
    crr_pct: '4.47',
    model: 'gap_sweep',
    unpriced_reason: null,
  },
  {
    receipt_token_id: 338,
    symbol: 'spUSDT',
    protocol_name: 'SparkLend',
    exposure_usd: '346708318.392322222449470000',
    applied: true,
    required_risk_capital_usd: '10442084.54',
    crr_pct: '3.63',
    model: 'gap_sweep',
    unpriced_reason: null,
  },
  {
    receipt_token_id: 723,
    symbol: 'spDAI',
    protocol_name: 'SparkLend',
    exposure_usd: '296086123.323543238248161014',
    applied: true,
    required_risk_capital_usd: '10123665.27',
    crr_pct: '5.40',
    model: 'gap_sweep',
    unpriced_reason: null,
  },
  {
    receipt_token_id: 735,
    symbol: 'spPYUSD',
    protocol_name: 'SparkLend',
    exposure_usd: '100009872.018352967986590000',
    applied: true,
    required_risk_capital_usd: '312255.16',
    crr_pct: '3.43',
    model: 'gap_sweep',
    unpriced_reason: null,
  },
  {
    receipt_token_id: 885660,
    symbol: 'sparkUSDCbc',
    protocol_name: 'Morpho Blue',
    exposure_usd: '9057828.812411176980000000',
    applied: true,
    required_risk_capital_usd: '435169.05',
    crr_pct: '5.31',
    model: 'gap_sweep',
    unpriced_reason: null,
  },
  {
    receipt_token_id: 269,
    symbol: 'spWETH',
    protocol_name: 'SparkLend',
    exposure_usd: '60851479.648374770322193501',
    applied: true,
    required_risk_capital_usd: '1469.59',
    crr_pct: '5.35',
    model: 'gap_sweep',
    unpriced_reason: null,
  },
  {
    receipt_token_id: 892750,
    symbol: 'spDAI',
    protocol_name: 'Morpho Blue',
    exposure_usd: '839.944852318554772032',
    applied: true,
    required_risk_capital_usd: '0.00',
    crr_pct: '0.00',
    model: 'gap_sweep',
    unpriced_reason: null,
  },
  {
    receipt_token_id: 34,
    symbol: 'aEthUSDT',
    protocol_name: 'Aave V3',
    exposure_usd: '5.941286811995430000',
    applied: true,
    required_risk_capital_usd: '0.06',
    crr_pct: '1.13',
    model: 'gap_sweep',
    unpriced_reason: null,
  },
  // The one unpriced row, and the reason the UI needs an `applied: false` state.
  {
    receipt_token_id: 850711,
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

const GROVE_PER_ALLOCATION: readonly AllocationRiskCapital[] = [
  {
    receipt_token_id: 736,
    symbol: 'spUSDS',
    protocol_name: 'SparkLend',
    exposure_usd: '124481521.310000000000000000',
    applied: true,
    required_risk_capital_usd: '5564324.20',
    crr_pct: '4.47',
    model: 'gap_sweep',
    unpriced_reason: null,
  },
];

const SPARK_RISK_CAPITAL: PrimeRiskCapital = {
  prime_id: SPARK_MAINNET_PROXY,
  proxy_address: SPARK_MAINNET_PROXY,
  model: 'gap_sweep',
  source: 'self',
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
  // Three shapes in one list: indexed with exposure, indexed and empty, and
  // unindexed (`null`, not `'0'`) — the distinction the UI must not flatten.
  prime_per_chain: [
    {
      proxy_address: SPARK_MAINNET_PROXY,
      chain: 'mainnet',
      exposure_usd: '1656538061.841276418798473974',
      required_risk_capital_usd: '44692696.19',
      allocation_count: 14,
    },
    {
      proxy_address: SPARK_BASE_PROXY,
      chain: 'base',
      exposure_usd: '0',
      required_risk_capital_usd: '0',
      allocation_count: 0,
    },
    {
      proxy_address: '0x345e368fccd62266b3f5f37c9a131fd1c39f5869',
      chain: 'unichain',
      exposure_usd: null,
      required_risk_capital_usd: null,
      allocation_count: null,
    },
    {
      proxy_address: '0x876664f0c9ff24d1aa355ce9f1680ae1a5bf36fb',
      chain: 'optimism',
      exposure_usd: null,
      required_risk_capital_usd: null,
      allocation_count: null,
    },
    {
      proxy_address: '0x92afd6f2385a90e44da3a8b60fe36f6cbe1d8709',
      chain: 'arbitrum',
      exposure_usd: null,
      required_risk_capital_usd: null,
      allocation_count: null,
    },
    {
      proxy_address: SPARK_AVALANCHE_PROXY,
      chain: 'avalanche-c',
      exposure_usd: '0.156324898675310000',
      required_risk_capital_usd: '0',
      allocation_count: 1,
    },
  ],
  prime_unserved_chains: ['unichain', 'optimism', 'arbitrum'],
};

const GROVE_RISK_CAPITAL: PrimeRiskCapital = {
  prime_id: GROVE_MAINNET_PROXY,
  proxy_address: GROVE_MAINNET_PROXY,
  model: 'gap_sweep',
  source: 'self',
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
  prime_per_chain: [
    {
      proxy_address: GROVE_MAINNET_PROXY,
      chain: 'mainnet',
      exposure_usd: '124481521.310000000000000000',
      required_risk_capital_usd: '5564324.20',
      allocation_count: 1,
    },
    {
      proxy_address: GROVE_BASE_PROXY,
      chain: 'base',
      exposure_usd: '0',
      required_risk_capital_usd: '0',
      allocation_count: 0,
    },
    {
      proxy_address: GROVE_AVALANCHE_PROXY,
      chain: 'avalanche-c',
      exposure_usd: '0',
      required_risk_capital_usd: '0',
      allocation_count: 0,
    },
  ],
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
 * `reference=true` moves every figure to prime scope and fills the
 * reference-only fields. Derived rather than captured: the reference-mode
 * contract is "unprefixed equals prefixed", so expressing it as a transform
 * keeps the two halves from drifting apart in the fixture.
 */
export function toReferenceRiskCapital(
  self: PrimeRiskCapital,
): PrimeRiskCapital {
  return {
    ...self,
    source: 'reference',
    model: null,
    exposure_usd: self.prime_exposure_usd,
    required_risk_capital_usd: self.prime_required_risk_capital_usd,
    modeled_exposure_usd: self.prime_modeled_exposure_usd,
    encumbrance_ratio: self.prime_encumbrance_ratio,
    junior_risk_capital_usd: '12500000.00',
    senior_risk_capital_usd: '35642491.09',
    internal_junior_risk_capital_usd: '7500000.00',
    internal_senior_risk_capital_usd: '21385494.65',
    external_junior_risk_capital_usd: '3750000.00',
    external_senior_risk_capital_usd: '14256996.44',
    tokenized_junior_risk_capital_usd: '1250000.00',
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
  return primeId === null ? 1 : PRIME_POOL_SHARE[primeId.toLowerCase()];
}

// Any registry token gets a breakdown so the drawer works for every
// allocation row; the mapped entries above keep their curated exposures and
// the fallback reuses the shared pool composition at a mid-size share. A token
// outside the registry still misses, keeping the 404 branch reachable.
export function breakdownFor(tokenAddress: string): RiskBreakdown | undefined {
  const mapped = RISK_BREAKDOWN_BY_TOKEN[tokenAddress.toLowerCase()];
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
    surafUsd: (Number(gapSweepUsd) * 0.809).toFixed(2),
    surafCrrPct: (Number(gapSweepCrrPct) * 0.809).toFixed(2),
  };

  return {
    asset_id: assetId,
    chain_id: chainId,
    prime_id: primeId,
    receipt_token_address: tokenAddress,
    max_rrc_usd: '23308466.81',
    max_crr_pct: '4.47',
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
          crr_pct: '3.62',
          unadjusted_crr_pct: '3.12',
          penalty_pp: '0.50',
          rating_id: 'sparklend-usds-v3',
          rating_version: '2026.07',
          source_commit_sha: '3eceb410076e6ff0f8c523e13bace9cd0cf984e6',
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
