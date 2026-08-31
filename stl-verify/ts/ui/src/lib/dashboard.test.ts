import { describe, expect, it, vi } from 'vitest';

import type { Allocation, Prime } from '../types/allocation';
import type { LocalChainRow, LocalProtocolRow } from '../types/local-data';
import {
  allocationNetworkKey,
  balancedColumns,
  buildChainLabelLookup,
  buildNetworkOptions,
  buildNetworkOptionsFromMetadata,
  buildProtocolOptions,
  buildProtocolOptionsFromMetadata,
  DIRECT_PROTOCOL_FILTER_VALUE,
  encumbranceSeverity,
  findPrimeGroup,
  findProtocolMetadata,
  formatCompactNumber,
  formatCompactUsd,
  formatDateTime,
  formatDeltaSign,
  formatDurationFromSeconds,
  formatFreshnessLabel,
  formatMultiplier,
  formatPercentValue,
  formatRatioPercent,
  formatTokenAmount,
  formatUsdPrice,
  formatUsdValue,
  formatWadValue,
  getAddressLabel,
  getAllocationKey,
  getCategoryLabel,
  getChainLabel,
  getExplorerUrl,
  getPrimeGroupKey,
  getProtocolLabel,
  groupPrimesByVault,
  parseNumericValue,
  riskModelCaptionSuffix,
  sortAllocations,
  sortByBucketStart,
  toChartSeries,
  truncateMiddle,
  wadToUnits,
} from './dashboard';

const ADDRESS = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2';

function makePrime(overrides: Partial<Prime> = {}): Prime {
  return {
    address: ADDRESS,
    chain: 'mainnet',
    chain_id: 1,
    id: ADDRESS,
    name: 'spark',
    prime_vault_address: '0x691a6c29e9e96dd897718305427ad5d534db16ba',
    role: 'alm',
    ...overrides,
  };
}

function makeAllocation(overrides: Partial<Allocation> = {}): Allocation {
  return {
    balance: '1',
    category: 'allocation',
    chain_id: 1,
    scope: 'proxy',
    source: 'indexed',
    symbol: 'WETH',
    underlying_symbol: 'WETH',
    ...overrides,
  };
}

function makeProtocolRow(
  overrides: Partial<LocalProtocolRow> = {},
): LocalProtocolRow {
  return {
    chain_id: 1,
    encode: 'sparklend',
    id: 1,
    name: 'SparkLend',
    ...overrides,
  };
}

function makeChainRow(overrides: Partial<LocalChainRow> = {}): LocalChainRow {
  return { chain_id: 1, name: 'Ethereum', ...overrides };
}

function makeBucket(bucketStart: string, netFlowUsd: string | null = null) {
  return { bucket_start: bucketStart, net_flow_usd: netFlowUsd };
}

const ABSENT_VALUES = [null, undefined, ''] as const;

describe('parseNumericValue', () => {
  it.each([
    ['5', 5],
    [5, 5],
    ['-1.25', -1.25],
    ['0', 0],
  ])('reads %o as the number %o', (input, expected) => {
    expect(parseNumericValue(input)).toBe(expected);
  });

  it.each(ABSENT_VALUES)('reads the absent value %o as null', (input) => {
    expect(parseNumericValue(input)).toBeNull();
  });

  it('returns null for a non-numeric string', () => {
    expect(parseNumericValue('abc')).toBeNull();
  });

  it('returns null for a non-finite number', () => {
    expect(parseNumericValue(Number.POSITIVE_INFINITY)).toBeNull();
  });

  it('warns with the caller-supplied context when parsing fails', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);

    parseNumericValue('abc', 'wadToUnits');

    expect(warn).toHaveBeenCalledWith(expect.stringContaining('abc'), {
      context: 'wadToUnits',
    });
    warn.mockRestore();
  });
});

describe('formatTokenAmount', () => {
  it.each([
    [1_234_567, '1.23M'],
    [1_500_000_000, '1.5B'],
    [-1_234_567, '-1.23M'],
  ])('compacts %o at or above a million as %o', (input, expected) => {
    expect(formatTokenAmount(input)).toBe(expected);
  });

  it.each([
    [1, '1'],
    [1234.5678, '1,234.57'],
    [999.5, '999.5'],
    [-12.5, '-12.5'],
  ])('renders %o with at most two decimals as %o', (input, expected) => {
    expect(formatTokenAmount(input)).toBe(expected);
  });

  it('renders exact zero without a decimal tail', () => {
    expect(formatTokenAmount(0)).toBe('0');
  });

  it.each([
    [0.000001234, '0.000001'],
    [0.5, '0.5'],
  ])('keeps six decimals on the sub-unit value %o', (input, expected) => {
    expect(formatTokenAmount(input)).toBe(expected);
  });

  it.each(ABSENT_VALUES)('renders the absent value %o as a dash', (input) => {
    expect(formatTokenAmount(input)).toBe('—');
  });
});

describe('formatUsdValue', () => {
  it.each([
    [1500, '$1.5K'],
    [1_234_567, '$1.23M'],
  ])('compacts %o at or above a thousand as %o', (input, expected) => {
    expect(formatUsdValue(input)).toBe(expected);
  });

  it.each([
    [999.5, '$999.50'],
    [12.5, '$12.50'],
    [0, '$0.00'],
  ])('keeps cents on %o below a thousand as %o', (input, expected) => {
    expect(formatUsdValue(input)).toBe(expected);
  });

  it.each(ABSENT_VALUES)('renders the absent value %o as a dash', (input) => {
    expect(formatUsdValue(input)).toBe('—');
  });
});

describe('formatUsdPrice', () => {
  it.each([
    [118432.55, '$118,432.55'],
    [1_500_000, '$1,500,000.00'],
  ])('never compacts %o, rendering %o', (input, expected) => {
    expect(formatUsdPrice(input)).toBe(expected);
  });

  it.each(ABSENT_VALUES)('renders the absent value %o as a dash', (input) => {
    expect(formatUsdPrice(input)).toBe('—');
  });
});

describe('formatCompactUsd', () => {
  it.each([
    [1_500_000_000, '$1.5B'],
    [36_000_000, '$36M'],
    [1_234_567, '$1.2M'],
  ])('renders %o to two significant digits as %o', (input, expected) => {
    expect(formatCompactUsd(input)).toBe(expected);
  });

  it.each(ABSENT_VALUES)('renders the absent value %o as a dash', (input) => {
    expect(formatCompactUsd(input)).toBe('—');
  });
});

describe('formatCompactNumber', () => {
  it.each([
    [1_500_000_000, '1.5B'],
    [36_000_000, '36M'],
    [1_234_567, '1.2M'],
  ])('renders %o to two significant digits as %o', (input, expected) => {
    expect(formatCompactNumber(input)).toBe(expected);
  });

  it.each(ABSENT_VALUES)('renders the absent value %o as a dash', (input) => {
    expect(formatCompactNumber(input)).toBe('—');
  });
});

describe('formatPercentValue', () => {
  it('renders a percentage to two decimals by default', () => {
    expect(formatPercentValue(12.3456)).toBe('12.35%');
  });

  it('honours a caller-chosen precision', () => {
    expect(formatPercentValue(12.3456, 0)).toBe('12%');
  });

  it.each(ABSENT_VALUES)('renders the absent value %o as a dash', (input) => {
    expect(formatPercentValue(input)).toBe('—');
  });
});

describe('formatRatioPercent', () => {
  it('scales a ratio to a percentage', () => {
    expect(formatRatioPercent(1.0345)).toBe('103.45%');
  });

  it('honours a caller-chosen precision', () => {
    expect(formatRatioPercent(1.0345, 1)).toBe('103.5%');
  });

  it.each(ABSENT_VALUES)('renders the absent value %o as a dash', (input) => {
    expect(formatRatioPercent(input)).toBe('—');
  });
});

describe('formatMultiplier', () => {
  it('renders three decimals and an x suffix', () => {
    expect(formatMultiplier(1.5)).toBe('1.500x');
  });

  it.each(ABSENT_VALUES)('renders the absent value %o as a dash', (input) => {
    expect(formatMultiplier(input)).toBe('—');
  });
});

describe('formatDeltaSign', () => {
  it.each([
    [1234.5678, '+1,234.57'],
    [0, '+0'],
  ])('marks the non-negative %o as %o', (input, expected) => {
    expect(formatDeltaSign(input)).toBe(expected);
  });

  it('marks a negative value with a typographic minus and no sign duplication', () => {
    expect(formatDeltaSign(-1234.5678)).toBe('−1,234.57');
  });

  it.each([
    [2_000_000, '+2M'],
    [-2_000_000, '−2M'],
  ])('compacts the magnitude of %o as %o', (input, expected) => {
    expect(formatDeltaSign(input)).toBe(expected);
  });

  it.each(ABSENT_VALUES)('renders the absent value %o as a dash', (input) => {
    expect(formatDeltaSign(input)).toBe('—');
  });
});

describe('truncateMiddle', () => {
  it('elides the middle of a value longer than the kept ends plus the ellipsis', () => {
    expect(truncateMiddle(ADDRESS)).toBe('0xc02aaa...756cc2');
  });

  it('returns a value short enough to show whole', () => {
    expect(truncateMiddle('0xc02aaa39b223fe8')).toBe('0xc02aaa39b223fe8');
  });

  it('honours caller-chosen end lengths', () => {
    expect(truncateMiddle(ADDRESS, 4, 4)).toBe('0xc0...6cc2');
  });

  it.each([null, undefined, ''])(
    'renders the absent value %o as a dash',
    (input) => {
      expect(truncateMiddle(input)).toBe('—');
    },
  );
});

describe('formatWadValue', () => {
  it.each([
    ['1500000000000000000', '1.5'],
    ['0', '0'],
    ['1000000000000000000000000', '1M'],
  ])('scales the plain wad %o down by 1e18 as %o', (input, expected) => {
    expect(formatWadValue(input)).toBe(expected);
  });

  it('expands a positive exponent rather than reading only its leading digit', () => {
    expect(formatWadValue('2.5707140E+27')).toBe('2.57B');
  });

  it('expands a negative exponent that leaves whole digits', () => {
    expect(formatWadValue('15000000000000000000e-1')).toBe('1.5');
  });

  it('expands a negative exponent that consumes every digit', () => {
    expect(formatWadValue('1.5e-1')).toBe('0');
  });

  it('expands a mantissa with no integer digits', () => {
    expect(formatWadValue('.5e19')).toBe('5');
  });

  it('expands a mantissa carrying an explicit plus sign', () => {
    expect(formatWadValue('+1.5E+18')).toBe('1.5');
  });

  it('keeps six fractional digits of a sub-unit wad', () => {
    expect(formatWadValue('1234500000000')).toBe('0.000001');
  });

  it('truncates rather than rounds the integer part of a fractional wad string', () => {
    expect(formatWadValue('1500000000000000000.9')).toBe('1.5');
  });

  it.each(ABSENT_VALUES)('renders the absent value %o as a dash', (input) => {
    expect(formatWadValue(input)).toBe('—');
  });

  it.each([['not-a-number'], ['1.2.3'], ['0x10'], ['1e']])(
    'rejects the non-numeric input %o',
    (input) => {
      const warn = vi
        .spyOn(console, 'warn')
        .mockImplementation(() => undefined);

      expect(formatWadValue(input)).toBe('—');
      expect(warn).toHaveBeenCalled();
      warn.mockRestore();
    },
  );

  it('rejects an exponent too large to expand', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);

    expect(formatWadValue('1e1001')).toBe('—');
    expect(warn).toHaveBeenCalled();
    warn.mockRestore();
  });
});

describe('wadToUnits', () => {
  it('scales a wad down to float units', () => {
    expect(wadToUnits('1500000000000000000')).toBe(1.5);
  });

  it.each(ABSENT_VALUES)('reads the absent value %o as null', (input) => {
    expect(wadToUnits(input)).toBeNull();
  });
});

describe('formatDurationFromSeconds', () => {
  it.each([
    [0, '0s'],
    [59, '59s'],
    [-5, '0s'],
    [90, '1m'],
    [3599, '59m'],
    [3600, '1h 0m'],
    [7260, '2h 1m'],
    [86400, '1d 0h'],
    [180000, '2d 2h'],
  ])('renders %o seconds as %o', (input, expected) => {
    expect(formatDurationFromSeconds(input)).toBe(expected);
  });

  it.each([null, undefined, Number.NaN])(
    'renders the unusable input %o as Unknown',
    (input) => {
      expect(formatDurationFromSeconds(input)).toBe('Unknown');
    },
  );
});

describe('formatFreshnessLabel', () => {
  const NOW = Date.parse('2026-05-07T12:00:00Z');

  it.each([
    ['2026-05-07T12:00:00Z', 'Just now'],
    ['2026-05-07T11:30:00Z', '30m ago'],
    ['2026-05-07T09:00:00Z', '3h ago'],
    ['2026-05-05T12:00:00Z', '2d ago'],
  ])('renders %o as %o', (input, expected) => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);

    expect(formatFreshnessLabel(input)).toBe(expected);
    vi.useRealTimers();
  });

  it('falls back to a calendar date beyond a week', () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);

    // Asserted by shape, not by literal: the fallback is toLocaleDateString,
    // whose output follows the runner's locale rather than the app's.
    const label = formatFreshnessLabel('2026-01-02T12:00:00Z');

    expect(label).toContain('2026');
    expect(label).not.toMatch(/ago/);
    vi.useRealTimers();
  });

  it('clamps a future timestamp to Just now rather than reporting negative age', () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);

    expect(formatFreshnessLabel('2026-05-07T13:00:00Z')).toBe('Just now');
    vi.useRealTimers();
  });

  it('passes an unparseable timestamp through unchanged', () => {
    expect(formatFreshnessLabel('never')).toBe('never');
  });
});

describe('formatDateTime', () => {
  it('renders a parseable timestamp in the en-US dashboard format', () => {
    expect(formatDateTime('2026-05-07T14:30:00Z')).toBe('May 7, 02:30 PM');
  });

  it('passes an unparseable timestamp through unchanged', () => {
    expect(formatDateTime('not a date')).toBe('not a date');
  });
});

describe('balancedColumns', () => {
  it.each([
    [0, 4, 1],
    [1, 4, 1],
    [6, 4, 3],
    [5, 3, 3],
    [4, 4, 4],
    [8, 4, 4],
    [9, 4, 3],
  ])(
    'spreads %o cards over at most %o columns as %o',
    (count, max, expected) => {
      expect(balancedColumns(count, max)).toBe(expected);
    },
  );
});

describe('encumbranceSeverity', () => {
  it.each([
    [0.5, 'healthy'],
    [0.79, 'healthy'],
    [0.8, 'at-risk'],
    [0.99, 'at-risk'],
    [1, 'low'],
    [1.029, 'low'],
    [1.03, 'high'],
    [2, 'high'],
  ])('classifies the ratio %o as %o', (ratio, expected) => {
    expect(encumbranceSeverity(ratio)).toBe(expected);
  });

  it.each([null, undefined, Number.NaN, Number.POSITIVE_INFINITY])(
    'reads the unusable ratio %o as healthy',
    (ratio) => {
      expect(encumbranceSeverity(ratio)).toBe('healthy');
    },
  );
});

describe('toChartSeries', () => {
  it('carries each bucket instant through as the point timestamp', () => {
    const series = toChartSeries(
      [makeBucket('2026-05-07T14:30:00Z', '5')],
      (bucket) => parseNumericValue(bucket.net_flow_usd),
    );

    expect(series).toEqual([
      {
        label: expect.stringContaining('14:30'),
        value: 5,
        timestamp: Date.parse('2026-05-07T14:30:00Z'),
      },
    ]);
  });

  it('drops a bucket whose figure is absent', () => {
    const series = toChartSeries(
      [
        makeBucket('2026-05-07T14:30:00Z'),
        makeBucket('2026-05-07T15:30:00Z', '5'),
      ],
      (bucket) => parseNumericValue(bucket.net_flow_usd),
    );

    expect(series.map((point) => point.value)).toEqual([5]);
  });

  it('drops a bucket whose instant cannot be parsed', () => {
    const series = toChartSeries([makeBucket('never', '5')], (bucket) =>
      parseNumericValue(bucket.net_flow_usd),
    );

    expect(series).toEqual([]);
  });
});

describe('sortByBucketStart', () => {
  it('orders buckets oldest first regardless of arrival order', () => {
    const buckets = [
      makeBucket('2026-05-07T15:00:00Z'),
      makeBucket('2026-05-07T13:00:00Z'),
      makeBucket('2026-05-07T14:00:00Z'),
    ];

    expect(sortByBucketStart(buckets).map((b) => b.bucket_start)).toEqual([
      '2026-05-07T13:00:00Z',
      '2026-05-07T14:00:00Z',
      '2026-05-07T15:00:00Z',
    ]);
  });

  it('leaves the caller-owned array untouched', () => {
    const buckets = [
      makeBucket('2026-05-07T15:00:00Z'),
      makeBucket('2026-05-07T13:00:00Z'),
    ];

    sortByBucketStart(buckets);

    expect(buckets[0]?.bucket_start).toBe('2026-05-07T15:00:00Z');
  });

  it('sorts an unparseable bucket start to the front rather than dropping it', () => {
    const buckets = [makeBucket('2026-05-07T13:00:00Z'), makeBucket('never')];

    expect(sortByBucketStart(buckets).map((b) => b.bucket_start)).toEqual([
      'never',
      '2026-05-07T13:00:00Z',
    ]);
  });
});

describe('getPrimeGroupKey', () => {
  it('keys a prime on its vault address', () => {
    expect(
      getPrimeGroupKey(makePrime({ prime_vault_address: '0xvault' })),
    ).toBe('0xvault');
  });

  it('falls back to the prime name when no vault address is on record', () => {
    expect(
      getPrimeGroupKey(makePrime({ name: 'grove', prime_vault_address: null })),
    ).toBe('grove');
  });
});

describe('groupPrimesByVault', () => {
  const mainnetRow = makePrime({
    address: '0xffff',
    chain: 'mainnet',
    chain_id: 1,
  });
  const baseRow = makePrime({
    address: '0xcccc',
    chain: 'base',
    chain_id: 8453,
  });
  const avalancheRow = makePrime({
    address: '0xbbbb',
    chain: 'avalanche-c',
    chain_id: 43114,
  });

  it('collapses every proxy row of one prime into a single group', () => {
    expect(
      groupPrimesByVault([mainnetRow, baseRow, avalancheRow]),
    ).toHaveLength(1);
  });

  it('counts the distinct chains a prime allocates on', () => {
    const [group] = groupPrimesByVault([mainnetRow, baseRow, avalancheRow]);

    expect(group?.chainCount).toBe(3);
  });

  it('dedupes one address that appears on two chains', () => {
    const onBase = makePrime({
      address: '0xaaaa',
      chain: 'base',
      chain_id: 8453,
    });
    const onMainnet = makePrime({
      address: '0xaaaa',
      chain: 'mainnet',
      chain_id: 1,
    });

    const [group] = groupPrimesByVault([onBase, onMainnet]);

    expect(group?.proxyAddresses).toEqual(['0xaaaa']);
  });

  it('prefers the mainnet proxy as the primary even when it arrives last', () => {
    const [group] = groupPrimesByVault([avalancheRow, baseRow, mainnetRow]);

    expect(group?.primaryProxyAddress).toBe('0xffff');
  });

  it('falls back to the lowest proxy address when the prime is not on mainnet', () => {
    const [group] = groupPrimesByVault([baseRow, avalancheRow]);

    expect(group?.primaryProxyAddress).toBe('0xbbbb');
  });

  it('keeps a prime with no vault address as its own group keyed on its name', () => {
    const [group] = groupPrimesByVault([
      makePrime({ name: 'grove', prime_vault_address: null }),
    ]);

    expect(group).toMatchObject({ key: 'grove', vaultAddress: null });
  });

  it('separates two primes holding different vault addresses', () => {
    const other = makePrime({
      address: '0xdddd',
      name: 'grove',
      prime_vault_address: '0x9999',
    });

    expect(groupPrimesByVault([mainnetRow, other]).map((g) => g.name)).toEqual([
      'spark',
      'grove',
    ]);
  });

  it('returns no groups for no rows', () => {
    expect(groupPrimesByVault([])).toEqual([]);
  });
});

describe('findPrimeGroup', () => {
  const groups = groupPrimesByVault([
    makePrime({ address: '0xffff', chain: 'mainnet', chain_id: 1 }),
    makePrime({ address: '0xcccc', chain: 'base', chain_id: 8453 }),
  ]);

  it.each([
    ['the group key verbatim', '0x691a6c29e9e96dd897718305427ad5d534db16ba'],
    ['a checksummed group key', '0x691A6C29E9E96DD897718305427AD5D534DB16BA'],
    ['a proxy address of the prime', '0xcccc'],
    ['a checksummed proxy address', '0xCCCC'],
  ])('resolves %s', (_case, requested) => {
    expect(findPrimeGroup(groups, requested)).toBe(groups[0]);
  });

  it('returns null for an address no prime answers to', () => {
    expect(findPrimeGroup(groups, '0x1234')).toBeNull();
  });

  it("prefers a group's own key over another group's proxy address", () => {
    const colliding = groupPrimesByVault([
      makePrime({
        address: '0xeeee',
        name: 'grove',
        prime_vault_address: '0x9999',
      }),
      makePrime({
        address: '0x1111',
        name: 'nova',
        prime_vault_address: '0xeeee',
      }),
    ]);

    expect(findPrimeGroup(colliding, '0xeeee')?.name).toBe('nova');
  });
});

describe('allocationNetworkKey', () => {
  it('keys an indexed row on its chain id', () => {
    expect(allocationNetworkKey(makeAllocation({ chain_id: 8453 }))).toBe(
      '8453',
    );
  });

  it('keys an unindexed row on its upstream network name', () => {
    expect(
      allocationNetworkKey(
        makeAllocation({ chain_id: null, network: 'plume' }),
      ),
    ).toBe('net:plume');
  });

  it('keeps two differently-named unindexed chains apart', () => {
    const plume = makeAllocation({ chain_id: null, network: 'plume' });
    const robinhood = makeAllocation({ chain_id: null, network: 'robinhood' });

    expect(allocationNetworkKey(plume)).not.toBe(
      allocationNetworkKey(robinhood),
    );
  });

  it('keeps a network literally named "1" apart from chain 1', () => {
    const named = makeAllocation({ chain_id: null, network: '1' });

    expect(allocationNetworkKey(named)).not.toBe(
      allocationNetworkKey(makeAllocation({ chain_id: 1 })),
    );
  });

  it('keys an unnamed unindexed row on the unknown sentinel', () => {
    expect(
      allocationNetworkKey(makeAllocation({ chain_id: null, network: null })),
    ).toBe('net:unknown');
  });
});

describe('getAllocationKey', () => {
  it('keys a receipt-token position on its receipt token id', () => {
    expect(getAllocationKey(makeAllocation({ receipt_token_id: 736 }))).toBe(
      '736',
    );
  });

  it('keys a direct holding on its network and underlying token', () => {
    const direct = makeAllocation({
      receipt_token_id: null,
      underlying_token_id: 42,
    });

    expect(getAllocationKey(direct)).toBe('direct:1:42');
  });

  it('falls back to the symbol when a holding carries no underlying token id', () => {
    const custody = makeAllocation({
      chain_id: 0,
      receipt_token_id: null,
      symbol: 'BTC',
      underlying_token_id: null,
    });

    expect(getAllocationKey(custody)).toBe('direct:0:BTC');
  });

  it('keeps the same asset on two unindexed chains as two rows', () => {
    const plume = makeAllocation({ chain_id: null, network: 'plume' });
    const robinhood = makeAllocation({ chain_id: null, network: 'robinhood' });

    expect(getAllocationKey(plume)).not.toBe(getAllocationKey(robinhood));
  });
});

describe('getChainLabel', () => {
  it('labels the off-chain sentinel chain id', () => {
    expect(getChainLabel(0)).toBe('Off-chain');
  });

  it('prefers the registry lookup over the viem name', () => {
    const labels = buildChainLabelLookup([
      makeChainRow({ name: 'Ethereum Mainnet' }),
    ]);

    expect(getChainLabel(1, labels)).toBe('Ethereum Mainnet');
  });

  it('falls back to the viem chain name when the registry has no row', () => {
    expect(getChainLabel(1, buildChainLabelLookup([]))).toBe('Ethereum');
  });

  it('falls back to a numbered label for a chain viem does not know', () => {
    expect(getChainLabel(999_999)).toBe('Chain 999999');
  });

  it.each([
    ['plume', 'Plume'],
    ['arbitrum_one', 'Arbitrum One'],
    ['op-mainnet', 'Op Mainnet'],
  ])('title-cases the upstream network slug %o as %o', (network, expected) => {
    expect(getChainLabel(null, undefined, network)).toBe(expected);
  });

  it.each([null, undefined, ''])(
    'labels an unidentifiable chain named %o as unknown',
    (network) => {
      expect(getChainLabel(null, undefined, network)).toBe('Unknown chain');
    },
  );
});

describe('getProtocolLabel', () => {
  it.each([null, undefined, DIRECT_PROTOCOL_FILTER_VALUE])(
    'labels %o as a direct holding',
    (protocol) => {
      expect(getProtocolLabel(protocol)).toBe('Direct');
    },
  );

  it('prefers a registry protocol name over the static label table', () => {
    expect(
      getProtocolLabel('spark', [makeProtocolRow({ name: 'Spark Lend v3' })]),
    ).toBe('Spark Lend v3');
  });

  it('falls back to the static label table', () => {
    expect(getProtocolLabel('spark')).toBe('SparkLend');
  });

  it('title-cases a protocol nothing recognises', () => {
    expect(getProtocolLabel('some_new_protocol')).toBe('Some New Protocol');
  });
});

describe('findProtocolMetadata', () => {
  it('returns null when there are no registry rows', () => {
    expect(findProtocolMetadata('spark', [])).toBeNull();
  });

  it('returns null when no registry row scores against the protocol', () => {
    expect(findProtocolMetadata('maple', [makeProtocolRow()])).toBeNull();
  });

  it('prefers the row on the requested chain over an equally-named one elsewhere', () => {
    const onBase = makeProtocolRow({ chain_id: 8453, id: 2 });

    expect(
      findProtocolMetadata('sparklend', [makeProtocolRow(), onBase], 8453),
    ).toBe(onBase);
  });

  it('resolves the known spark/sparklend alias', () => {
    const row = makeProtocolRow({ name: 'SparkLend' });

    expect(findProtocolMetadata('spark', [row])).toBe(row);
  });
});

describe('buildNetworkOptions', () => {
  it('counts the allocations on each network', () => {
    const options = buildNetworkOptions([
      makeAllocation({ chain_id: 1 }),
      makeAllocation({ chain_id: 1 }),
      makeAllocation({ chain_id: 8453 }),
    ]);

    expect(options).toEqual([
      { count: 2, label: 'Ethereum', value: '1' },
      { count: 1, label: 'Base', value: '8453' },
    ]);
  });

  it('orders indexed chains by chain id', () => {
    const options = buildNetworkOptions([
      makeAllocation({ chain_id: 8453 }),
      makeAllocation({ chain_id: 1 }),
      makeAllocation({ chain_id: 130 }),
    ]);

    expect(options.map((option) => option.value)).toEqual(['1', '130', '8453']);
  });

  it('sorts unindexed chains after every indexed one', () => {
    const options = buildNetworkOptions([
      makeAllocation({ chain_id: null, network: 'plume' }),
      makeAllocation({ chain_id: 8453 }),
    ]);

    expect(options.map((option) => option.value)).toEqual([
      '8453',
      'net:plume',
    ]);
  });

  it('orders unindexed chains among themselves by label', () => {
    const options = buildNetworkOptions([
      makeAllocation({ chain_id: null, network: 'robinhood' }),
      makeAllocation({ chain_id: null, network: 'plume' }),
    ]);

    expect(options.map((option) => option.label)).toEqual([
      'Plume',
      'Robinhood',
    ]);
  });

  it('labels each option through the supplied chain registry', () => {
    const labels = buildChainLabelLookup([
      makeChainRow({ name: 'Ethereum Mainnet' }),
    ]);

    expect(buildNetworkOptions([makeAllocation()], labels)[0]?.label).toBe(
      'Ethereum Mainnet',
    );
  });

  it('returns no options for no allocations', () => {
    expect(buildNetworkOptions([])).toEqual([]);
  });
});

describe('buildProtocolOptions', () => {
  it('counts the allocations under each protocol', () => {
    const options = buildProtocolOptions([
      makeAllocation({ protocol_name: 'spark' }),
      makeAllocation({ protocol_name: 'spark' }),
    ]);

    expect(options).toEqual([{ count: 2, label: 'SparkLend', value: 'spark' }]);
  });

  it('files a protocol-less allocation under the direct sentinel', () => {
    const options = buildProtocolOptions([
      makeAllocation({ protocol_name: null }),
    ]);

    expect(options).toEqual([
      { count: 1, label: 'Direct', value: DIRECT_PROTOCOL_FILTER_VALUE },
    ]);
  });

  it('orders options by their rendered label', () => {
    const options = buildProtocolOptions([
      makeAllocation({ protocol_name: 'spark' }),
      makeAllocation({ protocol_name: 'grove' }),
    ]);

    expect(options.map((option) => option.label)).toEqual([
      'Grove',
      'SparkLend',
    ]);
  });
});

describe('buildProtocolOptionsFromMetadata', () => {
  it('lists each registry protocol once, alphabetically, with no counts', () => {
    const options = buildProtocolOptionsFromMetadata([
      makeProtocolRow({ name: 'SparkLend' }),
      makeProtocolRow({ chain_id: 8453, id: 2, name: 'SparkLend' }),
      makeProtocolRow({ id: 3, name: 'Aave' }),
    ]);

    expect(options).toEqual([
      { count: 0, label: 'Aave', value: 'Aave' },
      { count: 0, label: 'SparkLend', value: 'SparkLend' },
    ]);
  });

  it('drops a registry row whose name is blank', () => {
    expect(
      buildProtocolOptionsFromMetadata([makeProtocolRow({ name: '  ' })]),
    ).toEqual([]);
  });
});

describe('buildNetworkOptionsFromMetadata', () => {
  it('lists each chain once in chain-id order with no counts', () => {
    const options = buildNetworkOptionsFromMetadata([
      makeChainRow({ chain_id: 8453, name: 'Base' }),
      makeChainRow({ chain_id: 1, name: 'Ethereum' }),
      makeChainRow({ chain_id: 1, name: 'Ethereum' }),
    ]);

    expect(options).toEqual([
      { count: 0, label: 'Ethereum', value: '1' },
      { count: 0, label: 'Base', value: '8453' },
    ]);
  });
});

describe('getExplorerUrl', () => {
  it('builds an address link for a known chain', () => {
    expect(getExplorerUrl(1, ADDRESS)).toBe(
      `https://etherscan.io/address/${ADDRESS}`,
    );
  });

  it('builds a transaction link when asked for one', () => {
    expect(getExplorerUrl(8453, '0xabc', 'tx')).toBe(
      'https://basescan.org/tx/0xabc',
    );
  });

  it('does not double the separator when the explorer url ends in a slash', () => {
    expect(getExplorerUrl(324, '0xabc')).toBe(
      'https://explorer.zksync.io/address/0xabc',
    );
  });

  it.each([null, undefined, 0, 999_999])(
    'returns null for the unlinkable chain %o',
    (chainId) => {
      expect(getExplorerUrl(chainId, ADDRESS)).toBeNull();
    },
  );
});

describe('getCategoryLabel', () => {
  it.each([
    ['allocation', 'Allocation'],
    ['pol', 'Protocol Owned Liquidity'],
    ['psm3', 'PSM3'],
    ['asset', 'Asset'],
    ['custody', 'Custody'],
  ] as const)('labels the category %o as %o', (category, expected) => {
    expect(getCategoryLabel(category)).toBe(expected);
  });

  it.each(['', undefined] as const)(
    'falls back for the absent category %o',
    (category) => {
      expect(getCategoryLabel(category)).toBe('Unknown');
    },
  );

  it('honours a caller-chosen fallback', () => {
    expect(getCategoryLabel(undefined, 'Other')).toBe('Other');
  });
});

describe('riskModelCaptionSuffix', () => {
  it.each([
    ['gap_sweep', ', 15% stress'],
    ['core_model', ', expected-loss based'],
    ['some_future_model', ''],
    [null, ''],
  ])('captions the model %o with %o', (model, expected) => {
    expect(riskModelCaptionSuffix(model)).toBe(expected);
  });
});

describe('sortAllocations', () => {
  it('orders allocations by descending balance', () => {
    const sorted = sortAllocations([
      makeAllocation({ balance: '1', symbol: 'A' }),
      makeAllocation({ balance: '10', symbol: 'B' }),
    ]);

    expect(sorted.map((allocation) => allocation.symbol)).toEqual(['B', 'A']);
  });

  it('breaks a balance tie on symbol', () => {
    const sorted = sortAllocations([
      makeAllocation({ balance: '1', symbol: 'B' }),
      makeAllocation({ balance: '1', symbol: 'A' }),
    ]);

    expect(sorted.map((allocation) => allocation.symbol)).toEqual(['A', 'B']);
  });

  it('sorts a null balance as zero rather than dropping the row', () => {
    const sorted = sortAllocations([
      makeAllocation({ balance: null, symbol: 'A' }),
      makeAllocation({ balance: '1', symbol: 'B' }),
    ]);

    expect(sorted.map((allocation) => allocation.symbol)).toEqual(['B', 'A']);
  });

  it('leaves the caller-owned array untouched', () => {
    const allocations = [
      makeAllocation({ balance: '1', symbol: 'A' }),
      makeAllocation({ balance: '10', symbol: 'B' }),
    ];

    sortAllocations(allocations);

    expect(allocations[0]?.symbol).toBe('A');
  });
});

describe('getAddressLabel', () => {
  it('returns null for an address the dictionary does not hold', () => {
    expect(getAddressLabel(ADDRESS)).toBeNull();
  });
});
