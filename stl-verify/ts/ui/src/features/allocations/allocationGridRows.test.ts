import { describe, expect, it } from 'vitest';

import type {
  Allocation,
  AllocationRiskCapital,
  Prime,
} from '../../shared/types/allocation';
import {
  rowExposureUsd,
  toAllocationGridRow,
  withRrcShare,
  type AllocationGridRow,
  type RiskFetchState,
} from './allocationGridRows';

const allocation = (overrides: Partial<Allocation> = {}): Allocation => ({
  balance: '100',
  category: 'allocation',
  chain_id: 1,
  scope: 'proxy',
  source: 'indexed',
  symbol: 'spUSDS',
  underlying_symbol: 'USDS',
  ...overrides,
});

const riskCapital = (
  overrides: Partial<AllocationRiskCapital> = {},
): AllocationRiskCapital => ({
  applied: true,
  exposure_usd: '1000',
  protocol_name: 'SparkLend',
  receipt_token_id: 736,
  source: 'indexed',
  symbol: 'spUSDS',
  ...overrides,
});

const prime = (overrides: Partial<Prime> = {}): Prime => ({
  address: '0x1601843c5e9bc251a3272907010afa41fa18347e',
  chain_id: 1,
  id: '0x691a6c29e9e96dd897718305427ad5d534db16ba',
  name: 'spark',
  role: 'alm',
  ...overrides,
});

/**
 * The risk-capital helpers are module-private, so they are exercised through
 * `toAllocationGridRow` — the surface the grid actually calls.
 */
const buildRow = (
  alloc: Allocation,
  entry?: AllocationRiskCapital,
  selectedPrime: Prime | null = prime(),
  state: RiskFetchState = 'ready',
): AllocationGridRow =>
  toAllocationGridRow(
    alloc,
    entry === undefined ? new Map() : new Map([['k', entry]]),
    state,
    selectedPrime,
  );

const rowWithRrc = (
  riskCapitalUsd: number | null,
  chainMismatch = false,
): AllocationGridRow => ({
  ...allocation(),
  risk: {
    state: 'ready',
    entry: undefined,
    chainMismatch,
    fromReference: false,
    exposureUsd: null,
    riskCapitalUsd,
    crrPct: null,
    sharePct: null,
  },
});

describe('rowExposureUsd', () => {
  it("prefers STL's measured value over Sky's", () => {
    expect(
      rowExposureUsd(
        allocation({ amount_usd: '1000', reference_amount_usd: '2000' }),
      ),
    ).toBe(1000);
  });

  it("falls back to Sky's value when STL priced nothing", () => {
    expect(
      rowExposureUsd(
        allocation({ amount_usd: null, reference_amount_usd: '2000' }),
      ),
    ).toBe(2000);
  });

  it("keeps a published zero rather than falling back to Sky's figure", () => {
    expect(
      rowExposureUsd(
        allocation({ amount_usd: '0', reference_amount_usd: '2000' }),
      ),
    ).toBe(0);
  });

  it('is null when neither side published a value', () => {
    expect(rowExposureUsd(allocation())).toBeNull();
  });
});

describe('toAllocationGridRow', () => {
  it('derives the requirement from the ratio and exposure the row shows', () => {
    const row = buildRow(
      allocation({ amount_usd: '1000', position_keys: ['k'] }),
      riskCapital({ crr_pct: '5' }),
    );

    expect(row.risk.exposureUsd).toBe(1000);
    expect(row.risk.crrPct).toBe(5);
    // crrPct is a 0-100 percentage at every boundary, so 5% of 1000 is 50.
    expect(row.risk.riskCapitalUsd).toBe(50);
  });

  it('leaves sharePct for withRrcShare, which alone sees the whole column', () => {
    expect(
      buildRow(allocation({ amount_usd: '1000' })).risk.sharePct,
    ).toBeNull();
  });

  it('carries the fetch state onto the row', () => {
    expect(
      buildRow(allocation(), undefined, prime(), 'loading').risk.state,
    ).toBe('loading');
    expect(buildRow(allocation(), undefined, prime(), 'error').risk.state).toBe(
      'error',
    );
  });

  it('withholds a requirement when either figure is missing', () => {
    expect(
      buildRow(
        allocation({ amount_usd: null, position_keys: ['k'] }),
        riskCapital({ crr_pct: '5' }),
      ).risk.riskCapitalUsd,
    ).toBeNull();
    expect(
      buildRow(
        allocation({ amount_usd: '1000', position_keys: ['k'] }),
        riskCapital({ crr_pct: null }),
      ).risk.riskCapitalUsd,
    ).toBeNull();
  });

  describe('risk entry lookup', () => {
    it('takes the first position key that resolves', () => {
      const row = toAllocationGridRow(
        allocation({ position_keys: ['miss', 'k'] }),
        new Map([['k', riskCapital({ symbol: 'found' })]]),
        'ready',
        prime(),
      );

      expect(row.risk.entry?.symbol).toBe('found');
    });

    it('has no entry when no key matches, and when there are no keys', () => {
      const map = new Map([['k', riskCapital()]]);
      expect(
        toAllocationGridRow(
          allocation({ position_keys: ['z'] }),
          map,
          'ready',
          prime(),
        ).risk.entry,
      ).toBeUndefined();
      expect(
        toAllocationGridRow(allocation(), map, 'ready', prime()).risk.entry,
      ).toBeUndefined();
    });
  });

  describe('crrPct preference', () => {
    it("prefers the model's ratio over Sky's", () => {
      expect(
        buildRow(
          allocation({ position_keys: ['k'] }),
          riskCapital({ crr_pct: '4.47', reference_crr_pct: '9.99' }),
        ).risk.crrPct,
      ).toBe(4.47);
    });

    it("falls back to Sky's ratio only when the model reports none", () => {
      expect(
        buildRow(
          allocation({ position_keys: ['k'] }),
          riskCapital({ crr_pct: null, reference_crr_pct: '9.99' }),
        ).risk.crrPct,
      ).toBe(9.99);
    });

    it('keeps a model-published zero over a non-zero reference', () => {
      expect(
        buildRow(
          allocation({ position_keys: ['k'] }),
          riskCapital({ crr_pct: '0', reference_crr_pct: '9.99' }),
        ).risk.crrPct,
      ).toBe(0);
    });
  });

  describe('chainMismatch', () => {
    it('flags a receipt-token row on a chain the risk fetch did not cover', () => {
      expect(
        buildRow(
          allocation({ chain_id: 10, receipt_token_id: 736 }),
          undefined,
          prime({ chain_id: 1 }),
        ).risk.chainMismatch,
      ).toBe(true);
    });

    it("does not flag a row on the prime's own chain", () => {
      expect(
        buildRow(
          allocation({ chain_id: 1, receipt_token_id: 736 }),
          undefined,
          prime({ chain_id: 1 }),
        ).risk.chainMismatch,
      ).toBe(false);
    });

    it('does not flag a row that could never carry a figure', () => {
      expect(
        buildRow(
          allocation({
            chain_id: 0,
            receipt_token_id: null,
            category: 'custody',
          }),
          undefined,
          prime({ chain_id: 1 }),
        ).risk.chainMismatch,
      ).toBe(false);
    });

    it('does not flag anything without a selected prime', () => {
      expect(
        buildRow(
          allocation({ chain_id: 10, receipt_token_id: 736 }),
          undefined,
          null,
        ).risk.chainMismatch,
      ).toBe(false);
    });
  });

  describe('fromReference', () => {
    it('is true when STL priced nothing', () => {
      expect(
        buildRow(allocation({ amount_usd: null, reference_amount_usd: '5' }))
          .risk.fromReference,
      ).toBe(true);
    });

    it('is true for a wholly Sky-reported row', () => {
      expect(
        buildRow(
          allocation({ amount_usd: '1000', position_keys: ['k'] }),
          riskCapital({ source: 'reference' }),
        ).risk.fromReference,
      ).toBe(true);
    });

    it("is true when Sky's ratio filled in for a missing model ratio", () => {
      expect(
        buildRow(
          allocation({ amount_usd: '1000', position_keys: ['k'] }),
          riskCapital({ crr_pct: null, reference_crr_pct: '9' }),
        ).risk.fromReference,
      ).toBe(true);
    });

    it("is false when both figures are the model's own", () => {
      expect(
        buildRow(
          allocation({ amount_usd: '1000', position_keys: ['k'] }),
          riskCapital({ crr_pct: '5', reference_crr_pct: '9' }),
        ).risk.fromReference,
      ).toBe(false);
    });
  });
});

describe('withRrcShare', () => {
  it('gives shares that sum to 1 across contributing rows', () => {
    const shares = withRrcShare([
      rowWithRrc(50),
      rowWithRrc(30),
      rowWithRrc(20),
    ]).map((row) => row.risk.sharePct);

    expect(shares).toEqual([0.5, 0.3, 0.2]);
    expect(
      shares.reduce<number>((sum, share) => sum + (share ?? 0), 0),
    ).toBeCloseTo(1, 10);
  });

  it('excludes a chain-mismatched row from both numerator and denominator', () => {
    const rows = withRrcShare([
      rowWithRrc(50),
      rowWithRrc(50),
      rowWithRrc(100, true),
    ]);

    expect(rows.map((row) => row.risk.sharePct)).toEqual([0.5, 0.5, null]);
  });

  it('excludes a row with no derived requirement', () => {
    const rows = withRrcShare([
      rowWithRrc(75),
      rowWithRrc(25),
      rowWithRrc(null),
    ]);

    expect(rows.map((row) => row.risk.sharePct)).toEqual([0.75, 0.25, null]);
  });

  it('leaves rows untouched when nothing contributes', () => {
    const input = [rowWithRrc(null), rowWithRrc(0)];
    expect(withRrcShare(input)).toBe(input);
  });
});
