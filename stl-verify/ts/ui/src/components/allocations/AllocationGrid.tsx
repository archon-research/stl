import {
  Badge,
  type ColumnDef,
  DataTable,
  EmptyState,
  ErrorState,
  SearchInput,
  type SkeletonColumnHint,
  type SortingState,
  StyledSelect,
  useDataTable,
} from '@archon-research/design-system';
import { toSearchOption } from '@archon-research/router-kit';
import { useNavigate, useSearch } from '@tanstack/react-router';
import { useEffect, useMemo, useState, type ChangeEvent } from 'react';

import { css, cx } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { getActionColorClass, getActionIcon } from '../../lib/activity';
import {
  encumbranceSeverity,
  formatDateTime,
  formatFreshnessLabel,
  formatPercentValue,
  formatRatioPercent,
  formatTokenAmount,
  formatUsdValue,
  getAllocationKey,
  getCategoryLabel,
  getChainLabel,
  getExplorerUrl,
  getProtocolLabel,
  parseNumericValue,
  type ChainLabelLookup,
} from '../../lib/dashboard';
import {
  preferIndexed,
  preferReference,
  useProvenanceView,
} from '../../lib/provenance';
import { ALLOCATION_CATEGORIES } from '../../router/search-params';
import type {
  Allocation,
  AllocationCategory,
  AllocationRiskCapital,
  Prime,
  PrimeDebtBucket,
  PrimeDebtSnapshot,
  PrimeRiskCapital,
} from '../../types/allocation';
import type { LocalProtocolRow } from '../../types/local-data';
import {
  ChainLogo,
  PageShell,
  ProtocolLogo,
  tableHeaderTypographyClassName,
  TokenAddress,
  TokenLogo,
} from '../shared';
import { findMetricChart, type MetricChartSpec } from './metricCards';
import { PrimeMetricsBand } from './PrimeMetricsBand';
import { TabNotePanel } from './tabs/TabStatePanels';

type AllocationGridProps = {
  allocations: Allocation[];
  riskCapital: PrimeRiskCapital | null;
  chainLabels: ChainLabelLookup;
  errorMessage: string | null;
  filteredAllocations: Allocation[];
  topMetricsAllocations: Allocation[];
  isLoading: boolean;
  isRiskCapitalLoading: boolean;
  isPrimeDebtLoading: boolean;
  localProtocols: LocalProtocolRow[];
  onSelectAllocation: (allocationKey: string) => void;
  primeDebtSnapshot: PrimeDebtSnapshot | null;
  referenceDebt: PrimeDebtBucket | null;
  onSearchChange: (value: string) => void;
  onSortingChange: (
    sorting: SortingState | ((old: SortingState) => SortingState),
  ) => void;
  searchValue: string;
  selectedAllocationKey: string | null;
  selectedPrime: Prime | null;
  sorting: SortingState;
  metricCharts: MetricChartSpec[];
  isChartsLoading: boolean;
  chartsErrorMessage: string | null;
  riskCapitalErrorMessage: string | null;
  primeDebtErrorMessage: string | null;
  noticeMessage: string | null;
  primeCollateralUsd: number | null;
  primeCollateralObservedAt: string | null;
  capitalObservedAt: string | null;
};

// Fill override for the `Badge` these chips render as: `Badge`'s `colorPalette`
// ships six status-flavoured hues, so it cannot give five strategy categories
// distinct fills, and its red would read as an alarm on a routine category. The
// `categorical.*` tokens encode grouping without status meaning, and their hue
// order matches `chart.series`, so a chip and its series line read as the same
// category. Everything else about the chip — radius, weight, size, padding — is
// the recipe's.
//
// One literal `css()` call per category, evaluated at module scope, so the cell
// picks a finished class name: see `lib/activity.tsx` for why Panda cannot
// extract a token path handed in as a variable.
const CATEGORY_CHIP_CLASS: Record<AllocationCategory | 'unknown', string> = {
  allocation: css({ bg: 'categorical.1.bg', color: 'categorical.1.fg' }),
  pol: css({ bg: 'categorical.2.bg', color: 'categorical.2.fg' }),
  psm3: css({ bg: 'categorical.3.bg', color: 'categorical.3.fg' }),
  asset: css({ bg: 'categorical.4.bg', color: 'categorical.4.fg' }),
  custody: css({ bg: 'categorical.5.bg', color: 'categorical.5.fg' }),
  // No override: `Badge`'s own subtle × neutral default is this fill.
  unknown: '',
};

function getCategoryChipClass(
  category: AllocationCategory | undefined,
): string {
  // `AllocationCategory` is a compile-time union over an unvalidated API response,
  // so a category the backend adds later arrives as an unlisted string. Keying on
  // own-property presence rather than `?? 'unknown'` means that renders the neutral
  // chip instead of an unstyled one -- matching how getCategoryLabel already
  // degrades.
  return category !== undefined && Object.hasOwn(CATEGORY_CHIP_CLASS, category)
    ? CATEGORY_CHIP_CLASS[category]
    : CATEGORY_CHIP_CLASS.unknown;
}

/**
 * The badge text for a row only one provenance reported, or `null`.
 *
 * Both provenances get a badge, not just Sky's: under `source=both` the API
 * carries a row whichever side reported it, so an STL-only row is as much a
 * single-sourced figure as a Sky-only one — and it can be a large one. The
 * merged mainnet spark response puts a $57M `spWETH` position in that bucket,
 * well above several rows that do carry the Sky-only mark.
 *
 * This marks most of the table rather than a handful, and that is the honest
 * reading: Sky's monitor covers mainnet spark far more than the other chains,
 * so across a vault's primes a corroborated row is the exception. Spark's vault
 * view badges 25 of 33 rows. A bare row means both sides reported the position,
 * which is the claim worth being able to trust on sight.
 */
function soleReporterLabel(
  source: Allocation['source'],
  shown: { showsIndexed: boolean; showsReference: boolean },
): string | null {
  if (source === 'reference') return shown.showsIndexed ? 'Legacy only' : null;
  if (source === 'indexed') return shown.showsReference ? 'Verify only' : null;
  return null;
}

function AllocationAssetCell({
  allocation,
  localProtocols,
  chainLabels,
}: {
  allocation: Allocation;
  localProtocols: LocalProtocolRow[];
  chainLabels: ChainLabelLookup;
}) {
  // A badge marks a row against the other provenance's rows, so it says nothing
  // unless those are on screen too — which is only the merged view, since a
  // single-provenance response holds nothing to stand out from.
  const { showsIndexed: showsIndexedNow, showsReference: showsReferenceNow } =
    useProvenanceView();
  const soleReporter = soleReporterLabel(allocation.source, {
    showsIndexed: showsIndexedNow,
    showsReference: showsReferenceNow,
  });
  const chainLabel = getChainLabel(
    allocation.chain_id,
    chainLabels,
    allocation.network,
  );

  return (
    <div className={css({ display: 'grid', gap: '1', minWidth: 0 })}>
      <div className={flex({ align: 'center', gap: '1.5', wrap: 'wrap' })}>
        <p
          className={css({
            m: 0,
            fontSize: 'sm',
            fontWeight: 'semibold',
            color: 'text.strong',
          })}
        >
          {allocation.symbol}
        </p>
        {/* Absent exactly when both provenances reported the row — see
            `soleReporterLabel` for why that, and not scarcity, is the rule. */}
        {soleReporter === null ? null : (
          <Badge size="sm" variant="subtle">
            {soleReporter}
          </Badge>
        )}
      </div>
      <div className={flex({ gap: '1.5', wrap: 'wrap' })}>
        <span
          className={css({
            fontSize: 'xs',
            color: 'text.muted',
            display: 'inline-flex',
            alignItems: 'center',
            gap: '1.5',
            whiteSpace: 'nowrap',
          })}
        >
          <ProtocolLogo
            protocolName={getProtocolLabel(
              allocation.protocol_name,
              localProtocols,
              allocation.chain_id,
            )}
            size="5"
          />
          {getProtocolLabel(
            allocation.protocol_name,
            localProtocols,
            allocation.chain_id,
          )}
        </span>
        <span
          className={css({
            fontSize: 'xs',
            color: 'text.muted',
            display: 'inline-flex',
            alignItems: 'center',
            gap: '1.5',
            whiteSpace: 'nowrap',
          })}
        >
          <ChainLogo
            chainId={allocation.chain_id}
            label={chainLabel}
            size="5"
          />
          {chainLabel}
        </span>
      </div>
    </div>
  );
}

function AllocationUnderlyingCell({ allocation }: { allocation: Allocation }) {
  return (
    <div
      className={css({
        display: 'flex',
        flexDirection: 'column',
        gap: '1',
      })}
    >
      <div className={flex({ align: 'center', gap: '2' })}>
        <TokenLogo
          address={allocation.underlying_token_address}
          chainId={allocation.chain_id}
          size="6"
          symbol={allocation.underlying_symbol}
        />
        <span
          className={css({
            fontSize: 'sm',
            fontWeight: 'semibold',
            color: 'text.strong',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
            m: 0,
          })}
        >
          {allocation.underlying_symbol}
        </span>
      </div>
      <TokenAddress
        address={allocation.underlying_token_address}
        chainId={allocation.chain_id}
        compact
        style={{ fontSize: '0.8rem' }}
      />
    </div>
  );
}

function AllocationExposureCell({ row }: { row: AllocationGridRow }) {
  const allocation = row;
  // The column shows Verify's value and falls back to Legacy's, so the title
  // names whichever side is not on display — including the case where Verify
  // has no figure at all, which a bare number would otherwise pass off as its
  // own valuation.
  const exposureUsd = row.risk.exposureUsd;
  const verifyUsd = parseNumericValue(allocation.amount_usd);
  const legacyUsd = parseNumericValue(allocation.reference_amount_usd);
  const valuationTitle =
    verifyUsd === null
      ? legacyUsd === null
        ? undefined
        : "Legacy's value; Verify prices none of this position"
      : legacyUsd === null
        ? undefined
        : `Verify's value; Legacy reports ${formatUsdValue(legacyUsd)}`;

  return (
    <div
      className={css({
        display: 'flex',
        flexDirection: 'column',
        gap: '1',
      })}
    >
      <div className={flex({ align: 'center', gap: '2' })}>
        <TokenLogo
          address={allocation.receipt_token_address}
          chainId={allocation.chain_id}
          protocolName={allocation.protocol_name}
          size="6"
          symbol={allocation.symbol}
        />
        <span
          className={css({
            fontSize: 'sm',
            fontWeight: 'semibold',
            color: 'text.strong',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
            // Tabular figures so amounts align down the column. Not the
            // column's `meta.mono`: this cell is a composite (logo, value,
            // address) and mono would restyle all of it.
            fontVariantNumeric: 'tabular-nums',
            m: 0,
          })}
        >
          <span title={valuationTitle}>
            {exposureUsd !== null
              ? formatUsdValue(exposureUsd)
              : `${formatTokenAmount(allocation.balance)} ${allocation.symbol}`}
          </span>
        </span>
      </div>
      <TokenAddress
        address={allocation.receipt_token_address}
        chainId={allocation.chain_id}
        compact
        style={{ fontSize: '0.8rem' }}
      />
    </div>
  );
}

// Approximate the latest flow's USD value from the position's current implied
// price (amount_usd / balance) rather than a historical price: the activity
// row carries only a token-unit tx_amount, and this is a magnitude annotation,
// not an accounting figure. Falls back to the token amount when unpriced.
function formatActivityMagnitude(allocation: Allocation): string | null {
  const amount = parseNumericValue(allocation.latest_activity_amount);
  // Sweeps are internal reallocations with tx_amount 0; show the icon alone
  // rather than a misleading "$0.00".
  if (amount === null || amount === 0) {
    return null;
  }

  const action = allocation.latest_activity_action?.toLowerCase();
  const sign = action === 'in' ? '+' : action === 'out' ? '-' : '';

  const balance = parseNumericValue(allocation.balance);
  const amountUsd = parseNumericValue(allocation.amount_usd);
  if (amountUsd !== null && balance !== null && balance > 0) {
    return `${sign}${formatUsdValue(amount * (amountUsd / balance))}`;
  }

  return `${sign}${formatTokenAmount(amount)} ${allocation.symbol}`;
}

function AllocationActivityCell({ allocation }: { allocation: Allocation }) {
  if (!allocation.latest_activity_at) {
    return (
      <p
        className={css({
          m: 0,
          fontSize: 'sm',
          color: 'text.muted',
        })}
      >
        —
      </p>
    );
  }

  const actionColorClass = getActionColorClass(
    allocation.latest_activity_action,
  );
  const actionIcon = getActionIcon(allocation.latest_activity_action);
  const magnitude = formatActivityMagnitude(allocation);

  return (
    <div>
      <div className={flex({ align: 'center', gap: '1.5' })}>
        {actionIcon ? (
          <span
            className={cx(css({ display: 'inline-flex' }), actionColorClass)}
          >
            {actionIcon}
          </span>
        ) : null}
        <span
          className={css({
            fontSize: 'sm',
            fontWeight: 'semibold',
            color: 'text.strong',
          })}
        >
          {formatFreshnessLabel(allocation.latest_activity_at)}
        </span>
        {magnitude ? (
          <span
            className={cx(
              css({
                fontSize: 'xs',
                fontWeight: 'medium',
                whiteSpace: 'nowrap',
              }),
              actionColorClass,
            )}
          >
            {magnitude}
          </span>
        ) : null}
      </div>
      <p
        className={css({
          m: 0,
          fontSize: 'xs',
          color: 'text.muted',
        })}
      >
        {formatDateTime(allocation.latest_activity_at)}
      </p>
    </div>
  );
}

function AllocationCategoryCell({ allocation }: { allocation: Allocation }) {
  const category = allocation.category;

  return (
    <Badge className={getCategoryChipClass(category)}>
      {getCategoryLabel(category)}
    </Badge>
  );
}

// Joined on the keys both endpoints publish rather than on `receipt_token_id`,
// which only STL's own rows carry: Sky prices off-chain custody and the Arkis
// vault — its two largest requirements — and STL resolves no receipt token for
// either, so an id join left them blank. The keys encode the rules that make
// those pair up (custody by protocol, everything else by chain and address), so
// the grid does not restate them.
function lookupAllocationRiskCapital(
  riskByPositionKey: Map<string, AllocationRiskCapital>,
  allocation: Allocation,
): AllocationRiskCapital | undefined {
  for (const key of allocation.position_keys ?? []) {
    const entry = riskByPositionKey.get(key);
    if (entry !== undefined) return entry;
  }

  return undefined;
}

// Required risk capital in USD, derived rather than carried: `crr x exposure`,
// against the two figures shown beside it in the same row.
//
// Neither provenance's published RRC is used. Under `both` the ratio is Sky's
// and the exposure is STL's, so upstream's own requirement was computed against
// a different exposure than the one on screen and the three columns did not
// reconcile. Deriving costs provenance purity — the product of two sources is a
// figure neither published — and buys a row a reader can check by eye, which is
// the trade this table is for.
function derivedRiskCapitalUsd(
  crrPct: number | null,
  exposureUsd: number | null,
): number | null {
  if (crrPct === null || exposureUsd === null) {
    return null;
  }

  // `crrPct` is a 0-100 percentage at every boundary in this codebase; upstream
  // reports a 0-1 fraction and the adapter rescales once.
  return (exposureUsd * crrPct) / 100;
}

// Comparable capital-risk ratio (0-100), Sky's preferred over STL's.
function preferredCrrPct(
  entry: AllocationRiskCapital | undefined,
): number | null {
  if (entry === undefined) {
    return null;
  }

  return parseNumericValue(
    preferReference(entry.reference_crr_pct, entry.crr_pct),
  );
}

// Each row's share of the requirement the table itself accounts for, 0-1.
//
// The denominator is Σ of the RRC column over the rows on screen, not the
// prime's published total: the RRC column is derived per row, so dividing by a
// total computed some other way would not sum to 1 and a reader adding the
// column up would find it short. `encumbrance_contribution` on the row divides
// by available capital instead and sums to the encumbrance ratio, which is a
// different question.
//
// Filtering therefore rebases the column — it answers "of what is shown" — and
// rows with no RRC are absent from both the numerator and the sum.
//
// Summing the visible rows was tried before and abandoned, when the RRC column
// was carried from the risk-capital response: Sky priced positions STL resolved
// no receipt token for — off-chain custody and the Arkis vault, its two largest
// — so they never reached a grid row and the denominator collapsed to $2.8M
// against a real $19.1M. Deriving each row's RRC from the CRR and exposure it
// already shows is what makes this safe now, because both of those positions do
// carry a CRR: measured against spark, they are the two largest contributors to
// a $22.9M sum. Check that before carrying the column back to a published
// total — the failure was a join gap, not the arithmetic.
function withRrcShare(rows: AllocationGridRow[]): AllocationGridRow[] {
  const total = rows.reduce(
    (sum, row) => sum + (row.risk.riskCapitalUsd ?? 0),
    0,
  );
  if (total === 0) {
    return rows;
  }

  return rows.map((row) =>
    row.risk.riskCapitalUsd === null
      ? row
      : {
          ...row,
          risk: { ...row.risk, sharePct: row.risk.riskCapitalUsd / total },
        },
  );
}

// riskByPositionKey is built from a risk-capital call scoped to
// selectedPrime's own chain, so an allocation on a different chain has no
// entry there for the same reason a genuine non-applicable allocation
// doesn't: the map simply has nothing for its receipt_token_id. Distinguish
// the two so a real risk capital figure that is merely uncomputed for this
// chain doesn't read as the same "n/a" as an allocation no risk model
// applies to.
//
// The receipt_token_id check gates this to rows that could ever carry a
// figure. A null receipt_token_id (the Anchorage custody row, and every
// direct/bare holding) can never key into riskByPositionKey regardless of
// chain, so without this check a mainnet-primary prime would flag its own
// off-chain custody row (chain_id 0) as a cross-chain mismatch and claim a
// figure exists but merely wasn't fetched, when "n/a" is the correct read.
function isRiskCapitalChainMismatch(
  selectedPrime: Prime | null,
  allocation: Allocation,
): boolean {
  return (
    allocation.receipt_token_id != null &&
    selectedPrime !== null &&
    allocation.chain_id !== selectedPrime.chain_id
  );
}

function AllocationRiskCapitalCell({
  risk,
}: {
  risk: AllocationGridRow['risk'];
}) {
  if (risk.chainMismatch) {
    return (
      <p
        title="Risk capital is not yet available for non-mainnet allocations."
        className={css({ m: 0, fontSize: 'sm', color: 'text.muted' })}
      >
        Not yet available
      </p>
    );
  }

  const unsettled = riskFetchPlaceholder(risk.state);
  if (unsettled) {
    return (
      <p
        title={unsettled.title}
        className={css({ m: 0, fontSize: 'sm', color: 'text.muted' })}
      >
        {unsettled.label}
      </p>
    );
  }

  if (risk.riskCapitalUsd === null) {
    return (
      <p className={css({ m: 0, fontSize: 'sm', color: 'text.muted' })}>n/a</p>
    );
  }

  return (
    <p
      title={riskProvenanceTitle(risk)}
      className={css({
        m: 0,
        fontSize: 'sm',
        fontWeight: 'semibold',
        color: 'text.strong',
      })}
    >
      {formatUsdValue(risk.riskCapitalUsd)}
    </p>
  );
}

/**
 * A tooltip, not a chip: under the composite view Legacy's ratio wins wherever
 * it reports one, so a visible marker would land on most rows and stop marking
 * anything (the same reason the Asset column badges only single-reporter rows).
 */
function riskProvenanceTitle(risk: AllocationGridRow['risk']): string {
  return risk.fromReference ? 'Legacy published figure' : 'Verify model figure';
}

/**
 * The muted stand-in for a risk cell whose figure is not settled, or null once
 * it is. Distinct from `n/a`: that asserts no model applies, which only the
 * settled state can claim.
 */
function riskFetchPlaceholder(
  state: RiskFetchState,
): { label: string; title: string } | null {
  if (state === 'loading') {
    return { label: '…', title: 'Loading risk capital' };
  }
  if (state === 'error') {
    return {
      label: 'unavailable',
      title: 'The risk-capital request failed; retry from the metrics band.',
    };
  }
  return null;
}

/**
 * A grid row carries its risk figures as data rather than having the column
 * accessors look them up: TanStack caches `row.getValue` per row for the
 * lifetime of a `data` identity, so a lookup through a column closure freezes
 * at whatever the map held when a value was first read — risk capital arrives
 * after the allocations, and sorting by CRR ordered the stale nulls while the
 * cells (re-rendered with the fresh closure) showed real figures. Deriving the
 * figures into the rows makes risk arrival a `data` change, which is the one
 * signal TanStack rebuilds its caches on.
 */
/**
 * `state` keeps the risk columns honest while the risk-capital call is not
 * settled: `n/a` claims no model applies, which is false both mid-flight and
 * after a failed fetch — a staging outage once rendered a full column of
 * `n/a` with the only error signal far away in the metrics band.
 */
type RiskFetchState = 'loading' | 'error' | 'ready';

type AllocationGridRow = Allocation & {
  risk: {
    state: RiskFetchState;
    entry: AllocationRiskCapital | undefined;
    chainMismatch: boolean;
    /** True when the figures shown lean on Legacy's published values. */
    fromReference: boolean;
    exposureUsd: number | null;
    riskCapitalUsd: number | null;
    crrPct: number | null;
    sharePct: number | null;
  };
};

// The value the Exposure column shows, as a number.
//
// The headline total is the sum of this over every row, so the two are the same
// rule read twice rather than two rules that happen to agree. Sky-only rows are
// included: after the merge each position appears once whichever side reported
// it, so summing them all counts the money once — and excluding them made the
// headline unable to match the table it sits above by construction.
function rowExposureUsd(allocation: Allocation): number | null {
  return parseNumericValue(
    preferIndexed(allocation.amount_usd, allocation.reference_amount_usd),
  );
}

function toAllocationGridRow(
  allocation: Allocation,
  riskByPositionKey: Map<string, AllocationRiskCapital>,
  riskFetchState: RiskFetchState,
  selectedPrime: Prime | null,
): AllocationGridRow {
  const entry = lookupAllocationRiskCapital(riskByPositionKey, allocation);
  // The row's own value, which is the same measurement in both provenances —
  // not the risk-capital breakdown's `exposure`, which covers only the priced
  // subset and runs about a third smaller.
  const exposureUsd = rowExposureUsd(allocation);
  const crrPct = preferredCrrPct(entry);
  return {
    ...allocation,
    risk: {
      state: riskFetchState,
      entry,
      chainMismatch: isRiskCapitalChainMismatch(selectedPrime, allocation),
      // Whether the figures shown lean on Sky. The ratio prefers Sky's and the
      // value prefers STL's, so a row is Sky-flavoured when either side of that
      // actually fell to Sky.
      fromReference:
        allocation.amount_usd == null ||
        (entry !== undefined &&
          (entry.source === 'reference' || entry.reference_crr_pct != null)),
      exposureUsd,
      riskCapitalUsd: derivedRiskCapitalUsd(crrPct, exposureUsd),
      crrPct,
      // Filled by `withRrcShare` once every row is known: the denominator is
      // the column's own sum, which no single row can see.
      sharePct: null,
    },
  };
}

function createAllocationColumns(
  chainLabels: ChainLabelLookup,
  localProtocols: LocalProtocolRow[],
): ColumnDef<AllocationGridRow>[] {
  return [
    {
      id: 'symbol',
      header: 'Asset',
      accessorFn: (allocation) => allocation.symbol,
      cell: ({ row }) => (
        <AllocationAssetCell
          allocation={row.original}
          chainLabels={chainLabels}
          localProtocols={localProtocols}
        />
      ),
    },
    {
      id: 'underlying_symbol',
      header: 'Underlying',
      accessorFn: (allocation) => allocation.underlying_symbol,
      cell: ({ row }) => <AllocationUnderlyingCell allocation={row.original} />,
    },
    {
      // Named for what it renders: `amount_usd`, the position's USD exposure —
      // the same quantity Sky's monitor publishes as EXPOSURE. The token
      // quantity appears only as the fallback for an unpriced row.
      id: 'exposure',
      header: 'Exposure',
      // Sorts on what the cell shows. Sorting the token balance instead would
      // order 4,722 BTC below 869M spUSDS while the column displays $250M above
      // $869M. An unpriced row has no exposure to sort by, so it sorts last
      // rather than tying with a genuine zero.
      accessorFn: (allocation) => allocation.risk.exposureUsd ?? -1,
      cell: ({ row }) => <AllocationExposureCell row={row.original} />,
      // Bar reflects USD value so magnitudes compare across heterogeneous
      // tokens; the cell text keeps the token holding. NaN (not null) suppresses
      // the bar for unpriced rows: a null here would fall back to the column
      // accessor (token balance), mixing token units into the USD domain.
      meta: {
        magnitude: {
          scale: 'linear',
          getValue: (allocation) => allocation.risk.exposureUsd ?? NaN,
          getValueText: () => null,
        },
      },
    },
    {
      id: 'latest_activity_at',
      header: 'Latest Activity',
      accessorFn: (allocation) => {
        const latestActivityAt = allocation.latest_activity_at;
        return latestActivityAt ? new Date(latestActivityAt).getTime() : 0;
      },
      cell: ({ row }) => <AllocationActivityCell allocation={row.original} />,
    },
    {
      id: 'category',
      header: 'Category',
      accessorFn: (allocation) => allocation.category,
      cell: ({ row }) => <AllocationCategoryCell allocation={row.original} />,
    },
    {
      id: 'risk_capital',
      // Named as Sky names it, since the two are compared side by side.
      header: 'RRC',
      // A row without a figure — chain-mismatched or no model — sorts below
      // genuine zeroes (-1) rather than tying with them.
      accessorFn: (allocation) =>
        allocation.risk.chainMismatch
          ? -1
          : (allocation.risk.riskCapitalUsd ?? -1),
      cell: ({ row }) => <AllocationRiskCapitalCell risk={row.original.risk} />,
      // No bar for n/a or chain-mismatched rows: NaN suppresses it (see
      // Balance for why not null).
      meta: {
        magnitude: {
          scale: 'linear',
          getValue: (allocation) =>
            allocation.risk.chainMismatch
              ? NaN
              : (allocation.risk.riskCapitalUsd ?? NaN),
          getValueText: () => null,
        },
        // Single-value USD cell, so the column can take mono + tabular figures
        // wholesale.
        mono: true,
        align: 'right',
      },
    },
    {
      id: 'crr',
      header: 'CRR',
      // A row with no ratio sorts below a genuine 0% rather than tying with it.
      accessorFn: (allocation) =>
        allocation.risk.chainMismatch ? -1 : (allocation.risk.crrPct ?? -1),
      cell: ({ row }) => (
        <AllocationRatioCell
          value={
            row.original.risk.chainMismatch ? null : row.original.risk.crrPct
          }
          format={formatPercentValue}
          state={row.original.risk.state}
          title={riskProvenanceTitle(row.original.risk)}
        />
      ),
      meta: {
        magnitude: {
          scale: 'linear',
          // Pinned to the ratio's own scale: a column-relative domain would
          // render 40/45/50% as empty→half→full, hiding the absolute level.
          domain: { min: 0, max: 100 },
          getValue: (allocation) =>
            allocation.risk.chainMismatch
              ? NaN
              : (allocation.risk.crrPct ?? NaN),
          getValueText: () => null,
        },
        mono: true,
        align: 'right',
      },
    },
    {
      id: 'rrc_share',
      header: 'RRC share',
      accessorFn: (allocation) =>
        allocation.risk.chainMismatch ? -1 : (allocation.risk.sharePct ?? -1),
      cell: ({ row }) => (
        <AllocationRatioCell
          value={
            row.original.risk.chainMismatch ? null : row.original.risk.sharePct
          }
          format={formatRatioPercent}
          state={row.original.risk.state}
          title={riskProvenanceTitle(row.original.risk)}
        />
      ),
      meta: {
        magnitude: {
          scale: 'linear',
          getValue: (allocation) =>
            allocation.risk.chainMismatch
              ? NaN
              : (allocation.risk.sharePct ?? NaN),
          getValueText: () => null,
        },
        mono: true,
        align: 'right',
      },
    },
  ];
}

function AllocationRatioCell({
  value,
  format,
  state,
  title,
}: {
  value: number | null;
  format: (value: number | null) => string;
  state: RiskFetchState;
  title?: string;
}) {
  const unsettled = riskFetchPlaceholder(state);
  if (unsettled) {
    return (
      <p
        title={unsettled.title}
        className={css({ m: 0, fontSize: 'sm', color: 'text.muted' })}
      >
        {state === 'loading' ? unsettled.label : '—'}
      </p>
    );
  }

  return (
    <p
      title={value === null ? undefined : title}
      className={css({
        m: 0,
        fontSize: 'sm',
        fontWeight: 'semibold',
        color: value === null ? 'text.muted' : 'text.strong',
      })}
    >
      {format(value)}
    </p>
  );
}

export function AllocationGrid({
  allocations,
  riskCapital,
  chainLabels,
  errorMessage,
  filteredAllocations,
  topMetricsAllocations,
  isLoading,
  isRiskCapitalLoading,
  isPrimeDebtLoading,
  localProtocols,
  onSelectAllocation,
  primeDebtSnapshot,
  referenceDebt,
  onSearchChange,
  onSortingChange,
  searchValue,
  selectedAllocationKey,
  selectedPrime,
  sorting,
  metricCharts,
  isChartsLoading,
  chartsErrorMessage,
  riskCapitalErrorMessage,
  primeDebtErrorMessage,
  noticeMessage,
  primeCollateralUsd,
  primeCollateralObservedAt,
  capitalObservedAt,
}: AllocationGridProps) {
  // The provenance on screen, not the one fetched: narrowing a composite
  // response changes what is shown without issuing a request.
  const { showsReference: showsReferenceNow } = useProvenanceView();
  const [localSearchValue, setLocalSearchValue] = useState(searchValue);

  // The category filter lives in the URL rather than in local state so it is
  // shareable alongside the other grid filters, and so the shell's per-prime
  // reset clears it with `network`/`protocol`. Not strict, matching the drawer:
  // the shell renders this from the root route, so the match is not guaranteed.
  const allocationSearch = useSearch({
    from: '/allocation',
    shouldThrow: false,
  });
  const navigate = useNavigate();
  const categoryFilter: AllocationCategory | '' =
    allocationSearch?.category ?? '';

  const handleCategoryChange = (value: string) => {
    void navigate({
      to: '.',
      search: (previous) => ({
        ...previous,
        category: toSearchOption(value, ALLOCATION_CATEGORIES),
      }),
      replace: true,
    });
  };

  // Composes with — never replaces — the search box and the top bar's
  // network/protocol filters: those are already applied upstream in
  // `filteredAllocations`, and this narrows what survives them.
  const visibleAllocations = useMemo(
    () =>
      categoryFilter === ''
        ? filteredAllocations
        : filteredAllocations.filter(
            (allocation) => allocation.category === categoryFilter,
          ),
    [categoryFilter, filteredAllocations],
  );

  useEffect(() => {
    setLocalSearchValue(searchValue);
  }, [searchValue]);

  useEffect(() => {
    if (localSearchValue === searchValue) {
      return;
    }

    const timeoutId = window.setTimeout(() => {
      onSearchChange(localSearchValue);
    }, 300);

    return () => window.clearTimeout(timeoutId);
  }, [localSearchValue, onSearchChange, searchValue]);

  const summary = useMemo(() => {
    if (topMetricsAllocations.length === 0) {
      return null;
    }

    const totalUsd = topMetricsAllocations.reduce(
      (sum, allocation) => sum + (rowExposureUsd(allocation) ?? 0),
      0,
    );

    const latestActivityAt = topMetricsAllocations.reduce<string | null>(
      (latest, allocation) => {
        if (!allocation.latest_activity_at) {
          return latest;
        }

        if (!latest) {
          return allocation.latest_activity_at;
        }

        return new Date(allocation.latest_activity_at) > new Date(latest)
          ? allocation.latest_activity_at
          : latest;
      },
      null,
    );

    return {
      allocationCount: topMetricsAllocations.length,
      latestActivityAt,
      totalUsd,
    };
  }, [topMetricsAllocations]);

  const overallSummary = useMemo(() => {
    if (allocations.length === 0) {
      return null;
    }

    return {
      allocationCount: allocations.length,
      totalUsd: allocations.reduce(
        (sum, allocation) => sum + (rowExposureUsd(allocation) ?? 0),
        0,
      ),
    };
  }, [allocations]);

  const debtWad = showsReferenceNow
    ? referenceDebt?.debt_wad
    : primeDebtSnapshot?.debt_wad;
  // Reference mode has no observation time: upstream publishes one figure per
  // prime per day, so the closest thing is the bucket the figure falls in. The
  // label says "as of" rather than "sync" so a boundary is not read as a
  // moment we observed the value.
  const debtObservedAt = showsReferenceNow
    ? referenceDebt?.bucket_start
    : primeDebtSnapshot?.synced_at;
  // "as of" either way: reference mode has only a daily bucket boundary, and
  // even the on-chain snapshot is a sync time rather than the block's own.
  const debtTimestampLabel = 'Debt as of';
  // Only reference mode lacks an ilk, but the label keys off its absence rather
  // than off the mode — an unknown ilk in either mode reads the same.
  const debtIlkLabel = primeDebtSnapshot?.ilk_name
    ? `Ilk ${primeDebtSnapshot.ilk_name}`
    : null;

  const hasSearchQuery = searchValue.trim().length > 0;

  const riskByPositionKey = useMemo(() => {
    const map = new Map<string, AllocationRiskCapital>();
    for (const entry of riskCapital?.per_allocation ?? []) {
      // Under every key the row answers to, so a grid row matching on any one
      // of its own finds it. First writer wins: the strongest key is listed
      // first, so a weaker one cannot displace it.
      for (const key of entry.position_keys ?? []) {
        if (!map.has(key)) map.set(key, entry);
      }
    }
    return map;
  }, [riskCapital]);

  // A new array when risk data lands, deliberately: see AllocationGridRow.
  const riskFetchState: RiskFetchState =
    riskCapital !== null
      ? 'ready'
      : isRiskCapitalLoading
        ? 'loading'
        : riskCapitalErrorMessage !== null
          ? 'error'
          : 'ready';
  const gridRows = useMemo<AllocationGridRow[]>(
    () =>
      withRrcShare(
        visibleAllocations.map((allocation) =>
          toAllocationGridRow(
            allocation,
            riskByPositionKey,
            riskFetchState,
            selectedPrime,
          ),
        ),
      ),
    [visibleAllocations, riskByPositionKey, riskFetchState, selectedPrime],
  );

  const columns = useMemo<ColumnDef<AllocationGridRow>[]>(
    () => createAllocationColumns(chainLabels, localProtocols),
    [chainLabels, localProtocols],
  );

  // Explicit hints replace DataTable's meta-derived ones wholesale, so they are
  // read off the same column defs rather than restated: only the leading Asset
  // cell needs a shape `meta` cannot express (a symbol over its protocol line).
  const skeletonColumnHints = useMemo<SkeletonColumnHint[]>(
    () =>
      columns.map((column, index) => {
        if (index === 0) return { kind: 'identity' };
        return column.meta?.align === 'right'
          ? { kind: 'numeric' }
          : { kind: 'text' };
      }),
    [columns],
  );

  const table = useDataTable(gridRows, columns, {
    enableSorting: true,
    onSortingChange,
    sorting,
  });

  const showTopMetricsSkeleton =
    selectedPrime !== null && (isLoading || isRiskCapitalLoading);

  const hasTopMetrics =
    riskCapital !== null || summary !== null || selectedPrime !== null;

  const allocationActivityChart = findMetricChart(
    metricCharts,
    'allocation-activity-volume',
  );
  const riskCapitalChart = findMetricChart(metricCharts, 'risk-capital');
  const totalCapitalChart = findMetricChart(metricCharts, 'total-capital');
  const primeDebtChart = findMetricChart(metricCharts, 'prime-debt-exposure');
  const primeCollateralChart = findMetricChart(
    metricCharts,
    'prime-collateral',
  );
  const encumbranceChart = findMetricChart(metricCharts, 'encumbrance-ratio');
  // One call decides the ratio for the card, its severity, its caption and the
  // chart's fallback value, so they cannot end up describing different
  // provenances — a Sky figure over a breach threshold beside STL's "within the
  // 100% breach level" would read as a bug in the threshold.
  const skyEncumbranceRatio = riskCapital?.reference_prime_encumbrance_ratio;
  const encumbranceRatio = parseNumericValue(
    preferReference(skyEncumbranceRatio, riskCapital?.prime_encumbrance_ratio),
  );
  const encumbranceBreach = encumbranceSeverity(encumbranceRatio);
  // Only STL's ratio is bounded by the chains STL does not serve. Sky's covers
  // whatever it covers, so the "at least this" caption below does not apply to
  // it.
  const unservedChains =
    skyEncumbranceRatio == null
      ? (riskCapital?.prime_unserved_chains ?? [])
      : [];
  // Absence has a cause worth naming: the ratio is required-over-total risk
  // capital, so it cannot be computed without a total. And where chains go
  // unserved the numerator is bounded, making the ratio a floor rather than a
  // measurement — on a risk surface that difference matters.
  // The band itself renders as a chip beside the value; this line carries only
  // what the chip cannot say — why a figure is absent, or that a bounded
  // numerator makes the ratio a floor rather than a measurement.
  const encumbranceCaption = (() => {
    if (encumbranceRatio === null) {
      return 'Needs total risk capital, which is not yet observed';
    }
    if (unservedChains.length > 0) {
      return `A floor: ${unservedChains.length} chain${unservedChains.length === 1 ? '' : 's'} unserved`;
    }
    return null;
  })();

  return (
    <PageShell>
      <div
        className={css({
          display: 'grid',
          gap: '4',
        })}
      >
        <div
          className={flex({
            align: 'flex-start',
            justify: 'space-between',
            gap: { base: '3', md: '4' },
            wrap: 'wrap',
          })}
        >
          <div
            className={css({
              display: 'grid',
              gap: '1',
              minWidth: { base: '0', md: '18rem' },
              flex: '1 1 20rem',
            })}
          >
            <div className={flex({ align: 'center', gap: '2.5' })}>
              {selectedPrime ? (
                <ProtocolLogo protocolName={selectedPrime.name} size="8" />
              ) : null}
              <h1
                className={css({
                  m: 0,
                  fontSize: { base: '3xl', md: '4xl' },
                  lineHeight: 'tight',
                  color: 'text.strong',
                })}
              >
                {selectedPrime ? selectedPrime.name : 'Select a prime'}
              </h1>
            </div>
            {/* The label ships with the address, never on its own: this is the
                one place the prime's wallet address is named, and an unlabelled
                hex string here was read as a balance. */}
            {selectedPrime ? (
              <div
                className={flex({
                  align: 'center',
                  gap: '1.5',
                  wrap: 'wrap',
                  rowGap: '0',
                })}
              >
                <span
                  className={css({
                    fontSize: 'xs',
                    color: 'text.muted',
                    whiteSpace: 'nowrap',
                  })}
                >
                  Raw wallet address:
                </span>
                <TokenAddress address={selectedPrime.id} />
              </div>
            ) : null}
          </div>
          {!showTopMetricsSkeleton ? (
            <div
              className={css({
                display: 'flex',
                flexWrap: 'wrap',
                gap: { base: '2.5', md: '4' },
                justifyContent: { base: 'flex-start', md: 'flex-end' },
                textAlign: { base: 'left', md: 'right' },
                flex: '1 1 22rem',
              })}
            >
              {summary ? (
                <div
                  className={css({
                    display: 'flex',
                    alignItems: 'center',
                    gap: '1.5',
                    flexWrap: 'wrap',
                    justifyContent: 'flex-end',
                  })}
                >
                  <span
                    className={css({
                      fontSize: 'sm',
                      fontWeight: 'semibold',
                      color: 'text.strong',
                    })}
                  >
                    Latest activity{' '}
                    {summary.latestActivityAt
                      ? formatFreshnessLabel(summary.latestActivityAt)
                      : '—'}
                  </span>
                  <span
                    className={css({
                      fontSize: 'xs',
                      lineHeight: 'short',
                      color: 'text.muted',
                    })}
                  >
                    {summary.latestActivityAt
                      ? formatDateTime(summary.latestActivityAt)
                      : 'No indexed activity'}
                  </span>
                </div>
              ) : null}
              {selectedPrime ? (
                <div
                  className={css({
                    display: 'flex',
                    alignItems: 'center',
                    gap: '1.5',
                    flexWrap: 'wrap',
                    justifyContent: 'flex-end',
                  })}
                >
                  <span
                    className={css({
                      fontSize: 'sm',
                      fontWeight: 'semibold',
                      color: 'text.strong',
                    })}
                  >
                    {debtTimestampLabel}{' '}
                    {isPrimeDebtLoading
                      ? 'Loading...'
                      : primeDebtErrorMessage
                        ? 'Error'
                        : debtObservedAt
                          ? formatFreshnessLabel(debtObservedAt)
                          : '—'}
                  </span>
                  <span
                    className={css({
                      fontSize: 'xs',
                      lineHeight: 'short',
                      color: 'text.muted',
                    })}
                  >
                    {isPrimeDebtLoading
                      ? 'Waiting for sync timestamp'
                      : primeDebtErrorMessage
                        ? primeDebtErrorMessage
                        : debtObservedAt
                          ? formatDateTime(debtObservedAt)
                          : 'No debt timestamp'}
                  </span>
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
        {noticeMessage === null ? null : (
          <TabNotePanel message={noticeMessage} />
        )}
        <PrimeMetricsBand
          isSkeleton={showTopMetricsSkeleton}
          hasTopMetrics={hasTopMetrics}
          summary={summary}
          overallSummary={overallSummary}
          hasSearchQuery={hasSearchQuery}
          riskCapital={riskCapital}
          capitalObservedAt={capitalObservedAt}
          riskCapitalErrorMessage={riskCapitalErrorMessage}
          summaryErrorMessage={errorMessage}
          primeDebtErrorMessage={primeDebtErrorMessage}
          hasPrime={selectedPrime !== null}
          collateral={{
            usd: primeCollateralUsd,
            observedAt: primeCollateralObservedAt,
            isLoading,
          }}
          encumbrance={{
            ratio: encumbranceRatio,
            caption: encumbranceCaption,
            severity: encumbranceBreach,
          }}
          debt={{
            explorerUrl: selectedPrime
              ? getExplorerUrl(
                  selectedPrime.chain_id,
                  selectedPrime.address,
                  'address',
                )
              : null,
            wad: debtWad,
            ilkLabel: debtIlkLabel,
            isLoading: isPrimeDebtLoading,
          }}
          charts={{
            activity: allocationActivityChart,
            exposure: riskCapitalChart,
            totalCapital: totalCapitalChart,
            collateral: primeCollateralChart,
            encumbrance: encumbranceChart,
            debt: primeDebtChart,
          }}
          isChartsLoading={isChartsLoading}
          chartsErrorMessage={chartsErrorMessage}
        />
        {/* The provenance footnote lived here. Extracted whole to
            `MetricsFootnote` and deliberately not rendered — see that file for
            why, and for how to switch it back on. */}
        <div
          className={css({
            display: 'grid',
            gridTemplateColumns: {
              base: '1fr',
              lg: 'auto minmax(28rem, 36rem)',
            },
            gap: { base: '3', md: '4', lg: '5' },
            alignItems: 'end',
          })}
        >
          <span
            className={css({
              display: 'inline-flex',
              width: 'fit-content',
              alignItems: 'center',
              borderRadius: 'full',
              bg: 'bg.neutral',
              px: '3',
              py: '1',
              fontSize: 'xs',
              fontWeight: 'semibold',
              letterSpacing: '0.1em',
              textTransform: 'uppercase',
              color: 'text.muted',
            })}
          >
            Allocations
          </span>
          {/* Same shape as the top bar's network/protocol filters — a
              `StyledSelect` in an 11rem cell whose placeholder option is the
              cleared state — so the three read as one filter family even though
              this one is scoped to the grid rather than the page. */}
          <div
            className={css({
              display: 'flex',
              flexWrap: 'wrap',
              alignItems: 'end',
              gap: '3',
              minWidth: '0',
              width: '100%',
              justifySelf: { lg: 'end' },
            })}
          >
            <div
              className={css({
                width: { base: '100%', sm: '11rem' },
                flexShrink: 0,
              })}
            >
              <StyledSelect
                aria-label="Filter by category"
                value={categoryFilter}
                onChange={(event: ChangeEvent<HTMLSelectElement>) =>
                  handleCategoryChange(event.target.value)
                }
                disabled={!selectedPrime}
              >
                <option value="">All categories</option>
                {ALLOCATION_CATEGORIES.map((category) => (
                  <option key={category} value={category}>
                    {getCategoryLabel(category)}
                  </option>
                ))}
              </StyledSelect>
            </div>
            <div
              className={css({
                flex: '1 1 16rem',
                minWidth: '0',
              })}
            >
              <SearchInput
                aria-label="Search allocations"
                disabled={!selectedPrime}
                onValueChange={setLocalSearchValue}
                placeholder="Search assets, protocols, chains"
                value={localSearchValue}
              />
            </div>
          </div>
        </div>
      </div>

      <div className={css({ mt: '6' })}>
        {!selectedPrime && !isLoading ? (
          <EmptyState
            title="Choose a prime to load positions"
            description="The main grid activates once a prime is selected from the sidebar."
            stretch
          />
        ) : null}

        {selectedPrime && errorMessage ? (
          <ErrorState
            title="Unable to load allocations"
            description="An error occurred while fetching allocation data."
            errorMessage={errorMessage}
            tone="critical"
            size="inline"
          />
        ) : null}

        {selectedPrime &&
        !errorMessage &&
        !isLoading &&
        allocations.length === 0 ? (
          <EmptyState
            title="No allocations returned"
            description="The selected prime did not return any allocation rows from the API."
            stretch
          />
        ) : null}

        {selectedPrime &&
        !errorMessage &&
        !isLoading &&
        allocations.length > 0 &&
        visibleAllocations.length === 0 ? (
          <EmptyState
            title="No rows match the active filters"
            description="Clear the category or search filter above the grid, or one of the filters in the top bar, to restore the allocation grid."
            stretch
          />
        ) : null}

        {selectedPrime &&
        !errorMessage &&
        (isLoading || visibleAllocations.length > 0) ? (
          <div className={tableHeaderTypographyClassName}>
            <DataTable
              table={table}
              isLoading={isLoading}
              onRowClick={(allocation) =>
                onSelectAllocation(getAllocationKey(allocation))
              }
              getRowKey={getAllocationKey}
              selectedRowKey={selectedAllocationKey}
              density="compact"
              // Six nowrap columns push min-content well past this, so it binds
              // only on the loading skeleton, which has no intrinsic width.
              minWidth="48rem"
              // No `firstColumnTall`: the identity hint owns that cell's height.
              skeletonConfig={{ rows: 8, columnHints: skeletonColumnHints }}
            />
          </div>
        ) : null}
      </div>
    </PageShell>
  );
}
