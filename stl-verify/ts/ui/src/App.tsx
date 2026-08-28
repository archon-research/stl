import type { ChartColorToken } from '@archon-research/charting';
import {
  buildRowSearchString,
  matchesSearchQuery,
  type SortingState,
} from '@archon-research/design-system';
import { toSearchOption } from '@archon-research/router-kit';
import {
  useQueries,
  useQuery,
  type UseQueryResult,
} from '@tanstack/react-query';
import {
  useMatchRoute,
  useNavigate,
  useParams,
  useRouter,
  useSearch,
} from '@tanstack/react-router';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { css } from '#styled-system/css';

import { AllocationGrid } from './components/allocations/AllocationGrid';
import { BottomPanel } from './components/allocations/BottomPanel';
import type {
  ChartDatum,
  MetricChartSpec,
} from './components/allocations/metricCards';
import { RiskDetailDrawer } from './components/allocations/RiskDetailDrawer';
import { ActivityFeed } from './components/allocations/tabs/ActivityFeed';
// DEFAULT_RANGE_PRESET comes from the local shared barrel so the temporary 24h
// override in components/shared/index.ts applies here too; see that file.
import {
  ChainLogo,
  DEFAULT_RANGE_PRESET,
  presetToRange,
  ProtocolLogo,
  type RangePreset,
  type TimeRange,
  TokenLogo,
} from './components/shared';
import { CollapsibleSidebarLayout } from './components/shared/CollapsibleSidebarLayout';
import { PrimeSidebar } from './components/shared/PrimeSidebar';
import { TopBar } from './components/shared/TopBar';
import { useUrlSyncedTableState } from './data-table/hooks';
import { usePrimeChartData } from './hooks/usePrimeChartData';
import { useProvenanceAvailability } from './hooks/useProvenanceAvailability';
import {
  allocationNetworkKey,
  buildChainLabelLookup,
  buildNetworkOptions,
  buildNetworkOptionsFromMetadata,
  buildProtocolOptions,
  buildProtocolOptionsFromMetadata,
  DIRECT_PROTOCOL_FILTER_VALUE,
  ENCUMBRANCE_AT_RISK_THRESHOLD,
  ENCUMBRANCE_HIGH_SEVERITY_THRESHOLD,
  ENCUMBRANCE_LOW_SEVERITY_THRESHOLD,
  encumbranceSeverity,
  formatChartTimestampLabel,
  formatCompactNumber,
  formatCompactUsd,
  formatRatioPercent,
  formatTokenAmount,
  formatUsdValue,
  findPrimeGroup,
  getAllocationKey,
  getChainLabel,
  getProtocolLabel,
  groupPrimesByVault,
  parseNumericValue,
  toChartSeries,
  truncateMiddle,
  wadToUnits,
} from './lib/dashboard';
import { toQueryErrorMessage } from './lib/errors';
import { logging } from './lib/logging';
import {
  narrowAllocations,
  narrowRiskCapital,
  preferReference,
  showsReference,
  useProvenanceView,
} from './lib/provenance';
import {
  allocationsQuery,
  chainsQuery,
  latestDebtSnapshotQuery,
  latestReferenceDebtQuery,
  primesQuery,
  protocolsQuery,
  riskCapitalQuery,
  tokenSymbolsQuery,
} from './lib/queries';
import { ACTIVITY_ACTIONS, type AppSearchPatch } from './router/search-params';
import type {
  Allocation,
  Prime,
  TimeSeriesResolution,
} from './types/allocation';
import type { LocalChainRow, LocalProtocolRow } from './types/local-data';

// Picks the chart's downsampling resolution for a range. This is deliberately
// NOT the server's window-to-resolution policy (`time_series.minimum_resolution`),
// which is only a *floor* — the finest resolution the backend will allow for a
// window. This instead picks a *display* resolution that (1) is always at least
// as coarse as that floor (so the request never 422s) and (2) keeps the bucket
// count under the 500 per-prime page cap. Letting the server default would pick
// its floor and silently truncate long ranges (365d at the PT6H floor is ~1460
// buckets, well over 500). Each value below must stay >= the server floor for
// its window; if the server's policy tightens, these must be revisited.
function getResolutionForRange(
  preset: RangePreset,
  range: TimeRange,
): TimeSeriesResolution {
  const presetMap: Record<
    Exclude<RangePreset, 'custom'>,
    TimeSeriesResolution
  > = {
    '1h': 'PT1M',
    '6h': 'PT5M',
    '24h': 'PT15M',
    '7d': 'PT1H',
    '30d': 'PT6H',
    '90d': 'P1D',
    '180d': 'P1D',
    '365d': 'P1D',
  };

  if (preset !== 'custom') {
    return presetMap[preset];
  }

  const fromMs = range.from_timestamp
    ? new Date(range.from_timestamp).getTime()
    : Number.NaN;
  const toMs = range.to_timestamp
    ? new Date(range.to_timestamp).getTime()
    : Number.NaN;

  if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || toMs <= fromMs) {
    return 'PT15M';
  }

  const durationMs = toMs - fromMs;

  if (durationMs <= 2 * 60 * 60 * 1000) {
    return 'PT1M';
  }
  if (durationMs <= 12 * 60 * 60 * 1000) {
    return 'PT5M';
  }
  if (durationMs <= 48 * 60 * 60 * 1000) {
    return 'PT15M';
  }
  if (durationMs <= 14 * 24 * 60 * 60 * 1000) {
    return 'PT1H';
  }
  if (durationMs <= 60 * 24 * 60 * 60 * 1000) {
    return 'PT6H';
  }
  return 'P1D';
}

type ViewNavigation = {
  view: 'allocation' | 'activities';
  primeKey: string | null;
  patch?: AppSearchPatch;
  replace?: boolean;
};

// Everything scoped to the prime that was just left behind. Cleared as part of
// the navigation so the URL never advertises a filter from the previous prime.
const PRIME_SCOPED_RESET: AppSearchPatch = {
  network: undefined,
  protocol: undefined,
  category: undefined,
  // Both action filters: each view owns its own key, and either may be the one
  // the departing prime left behind.
  aa: undefined,
  daa: undefined,
  drawer: undefined,
  row: undefined,
};

// Shared fallbacks for a query that has not answered yet. A literal `?? []`
// would hand every `useMemo` below a fresh array on each render, which is the
// identity those memos compare on.
const NO_PRIMES: Prime[] = [];
const NO_CHAINS: LocalChainRow[] = [];
const NO_PROTOCOLS: LocalProtocolRow[] = [];
const NO_TOKEN_SYMBOLS: string[] = [];
const NO_PROXIES: string[] = [];
const NO_ALLOCATIONS: Allocation[] = [];

/**
 * Folds the per-proxy allocation queries into the one list the screen reads.
 *
 * A failure on any single proxy blanks the whole set rather than quietly
 * showing a prime that is missing a chain — the call the old `Promise.all`
 * made, kept. Declared at module scope because react-query only memoises a
 * combined result while the `combine` reference holds still.
 */
function combineAllocations(results: readonly UseQueryResult<Allocation[]>[]) {
  const failed = results.find((result) => result.error !== null);

  return {
    allocations: failed
      ? NO_ALLOCATIONS
      : results.flatMap((result) => result.data ?? NO_ALLOCATIONS),
    errorMessage: toQueryErrorMessage(failed?.error),
    isLoading: results.some((result) => result.isPending),
    // Whether the rows on screen are this prime's, settled. An empty list from
    // a query that has not answered would otherwise read as an answer.
    isLoaded: results.length > 0 && results.every((result) => result.isSuccess),
  };
}

function App() {
  // What is on screen, which is not always what was fetched: narrowing a
  // composite response changes this without a request.
  const { provenance: shownProvenance, showsReference: showsReferenceNow } =
    useProvenanceView();
  const primesResult = useQuery(primesQuery());
  const primes = primesResult.data ?? NO_PRIMES;
  const isPrimesLoading = primesResult.isPending;
  const primesErrorMessage = toQueryErrorMessage(primesResult.error);
  const localChains = useQuery(chainsQuery()).data ?? NO_CHAINS;
  const localProtocols = useQuery(protocolsQuery()).data ?? NO_PROTOCOLS;
  const tokenSymbolOptions =
    useQuery(tokenSymbolsQuery()).data ?? NO_TOKEN_SYMBOLS;
  // View-local on purpose: collapsing the prime list is a momentary "give me the
  // whole width" gesture, not a preference worth persisting across sessions.
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false);
  // Not derived: the URL is replaced with the fallback prime, so afterwards
  // nothing but this still names the prime that was asked for.
  const [unknownPrimeMessage, setUnknownPrimeMessage] = useState<string | null>(
    null,
  );
  const navigate = useNavigate();
  const matchRoute = useMatchRoute();
  const sharedSearch = useSearch({ from: '__root__' });
  const allocationSearch = useSearch({
    from: '/allocation',
    shouldThrow: false,
  });
  const activitiesSearch = useSearch({
    from: '/activities',
    shouldThrow: false,
  });
  const primePathParams = useParams({
    from: '/allocation/$primeId',
    shouldThrow: false,
  });
  const { globalFilter, setGlobalFilter, setSorting, sorting } =
    useUrlSyncedTableState();

  const selectedView: 'allocation' | 'activities' = matchRoute({
    to: '/activities',
  })
    ? 'activities'
    : 'allocation';
  const selectedPrimeId =
    primePathParams?.primeId ?? sharedSearch.prime ?? null;
  const selectedNetwork = sharedSearch.network ?? null;
  const selectedProtocol = sharedSearch.protocol ?? null;
  const showAllPrimesInActivities =
    selectedView === 'activities' ? activitiesSearch?.allp !== '0' : false;

  // Param edits replace rather than push: a filter belongs in the URL but not in
  // the back-history, where it would take a Back press each to undo.
  const updateSearch = useCallback(
    (patch: AppSearchPatch) => {
      void navigate({
        to: '.',
        search: (previous) => ({ ...previous, ...patch }),
        replace: true,
      });
    },
    [navigate],
  );

  // View and prime are both addresses, not params: the prime rides in the path on
  // the allocation view and in the query on activities.
  const navigateToView = useCallback(
    ({ view, primeKey, patch, replace }: ViewNavigation) => {
      if (view === 'activities') {
        void navigate({
          to: '/activities',
          search: (previous) => ({
            ...previous,
            ...patch,
            prime: primeKey ?? undefined,
          }),
          replace,
        });
        return;
      }

      if (primeKey === null) {
        void navigate({
          to: '/allocation',
          search: (previous) => ({ ...previous, ...patch, prime: undefined }),
          replace,
        });
        return;
      }

      void navigate({
        to: '/allocation/$primeId',
        params: { primeId: primeKey },
        search: (previous) => ({ ...previous, ...patch, prime: undefined }),
        replace,
      });
    },
    [navigate],
  );

  // A usable from/to pair in the URL is the custom selection itself; `range`
  // only ever names a preset (see the root search schema).
  const customTimeRange = useMemo<TimeRange | null>(
    () =>
      sharedSearch.from && sharedSearch.to
        ? { from_timestamp: sharedSearch.from, to_timestamp: sharedSearch.to }
        : null,
    [sharedSearch.from, sharedSearch.to],
  );

  const searchRangePreset = sharedSearch.range ?? DEFAULT_RANGE_PRESET;
  const rangePreset: RangePreset = customTimeRange
    ? 'custom'
    : searchRangePreset;

  const timeRange = useMemo<TimeRange>(
    () => customTimeRange ?? presetToRange(searchRangePreset),
    [customTimeRange, searchRangePreset],
  );

  const handleRangeChange = (preset: RangePreset, range: TimeRange) => {
    const customRange = preset === 'custom' ? range : null;
    updateSearch({
      // The default preset stays out of the URL to keep it clean, and a custom
      // range is carried by from/to alone.
      range:
        preset === 'custom' || preset === DEFAULT_RANGE_PRESET
          ? undefined
          : preset,
      from: customRange?.from_timestamp,
      to: customRange?.to_timestamp,
    });
  };

  // One entry per prime (grouped by prime_vault_address), not one per ALM
  // proxy — a prime allocates through several proxies (one per chain), and
  // the sidebar/selection model addresses the prime, not a single proxy.
  const primeGroups = useMemo(() => groupPrimesByVault(primes), [primes]);

  const selectedPrimeGroup = useMemo(
    () => primeGroups.find((group) => group.key === selectedPrimeId) ?? null,
    [primeGroups, selectedPrimeId],
  );

  // Resolving the default (first) prime preserves the rest of the URL: a deep
  // link that names filters but no prime must keep those filters.
  useEffect(() => {
    if (isPrimesLoading) {
      return;
    }

    const fallbackGroup = primeGroups[0] ?? null;

    if (fallbackGroup === null) {
      // A failed prime fetch is not an empty prime list: dropping the prime out
      // of the URL here would destroy the deep link a retry could still serve.
      if (primesErrorMessage === null && selectedPrimeId !== null) {
        navigateToView({ view: selectedView, primeKey: null, replace: true });
      }
      return;
    }

    if (!selectedPrimeId) {
      navigateToView({
        view: selectedView,
        primeKey: fallbackGroup.key,
        replace: true,
      });
      return;
    }

    const requestedGroup = findPrimeGroup(primeGroups, selectedPrimeId);

    if (requestedGroup?.key === selectedPrimeId) {
      return;
    }

    if (requestedGroup !== null) {
      // The same prime under one of its other addresses — an ALM proxy, or the
      // vault checksummed. Canonicalising the URL is not a prime swap, so it
      // keeps the link's filters and raises no notice: the reader gets the
      // prime they asked for, which is the one already on screen.
      navigateToView({
        view: selectedView,
        primeKey: requestedGroup.key,
        replace: true,
      });
      return;
    }

    // Silently swapping primes renders one prime's data under another's link,
    // and the filters in that link were scoped to the prime that is gone.
    logging.warn('Requested prime is not in the prime list', {
      requestedPrimeKey: selectedPrimeId,
      fallbackPrimeKey: fallbackGroup.key,
    });
    setUnknownPrimeMessage(
      `Prime ${truncateMiddle(selectedPrimeId)} was not found; showing ${fallbackGroup.name}.`,
    );
    navigateToView({
      view: selectedView,
      primeKey: fallbackGroup.key,
      patch: PRIME_SCOPED_RESET,
      replace: true,
    });
  }, [
    isPrimesLoading,
    navigateToView,
    primeGroups,
    primesErrorMessage,
    selectedPrimeId,
    selectedView,
  ]);

  // The prime's rows, gathered from its per-chain ALM proxies. One query each,
  // so a chain's rows cache on their own and returning to a prime is free.
  const allocationProxies = selectedPrimeGroup?.proxyAddresses ?? NO_PROXIES;

  // One call for anything the server answers prime-wide: reference rows are
  // prime-scoped, and the merged view resolves the prime's proxies itself.
  // Fanning either out would show each position once per chain — exactly the
  // double-count the `scope` field warns about.
  const queriedProxies = showsReference
    ? allocationProxies.slice(0, 1)
    : allocationProxies;

  const {
    allocations: fetchedAllocations,
    errorMessage: allocationsErrorMessage,
    isLoading: isAllocationsLoading,
    isLoaded: areAllocationsLoaded,
  } = useQueries({
    queries: queriedProxies.map((proxyAddress) =>
      allocationsQuery(proxyAddress),
    ),
    combine: combineAllocations,
  });

  // What was fetched, narrowed to what is being shown. A composite response
  // holds both provenances, so switching between them is this projection rather
  // than a request — and doing it here, once, is what keeps the table, the
  // cards, the charts and the drawer from disagreeing about which they show.
  const allocations = useMemo(
    () => narrowAllocations(shownProvenance, fetchedAllocations),
    [shownProvenance, fetchedAllocations],
  );

  // The prime_* fields on this response are aggregated prime-wide server-side,
  // so one call against the primary proxy carries the same figures every
  // other proxy of the prime would return; fanning it out would only waste
  // requests.
  const primaryProxyAddress = selectedPrimeGroup?.primaryProxyAddress ?? null;

  const router = useRouter();
  const provenanceAvailability = useProvenanceAvailability();

  // A provenance this prime cannot be served from is rewritten to one it can,
  // rather than left to fail request by request. A full document load, because
  // `lib/provenance` reads the value once per session on purpose: a client-side
  // switch would leave already-fetched series on the old provenance.
  const provenanceFallback = provenanceAvailability.fallbackFor(
    selectedPrimeGroup?.name,
  );
  const redirectedProvenance = useRef(false);

  useEffect(() => {
    if (provenanceFallback === null || redirectedProvenance.current) {
      return;
    }

    redirectedProvenance.current = true;
    const { href } = router.buildLocation({
      to: '.',
      search: (previous: Record<string, unknown>) => ({
        ...previous,
        reference: undefined,
        source: provenanceFallback === 'both' ? undefined : provenanceFallback,
      }),
    });
    globalThis.location.assign(href);
  }, [provenanceFallback, router]);

  // Both reads below are prime-scoped snapshots with no range of their own, so
  // `enabled` is the whole gate. The empty address only ever reaches the key of
  // a query that will not run.
  const isPrimeSelected = primaryProxyAddress !== null;
  const forPrime = primaryProxyAddress ?? '';

  const riskCapitalResult = useQuery({
    ...riskCapitalQuery(forPrime),
    enabled: isPrimeSelected,
  });
  const fetchedRiskCapital = riskCapitalResult.data ?? null;
  const isRiskCapitalLoading = isPrimeSelected && riskCapitalResult.isPending;
  const riskCapitalErrorMessage = toQueryErrorMessage(riskCapitalResult.error);

  const riskCapital = useMemo(
    () => narrowRiskCapital(shownProvenance, fetchedRiskCapital),
    [shownProvenance, fetchedRiskCapital],
  );

  // `showsReference` is fixed for the session, so exactly one of these ever
  // runs — but both hooks are called, which is what keeps the order stable.
  const referenceDebtResult = useQuery({
    ...latestReferenceDebtQuery(forPrime),
    enabled: isPrimeSelected && showsReference,
  });
  const debtSnapshotResult = useQuery({
    ...latestDebtSnapshotQuery(forPrime),
    enabled: isPrimeSelected && !showsReference,
  });

  const referenceDebt = referenceDebtResult.data ?? null;
  const primeDebtSnapshot = debtSnapshotResult.data ?? null;
  const primeDebtResult = showsReference
    ? referenceDebtResult
    : debtSnapshotResult;
  const isPrimeDebtLoading = isPrimeSelected && primeDebtResult.isPending;
  const primeDebtErrorMessage = toQueryErrorMessage(primeDebtResult.error);

  const selectedPrime = useMemo(
    () => primes.find((prime) => prime.address === primaryProxyAddress) ?? null,
    [primaryProxyAddress, primes],
  );

  const chartResolution = useMemo(
    () => getResolutionForRange(rangePreset, timeRange),
    [rangePreset, timeRange],
  );

  const {
    debtBuckets,
    activityBuckets,
    totalCapitalBuckets,
    exposureBuckets,
    isLoading: isChartsLoading,
    errorMessage: chartsErrorMessage,
  } = usePrimeChartData(
    // Any one of the prime's proxies: the activity and exposure endpoints
    // resolve it prime-wide server-side. Total-capital and debt read
    // prime-scoped rows, so one address answers for the whole prime there too.
    primaryProxyAddress,
    timeRange.from_timestamp,
    timeRange.to_timestamp,
    chartResolution,
  );

  const chainLabels = useMemo(
    () => buildChainLabelLookup(localChains),
    [localChains],
  );

  // Activities spans every prime, so its filter options come from the global
  // registries; allocations scope to the selected prime's holdings.
  const isActivitiesView = selectedView === 'activities';

  const networkOptions = useMemo(
    () =>
      isActivitiesView
        ? buildNetworkOptionsFromMetadata(localChains)
        : buildNetworkOptions(allocations, chainLabels),
    [allocations, chainLabels, isActivitiesView, localChains],
  );

  const protocolOptions = useMemo(
    () =>
      isActivitiesView
        ? buildProtocolOptionsFromMetadata(localProtocols)
        : buildProtocolOptions(allocations, localProtocols),
    [allocations, isActivitiesView, localProtocols],
  );

  const areAllocationsSettled = !isPrimesLoading && areAllocationsLoaded;

  // Only rows loaded for this exact prime are an authoritative option list; []
  // or another prime's rows read as "no such option" and wipe ?network=. The
  // rows are keyed by proxy, so a prime's own answer is the only one that can
  // be in hand for it.
  const allocationOptionsUnready = !areAllocationsLoaded;
  const networkOptionsLoading = isActivitiesView
    ? localChains.length === 0
    : allocationOptionsUnready;
  const protocolOptionsLoading = isActivitiesView
    ? localProtocols.length === 0
    : allocationOptionsUnready;

  useEffect(() => {
    if (networkOptionsLoading || !selectedNetwork) {
      return;
    }

    if (!networkOptions.some((option) => option.value === selectedNetwork)) {
      updateSearch({ network: undefined });
    }
  }, [networkOptionsLoading, networkOptions, selectedNetwork, updateSearch]);

  useEffect(() => {
    if (protocolOptionsLoading || !selectedProtocol) {
      return;
    }

    if (!protocolOptions.some((option) => option.value === selectedProtocol)) {
      updateSearch({ protocol: undefined });
    }
  }, [protocolOptionsLoading, protocolOptions, selectedProtocol, updateSearch]);

  const searchFilteredAllocations = useMemo(
    () =>
      allocations.filter((allocation) => {
        const matchesGlobalFilter = matchesSearchQuery(
          buildRowSearchString([
            allocation.symbol,
            allocation.underlying_symbol,
            allocation.protocol_name,
            getProtocolLabel(
              allocation.protocol_name,
              localProtocols,
              allocation.chain_id,
            ),
            getChainLabel(allocation.chain_id, chainLabels, allocation.network),
            allocation.receipt_token_address,
            allocation.underlying_token_address,
          ]),
          globalFilter,
        );

        return matchesGlobalFilter;
      }),
    [allocations, chainLabels, globalFilter, localProtocols],
  );

  const filteredAllocations = useMemo(
    () =>
      searchFilteredAllocations.filter((allocation) => {
        const matchesNetwork =
          selectedNetwork === null ||
          allocationNetworkKey(allocation) === selectedNetwork;
        const matchesProtocol =
          selectedProtocol === null ||
          (selectedProtocol === DIRECT_PROTOCOL_FILTER_VALUE
            ? allocation.protocol_name === null
            : allocation.protocol_name === selectedProtocol);

        return matchesNetwork && matchesProtocol;
      }),
    [searchFilteredAllocations, selectedNetwork, selectedProtocol],
  );

  // Anchor for the reconstructed balance series. Two bases must line up with
  // the flows driving the reconstruction:
  //   - Scope: activity buckets are fetched per-prime (no network/protocol/
  //     search filter), so the anchor is the whole-prime total; anchoring on a
  //     filtered subset while subtracting whole-prime flows would be wrong. The
  //     chart is therefore intentionally unaffected by the table filters.
  //   - Valuation: net_flow_usd values both receipt-token and direct-asset
  //     flows, so the anchor sums amount_usd across all allocations (receipt
  //     positions and direct holdings alike) rather than receipt positions only.
  const primeTotalAllocationUsd = useMemo(
    () =>
      allocations.reduce((sum, allocation) => {
        const numericAmount = parseNumericValue(allocation.amount_usd);
        return numericAmount === null ? sum : sum + numericAmount;
      }, 0),
    [allocations],
  );

  // Reconstruct the total-allocation balance over time: anchor at the current
  // whole-prime total and walk backwards, undoing each bucket's signed USD net
  // flow. The newest bucket therefore lands exactly on the current total.
  // Flow-based, so it captures deposits/withdrawals but not price moves;
  // clamped at 0 since a negative balance is meaningless.
  //
  // This is only valid when the window ends at "now" so the newest bucket truly
  // is the current total. Presets always end now; a custom range is a fixed
  // window whose end drifts into the past, so anchoring its newest (past) bucket
  // at the current total would misstate every point. Suppress it for custom
  // ranges until a range-end anchor is available.
  const allocationBalanceSeries = useMemo<ChartDatum[]>(() => {
    if (rangePreset === 'custom' || activityBuckets.length === 0) {
      return [];
    }

    // Walked newest-first because each point is the one after it less its own
    // net flow, then flipped back into the ascending order the charts assume.
    const newestFirst: ChartDatum[] = [];
    let balance = primeTotalAllocationUsd;
    for (const bucket of [...activityBuckets].reverse()) {
      newestFirst.push({
        label: formatChartTimestampLabel(bucket.bucket_start),
        value: Math.max(balance, 0),
        timestamp: Date.parse(bucket.bucket_start),
      });
      balance -= parseNumericValue(bucket.net_flow_usd) ?? 0;
    }
    return newestFirst.reverse();
  }, [activityBuckets, primeTotalAllocationUsd, rangePreset]);

  const primeDebtSeries = useMemo<ChartDatum[]>(
    () => toChartSeries(debtBuckets, (bucket) => wadToUnits(bucket.debt_wad)),
    [debtBuckets],
  );

  // Total capital is the on-chain SubProxy treasury balance over time.
  const totalCapitalSeries = useMemo<ChartDatum[]>(
    () =>
      toChartSeries(totalCapitalBuckets, (bucket) =>
        parseNumericValue(bucket.total_capital_usd),
      ),
    [totalCapitalBuckets],
  );

  // Both ride the total-capital buckets: assets_usd and encumbrance_ratio come
  // from the same two upstream feeds, so a separate request could pair figures
  // observed at different instants. Reference mode only — self mode reports
  // them null, which filters to an empty series and a flat fallback card.
  const collateralSeries = useMemo<ChartDatum[]>(
    () =>
      toChartSeries(totalCapitalBuckets, (bucket) =>
        parseNumericValue(bucket.assets_usd),
      ),
    [totalCapitalBuckets],
  );

  const encumbranceSeries = useMemo<ChartDatum[]>(
    () =>
      toChartSeries(totalCapitalBuckets, (bucket) =>
        parseNumericValue(bucket.encumbrance_ratio),
      ),
    [totalCapitalBuckets],
  );

  // When the reference collateral figure was observed, which is not the bucket
  // serving it: the feed is daily and the value is carried forward, so without
  // showing this a figure up to a day old is indistinguishable from a fresh one.
  const primeCollateralObservedAt = showsReferenceNow
    ? (totalCapitalBuckets
        .filter((bucket) => bucket.assets_observed_at != null)
        .at(-1)?.assets_observed_at ?? null)
    : null;

  // The monitor's three figures share one stamp because they share one row. It
  // matters for the same reason the collateral one does, and more so since the
  // prior seeding reaches up to 90 days back.
  const capitalObservedAt = showsReferenceNow
    ? (totalCapitalBuckets
        .filter((bucket) => bucket.capital_observed_at != null)
        .at(-1)?.capital_observed_at ?? null)
    : null;

  // Reference mode publishes a real total-assets figure. Self mode has no
  // equivalent — STL does not index PSM3 and prices no Curve LP position — so
  // it shows what STL actually holds records for, captioned as such.
  // Buckets are oldest-first, so the newest observation is the last point.
  const primeCollateralValue = showsReferenceNow
    ? (collateralSeries.at(-1)?.value ?? null)
    : primeTotalAllocationUsd;

  // Priced receipt-token exposure over time; drives the Exposure card trend
  // (falls back to the flat current value below when no history is available).
  const exposureSeries = useMemo<ChartDatum[]>(
    () =>
      toChartSeries(exposureBuckets, (bucket) =>
        parseNumericValue(bucket.exposure_usd),
      ),
    [exposureBuckets],
  );

  const chartFromLabel = timeRange.from_timestamp
    ? formatChartTimestampLabel(timeRange.from_timestamp)
    : 'Range start';

  const chartToLabel = timeRange.to_timestamp
    ? formatChartTimestampLabel(timeRange.to_timestamp)
    : 'Range end';

  const metricCharts = useMemo<MetricChartSpec[]>(() => {
    const fallbackChart = (value: number | null): ChartDatum[] => {
      if (value === null) {
        return [];
      }
      // No timestamps: these two points are the window's edges holding the
      // current value flat, not observations. Leaving them null keeps the card
      // out of the synced cursor, which is the honest outcome — there is no
      // history here to line up with a sibling's.
      return [
        { label: chartFromLabel, value, timestamp: null },
        { label: chartToLabel, value, timestamp: null },
      ];
    };

    // The real time series when present, else the flat current-value
    // placeholder, which the card renders identically.
    const seriesOrFallback = (
      series: ChartDatum[],
      currentValue: number | null,
    ): ChartDatum[] =>
      series.length > 0 ? series : fallbackChart(currentValue);

    // Sky's figures where it reports them: these are the flat line a card falls
    // back to, which must land on the same number the card's value shows.
    const exposureValue = parseNumericValue(
      preferReference(
        riskCapital?.reference_prime_exposure_usd,
        riskCapital?.prime_exposure_usd,
      ),
    );

    const requiredRiskCapitalValue = parseNumericValue(
      preferReference(
        riskCapital?.reference_prime_required_risk_capital_usd,
        riskCapital?.prime_required_risk_capital_usd,
      ),
    );

    const totalRiskCapitalValue = parseNumericValue(
      preferReference(
        riskCapital?.reference_total_risk_capital_usd,
        riskCapital?.total_risk_capital_usd,
      ),
    );

    // The same read the debt card's headline makes: a reference view holds its
    // snapshot in `referenceDebt`, so reading only the indexed snapshot left
    // the fallback null there — and with the series also empty, the whole
    // chart vanished under a headline that had a figure.
    const primeDebtValue = wadToUnits(
      showsReferenceNow ? referenceDebt?.debt_wad : primeDebtSnapshot?.debt_wad,
    );

    const encumbranceValue = parseNumericValue(
      preferReference(
        riskCapital?.reference_prime_encumbrance_ratio,
        riskCapital?.prime_encumbrance_ratio,
      ),
    );

    // The line wears the band the current ratio sits in, so a healthy chart
    // is not painted breach-red.
    const encumbranceStroke: ChartColorToken = {
      healthy: 'chart.series.positive' as const,
      'at-risk': 'chart.series.quaternary' as const,
      low: 'identity.8' as const,
      high: 'chart.series.critical' as const,
    }[encumbranceSeverity(encumbranceValue)];

    // Legacy's is the preferred model, so its series is the one drawn. Whole
    // series: a line traced from both would trace neither. Verify's is not
    // drawn beside it — a reader who wants that switches the view's provenance,
    // which keeps every card the same shape whichever provenance is on screen.
    const preferSkySeries = (
      stl: ChartDatum[],
      sky: ChartDatum[],
    ): ChartDatum[] => (sky.length > 0 ? sky : stl);

    const exposure = preferSkySeries(
      exposureSeries,
      toChartSeries(exposureBuckets, (bucket) =>
        parseNumericValue(bucket.reference_exposure_usd),
      ),
    );

    const totalCapital = preferSkySeries(
      totalCapitalSeries,
      toChartSeries(totalCapitalBuckets, (bucket) =>
        parseNumericValue(bucket.reference_total_capital_usd),
      ),
    );

    const primeDebt = preferSkySeries(
      primeDebtSeries,
      toChartSeries(debtBuckets, (bucket) =>
        wadToUnits(bucket.reference_debt_wad),
      ),
    );

    // One ordinal series token per card, named rather than written out as a
    // `var()` read: the token type is what catches a typo (and a repeat of the
    // collision where two of these cards named the same token unnoticed).
    //
    // The provenance not leading rides dashed beside the one that is, on the
    const charts: MetricChartSpec[] = [
      {
        // Balance reconstructed from signed USD net flows, anchored at the
        // current total. When no activity history is available the card shows
        // an empty state rather than a flat current-value line.
        key: 'allocation-activity-volume',
        data: allocationBalanceSeries,
        stroke: 'chart.series.primary',
        formatValue: formatCompactUsd,
      },
      {
        // Exposure trend from priced receipt-token balances over time; falls
        // back to the flat current value when no history is available.
        key: 'risk-capital',
        data: seriesOrFallback(exposure, exposureValue),
        stroke: 'chart.series.secondary',
        formatValue: formatCompactUsd,
      },
      {
        key: 'total-capital',
        data: seriesOrFallback(totalCapital, totalRiskCapitalValue),
        stroke: 'chart.series.quaternary',
        formatValue: formatCompactUsd,
        // The requirement the caption states, drawn as one reference line —
        // no endpoint serves the requirement over time.
        thresholds:
          requiredRiskCapitalValue === null
            ? undefined
            : [
                {
                  value: requiredRiskCapitalValue,
                  // Named only. The figure is on the axis the line sits
                  // against, in the caption above, and in the cursor tooltip
                  // at full precision — repeating a rounded copy on the plot
                  // read as a fourth, slightly different number.
                  label: 'Required',
                  // Reported at the cursor too: the total is read directly
                  // against this line, so the two figures belong side by side.
                  showInTooltip: true,
                  // Muted, matching the encumbrance card's own early-warning
                  // line. A coloured limit competed with the series for the
                  // eye and read as a second quantity rather than a bound.
                  stroke: 'var(--colors-text-muted)',
                },
              ],
      },
      {
        key: 'prime-debt-exposure',
        data: seriesOrFallback(primeDebt, primeDebtValue),
        stroke: 'chart.series.quinary',
        formatValue: (value: number) => `${formatCompactNumber(value)} DAI`,
      },
      {
        key: 'prime-collateral',
        data: seriesOrFallback(collateralSeries, primeCollateralValue),
        stroke: 'chart.series.tertiary',
        formatValue: formatCompactUsd,
      },
      {
        key: 'encumbrance-ratio',
        data: seriesOrFallback(encumbranceSeries, encumbranceValue),
        stroke: encumbranceStroke,
        formatValue: formatRatioPercent,
        // Ascending, and all three bands the severity scale reads: the 80%
        // edge is STL's own early warning rather than an Atlas level, so it is
        // drawn in the muted hue the other two are deliberately not.
        thresholds: [
          {
            value: ENCUMBRANCE_AT_RISK_THRESHOLD,
            label: formatRatioPercent(ENCUMBRANCE_AT_RISK_THRESHOLD, 0),
            stroke: 'var(--colors-text-muted)',
          },
          {
            value: ENCUMBRANCE_LOW_SEVERITY_THRESHOLD,
            label: formatRatioPercent(ENCUMBRANCE_LOW_SEVERITY_THRESHOLD, 0),
            stroke: 'var(--colors-text-warning)',
          },
          {
            value: ENCUMBRANCE_HIGH_SEVERITY_THRESHOLD,
            label: formatRatioPercent(ENCUMBRANCE_HIGH_SEVERITY_THRESHOLD, 0),
            stroke: 'var(--colors-text-critical)',
          },
        ],
      },
    ];
    return charts.filter((chart) => chart.data.length > 0);
  }, [
    allocationBalanceSeries,
    riskCapital?.prime_exposure_usd,
    riskCapital?.prime_encumbrance_ratio,
    riskCapital?.reference_prime_encumbrance_ratio,
    riskCapital?.reference_prime_exposure_usd,
    riskCapital?.reference_prime_required_risk_capital_usd,
    riskCapital?.prime_required_risk_capital_usd,
    riskCapital?.reference_total_risk_capital_usd,
    riskCapital?.total_risk_capital_usd,
    chartFromLabel,
    chartToLabel,
    debtBuckets,
    exposureBuckets,
    exposureSeries,
    primeDebtSeries,
    primeDebtSnapshot?.debt_wad,
    referenceDebt?.debt_wad,
    showsReferenceNow,
    totalCapitalBuckets,
    totalCapitalSeries,
    collateralSeries,
    encumbranceSeries,
    primeCollateralValue,
  ]);

  // `row` restores a drawer deep link; anything the current filters exclude falls
  // back to the first row in view, so a tab always has something to render.
  const selectedAllocation = useMemo(() => {
    const requestedRow = allocationSearch?.row;
    const requested = requestedRow
      ? filteredAllocations.find(
          (allocation) => getAllocationKey(allocation) === requestedRow,
        )
      : undefined;

    return requested ?? filteredAllocations[0] ?? null;
  }, [allocationSearch?.row, filteredAllocations]);

  const selectedAllocationKey = selectedAllocation
    ? getAllocationKey(selectedAllocation)
    : null;

  // Derived, never corrected: a deep link names its row before the allocations
  // are fetched, so `drawer=1` waits for a row instead of being dropped as stale.
  const isDrawerOpen =
    allocationSearch?.drawer === '1' && selectedAllocation !== null;

  const selectedProtocolLabel = selectedAllocation
    ? getProtocolLabel(
        selectedAllocation.protocol_name,
        localProtocols,
        selectedAllocation.chain_id,
      )
    : null;

  const selectedChainLabel = selectedAllocation
    ? getChainLabel(
        selectedAllocation.chain_id,
        chainLabels,
        selectedAllocation.network,
      )
    : null;

  return (
    <div
      className={css({
        position: 'relative',
        // Not a workaround: the sidebar splitter's 1px indicator line is
        // redundant next to the sidebar's own border, so hide it and let the
        // col-resize cursor carry the affordance.
        '& [data-sidebar-layout] [data-scope="resize-handle"][data-part="indicator"]':
          {
            opacity: 0,
          },
      })}
    >
      <div data-sidebar-layout>
        <CollapsibleSidebarLayout
          isSidebarCollapsed={isSidebarCollapsed}
          sidebar={
            <PrimeSidebar
              primeGroups={primeGroups}
              selectedPrimeId={selectedPrimeId}
              isLoading={isPrimesLoading}
              errorMessage={primesErrorMessage}
              onSelectPrime={(primeKey) => {
                setUnknownPrimeMessage(null);
                navigateToView({
                  view: selectedView,
                  primeKey,
                  patch: PRIME_SCOPED_RESET,
                  replace: true,
                });
              }}
              showAllPrimes={showAllPrimesInActivities}
              canShowAllPrimes={selectedView === 'activities'}
              onShowAllPrimesChange={(value) =>
                updateSearch({ allp: value ? '1' : '0' })
              }
            />
          }
          topBar={
            <TopBar
              isSidebarCollapsed={isSidebarCollapsed}
              onToggleSidebar={() =>
                setIsSidebarCollapsed((collapsed) => !collapsed)
              }
              availableProvenances={provenanceAvailability.forPrime(
                selectedPrimeGroup?.name,
              )}
              hasSelectedPrime={selectedPrime !== null}
              networkOptions={networkOptions}
              onNetworkChange={(value) =>
                updateSearch({ network: value ?? undefined })
              }
              onProtocolChange={(value) =>
                updateSearch({ protocol: value ?? undefined })
              }
              protocolOptions={protocolOptions}
              selectedNetwork={selectedNetwork}
              selectedProtocol={selectedProtocol}
              selectedView={selectedView}
              onViewChange={(view) =>
                navigateToView({ view, primeKey: selectedPrimeId })
              }
              rangePreset={rangePreset}
              timeRange={timeRange}
              onRangeChange={handleRangeChange}
            />
          }
          main={
            selectedView === 'allocation' ? (
              <AllocationGrid
                allocations={allocations}
                riskCapital={riskCapital}
                chainLabels={chainLabels}
                errorMessage={allocationsErrorMessage}
                filteredAllocations={filteredAllocations}
                topMetricsAllocations={searchFilteredAllocations}
                isLoading={isAllocationsLoading}
                areAllocationsSettled={areAllocationsSettled}
                isRiskCapitalLoading={isRiskCapitalLoading}
                isPrimeDebtLoading={isPrimeDebtLoading}
                localProtocols={localProtocols}
                onSelectAllocation={(allocationKey) => {
                  updateSearch({ row: allocationKey, drawer: '1' });
                }}
                primeDebtSnapshot={primeDebtSnapshot}
                referenceDebt={referenceDebt}
                onSearchChange={setGlobalFilter}
                onSortingChange={setSorting}
                searchValue={globalFilter}
                selectedAllocationKey={selectedAllocationKey}
                selectedPrime={selectedPrime}
                sorting={sorting as SortingState}
                metricCharts={metricCharts}
                isChartsLoading={isChartsLoading}
                chartsErrorMessage={chartsErrorMessage}
                riskCapitalErrorMessage={riskCapitalErrorMessage}
                primeDebtErrorMessage={primeDebtErrorMessage}
                noticeMessage={unknownPrimeMessage}
                primeCollateralUsd={primeCollateralValue}
                primeCollateralObservedAt={primeCollateralObservedAt}
                capitalObservedAt={capitalObservedAt}
              />
            ) : (
              <ActivityFeed
                isEnabled
                mode="page"
                chainLabels={chainLabels}
                selectedNetwork={selectedNetwork}
                selectedProtocol={selectedProtocol}
                showAllPrimes={showAllPrimesInActivities}
                selectedPrime={selectedPrime}
                tokenOptions={tokenSymbolOptions}
                tokenFilter={activitiesSearch?.token ?? null}
                onTokenFilterChange={(value) =>
                  updateSearch({ token: value ?? undefined })
                }
                actionFilter={activitiesSearch?.aa}
                onActionFilterChange={(value) =>
                  updateSearch({ aa: toSearchOption(value, ACTIVITY_ACTIONS) })
                }
                externalRangePreset={rangePreset}
                externalTimeRange={timeRange}
                onRangeChange={handleRangeChange}
              />
            )
          }
        />
      </div>

      <RiskDetailDrawer
        detail={
          selectedAllocation
            ? `${formatTokenAmount(selectedAllocation.balance)} ${selectedAllocation.symbol} · ${formatUsdValue(selectedAllocation.amount_usd ?? null)}`
            : undefined
        }
        isOpen={selectedView === 'allocation' && isDrawerOpen}
        onClose={() => updateSearch({ drawer: undefined })}
        subtitle={
          selectedAllocation ? (
            <span
              className={css({
                display: 'inline-flex',
                alignItems: 'center',
                gap: '1.5',
                flexWrap: 'wrap',
                rowGap: '1',
              })}
            >
              <span
                className={css({
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '1',
                  whiteSpace: 'nowrap',
                })}
              >
                <ProtocolLogo
                  protocolName={selectedProtocolLabel ?? 'Unknown'}
                  size="4"
                />
                {selectedProtocolLabel}
              </span>
              <span
                className={css({
                  color: 'text.muted',
                  fontSize: 'xs',
                })}
              >
                ·
              </span>
              <span
                className={css({
                  display: 'inline-flex',
                  alignItems: 'center',
                  gap: '1',
                  whiteSpace: 'nowrap',
                })}
              >
                <ChainLogo
                  chainId={selectedAllocation.chain_id}
                  label={selectedChainLabel ?? undefined}
                  size="4"
                />
                {selectedChainLabel}
              </span>
            </span>
          ) : undefined
        }
        title={
          selectedAllocation ? (
            <span
              className={css({
                display: 'inline-flex',
                alignItems: 'center',
                gap: '1.5',
                minWidth: 0,
              })}
            >
              <TokenLogo
                address={selectedAllocation.receipt_token_address}
                chainId={selectedAllocation.chain_id}
                size="7"
                symbol={selectedAllocation.symbol}
              />
              <span>{selectedAllocation.symbol}</span>
            </span>
          ) : (
            'Risk details'
          )
        }
      >
        <BottomPanel
          allocations={allocations}
          chainLabels={chainLabels}
          errorMessage={allocationsErrorMessage}
          isDrawerOpen={isDrawerOpen}
          isLoading={isAllocationsLoading}
          selectedAllocation={selectedAllocation}
          selectedPrime={selectedPrime}
          riskCapital={riskCapital}
        />
      </RiskDetailDrawer>
    </div>
  );
}

export default App;
