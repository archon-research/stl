import {
  buildRowSearchString,
  matchesSearchQuery,
  SidebarLayout,
  type SortingState,
} from '@archon-research/design-system';
import {
  useMatchRoute,
  useNavigate,
  useParams,
  useSearch,
} from '@tanstack/react-router';
import { useCallback, useEffect, useMemo, useState } from 'react';

import { css } from '#styled-system/css';

import type {
  ChartDatum,
  MetricChartKind,
} from './components/allocations/AllocationGrid';
import {
  AllocationGrid,
  type MetricChartSpec,
} from './components/allocations/AllocationGrid';
import { BottomPanel } from './components/allocations/BottomPanel';
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
import { PrimeSidebar } from './components/shared/PrimeSidebar';
import { TopBar } from './components/shared/TopBar';
import { useUrlSyncedTableState } from './data-table/hooks';
import { usePrimeChartData } from './hooks/usePrimeChartData';
import {
  getAllocationsForProxies,
  getChains,
  getDataSources,
  getLatestPrimeDebtSnapshot,
  getLatestReferenceDebtBucket,
  getPrimeRiskCapital,
  getPrimes,
  getProtocols,
  getTokens,
} from './lib/api';
import {
  buildChainLabelLookup,
  buildNetworkOptions,
  buildNetworkOptionsFromMetadata,
  buildProtocolOptions,
  buildProtocolOptionsFromMetadata,
  DIRECT_PROTOCOL_FILTER_VALUE,
  formatChartTimestampLabel,
  formatCompactNumber,
  formatCompactUsd,
  formatTokenAmount,
  formatUsdValue,
  getChainLabel,
  getAllocationKey,
  getProtocolLabel,
  groupPrimesByVault,
  parseNumericValue,
  truncateMiddle,
  wadToUnits,
} from './lib/dashboard';
import { isAbortError, toErrorMessage } from './lib/errors';
import { logging } from './lib/logging';
import { REFERENCE_MODE } from './lib/referenceMode';
import {
  ACTIVITY_ACTIONS,
  type AppSearchPatch,
  toSearchOption,
} from './router/search-params';
import type {
  Allocation,
  DataSource,
  Prime,
  PrimeDebtBucket,
  PrimeDebtSnapshot,
  PrimeRiskCapital,
  TimeSeriesResolution,
  TokensResponse,
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

function App() {
  const [primes, setPrimes] = useState<Prime[]>([]);
  const [primesErrorMessage, setPrimesErrorMessage] = useState<string | null>(
    null,
  );
  const [isPrimesLoading, setIsPrimesLoading] = useState(true);
  const [allocations, setAllocations] = useState<Allocation[]>([]);
  const [allocationsErrorMessage, setAllocationsErrorMessage] = useState<
    string | null
  >(null);
  const [isAllocationsLoading, setIsAllocationsLoading] = useState(false);
  // Which prime `allocations` holds rows for. Loading flags are set in an effect
  // and so cannot gate that same commit's later effects; this marker can.
  const [loadedAllocationsPrimeKey, setLoadedAllocationsPrimeKey] = useState<
    string | null
  >(null);
  const [isRiskCapitalLoading, setIsRiskCapitalLoading] = useState(false);
  const [riskCapitalErrorMessage, setRiskCapitalErrorMessage] = useState<
    string | null
  >(null);
  const [, setDataSources] = useState<DataSource[]>([]);
  const [localChains, setLocalChains] = useState<LocalChainRow[]>([]);
  const [localProtocols, setLocalProtocols] = useState<LocalProtocolRow[]>([]);
  const [riskCapital, setRiskCapital] = useState<PrimeRiskCapital | null>(null);
  const [referenceDebt, setReferenceDebt] = useState<PrimeDebtBucket | null>(
    null,
  );
  const [primeDebtSnapshot, setPrimeDebtSnapshot] =
    useState<PrimeDebtSnapshot | null>(null);
  const [isPrimeDebtLoading, setIsPrimeDebtLoading] = useState(false);
  const [primeDebtErrorMessage, setPrimeDebtErrorMessage] = useState<
    string | null
  >(null);
  const [tokenSymbolOptions, setTokenSymbolOptions] = useState<string[]>([]);
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

  useEffect(() => {
    const controller = new AbortController();

    void getDataSources(controller.signal)
      .then((response) => {
        setDataSources(response.sources ?? []);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.error('Failed to load provenance data sources', {
          error,
        });
        setDataSources([]);
      });

    return () => controller.abort();
  }, []);

  useEffect(() => {
    const controller = new AbortController();

    void getTokens({ limit: 500 }, controller.signal)
      .then((response: TokensResponse) => {
        const symbols = Array.from(
          new Set(
            response
              .map((token) => token.symbol?.trim().toUpperCase() ?? '')
              .filter((symbol) => symbol.length > 0),
          ),
        ).sort((a, b) => a.localeCompare(b));

        setTokenSymbolOptions(symbols);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.warn('Failed to load token options for activities view', {
          error,
        });
        setTokenSymbolOptions([]);
      });

    return () => controller.abort();
  }, []);

  useEffect(() => {
    const controller = new AbortController();

    void Promise.all([
      getChains(controller.signal),
      getProtocols(controller.signal),
    ])
      .then(([chains, protocols]) => {
        setLocalChains(chains);
        setLocalProtocols(protocols);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.error('Failed to load local metadata (chains/protocols)', {
          error,
        });
        setLocalChains([]);
        setLocalProtocols([]);
      });

    return () => controller.abort();
  }, []);

  useEffect(() => {
    const controller = new AbortController();

    setIsPrimesLoading(true);
    setPrimesErrorMessage(null);

    void getPrimes(controller.signal)
      .then((response) => {
        setPrimes(response);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.error('Failed to load primes', { error });
        setPrimesErrorMessage(toErrorMessage(error));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsPrimesLoading(false);
        }
      });

    return () => controller.abort();
  }, []);

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

    if (primeGroups.some((group) => group.key === selectedPrimeId)) {
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

  useEffect(() => {
    if (!selectedPrimeGroup) {
      setAllocations([]);
      setLoadedAllocationsPrimeKey(null);
      setAllocationsErrorMessage(null);
      setIsAllocationsLoading(false);
      return;
    }

    const controller = new AbortController();

    setAllocations([]);
    setLoadedAllocationsPrimeKey(null);
    setIsAllocationsLoading(true);
    setAllocationsErrorMessage(null);

    // Fans out across every ALM proxy of the prime and concatenates; a
    // failure on any one proxy rejects the whole call (see
    // getAllocationsForProxies) rather than silently dropping that chain's
    // positions.
    void getAllocationsForProxies(
      selectedPrimeGroup.proxyAddresses,
      controller.signal,
    )
      .then((response) => {
        setAllocations(response);
        setLoadedAllocationsPrimeKey(selectedPrimeGroup.key);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.error('Failed to load allocations', {
          error,
          primeKey: selectedPrimeGroup.key,
        });
        setAllocationsErrorMessage(toErrorMessage(error));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsAllocationsLoading(false);
        }
      });

    return () => controller.abort();
  }, [selectedPrimeGroup]);

  // The prime_* fields on this response are aggregated prime-wide server-side,
  // so one call against the primary proxy carries the same figures every
  // other proxy of the prime would return; fanning it out would only waste
  // requests.
  const primaryProxyAddress = selectedPrimeGroup?.primaryProxyAddress ?? null;

  // The bucketed chart series below (debt/exposure/total-capital/activity) are
  // fetched for the primary proxy only, not fanned out (see the
  // usePrimeChartData call). For a prime with more than one proxy, that makes
  // those series describe one chain while the headline figures they sit next
  // to are prime-wide — real history for one chain would silently look like
  // real history for the whole prime. Chart specs below suppress the series
  // in that case rather than render a trend line that contradicts its own
  // headline number.
  const isMultiChainPrime =
    (selectedPrimeGroup?.proxyAddresses.length ?? 0) > 1;

  useEffect(() => {
    if (!primaryProxyAddress) {
      setRiskCapital(null);
      setIsRiskCapitalLoading(false);
      setRiskCapitalErrorMessage(null);
      return;
    }

    const controller = new AbortController();

    setIsRiskCapitalLoading(true);
    setRiskCapital(null);
    setRiskCapitalErrorMessage(null);

    void getPrimeRiskCapital(primaryProxyAddress, controller.signal)
      .then((response) => {
        if (!controller.signal.aborted) {
          setRiskCapital(response);
        }
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.warn('Risk capital unavailable for selected prime', {
          error,
          primaryProxyAddress,
        });
        setRiskCapital(null);
        setRiskCapitalErrorMessage(toErrorMessage(error));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsRiskCapitalLoading(false);
        }
      });

    return () => controller.abort();
  }, [primaryProxyAddress]);

  useEffect(() => {
    if (!primaryProxyAddress) {
      setPrimeDebtSnapshot(null);
      setReferenceDebt(null);
      setIsPrimeDebtLoading(false);
      setPrimeDebtErrorMessage(null);
      return;
    }

    const controller = new AbortController();

    setIsPrimeDebtLoading(true);
    setPrimeDebtSnapshot(null);
    setReferenceDebt(null);
    setPrimeDebtErrorMessage(null);

    void (
      REFERENCE_MODE
        ? getLatestReferenceDebtBucket(primaryProxyAddress, controller.signal)
        : getLatestPrimeDebtSnapshot(primaryProxyAddress, controller.signal)
    )
      .then((latest) => {
        if (controller.signal.aborted) {
          return;
        }
        if (REFERENCE_MODE) {
          setReferenceDebt(latest as PrimeDebtBucket | null);
        } else {
          setPrimeDebtSnapshot(latest as PrimeDebtSnapshot | null);
        }
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.warn('Prime debt snapshot unavailable for selected prime', {
          error,
          primaryProxyAddress,
        });
        setPrimeDebtSnapshot(null);
        setReferenceDebt(null);
        setPrimeDebtErrorMessage(toErrorMessage(error));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsPrimeDebtLoading(false);
        }
      });

    return () => controller.abort();
  }, [primaryProxyAddress]);

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
    // These per-bucket time-series endpoints have no prime-wide aggregation
    // (unlike risk-capital's prime_* fields), so summing them across a
    // prime's proxies is not well-defined here; scope to the primary proxy
    // rather than fan out.
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

  // Only rows loaded for this exact prime are an authoritative option list; []
  // or another prime's rows read as "no such option" and wipe ?network=.
  const allocationOptionsUnready =
    selectedPrimeGroup === null ||
    loadedAllocationsPrimeKey !== selectedPrimeGroup.key;
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
            getChainLabel(allocation.chain_id, chainLabels),
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
          String(allocation.chain_id) === selectedNetwork;
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
  //
  // It is also only valid for a single-proxy prime: the anchor
  // (primeTotalAllocationUsd) is cross-chain, but activityBuckets' net flows
  // are fetched for the primary proxy only (see usePrimeChartData below), so
  // for a multi-chain prime every point except the newest would be silently
  // over- or under-stated by whatever flowed through the other chains.
  // Suppress it rather than reconstruct a history the inputs can't support.
  const allocationBalanceSeries = useMemo<ChartDatum[]>(() => {
    if (
      isMultiChainPrime ||
      rangePreset === 'custom' ||
      activityBuckets.length === 0
    ) {
      return [];
    }

    const series = new Array<ChartDatum>(activityBuckets.length);
    let balance = primeTotalAllocationUsd;
    for (let index = activityBuckets.length - 1; index >= 0; index -= 1) {
      const bucket = activityBuckets[index];
      series[index] = {
        label: formatChartTimestampLabel(bucket.bucket_start),
        value: Math.max(balance, 0),
      };
      balance -= parseNumericValue(bucket.net_flow_usd) ?? 0;
    }
    return series;
  }, [
    activityBuckets,
    isMultiChainPrime,
    primeTotalAllocationUsd,
    rangePreset,
  ]);

  const primeDebtSeries = useMemo<ChartDatum[]>(
    () =>
      debtBuckets
        .map((bucket) => ({
          label: formatChartTimestampLabel(bucket.bucket_start),
          value: wadToUnits(bucket.debt_wad) ?? Number.NaN,
        }))
        .filter((point) => Number.isFinite(point.value)),
    [debtBuckets],
  );

  // Total capital is the on-chain SubProxy treasury balance over time.
  const totalCapitalSeries = useMemo<ChartDatum[]>(
    () =>
      totalCapitalBuckets
        .map((bucket) => ({
          label: formatChartTimestampLabel(bucket.bucket_start),
          value: parseNumericValue(bucket.total_capital_usd) ?? Number.NaN,
        }))
        .filter((point) => Number.isFinite(point.value)),
    [totalCapitalBuckets],
  );

  // Priced receipt-token exposure over time; drives the Exposure card trend
  // (falls back to the flat current value below when no history is available).
  const exposureSeries = useMemo<ChartDatum[]>(
    () =>
      exposureBuckets
        .map((bucket) => ({
          label: formatChartTimestampLabel(bucket.bucket_start),
          value: parseNumericValue(bucket.exposure_usd) ?? Number.NaN,
        }))
        .filter((point) => Number.isFinite(point.value)),
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
      return [
        { label: chartFromLabel, value },
        { label: chartToLabel, value },
      ];
    };

    // Pick the real time series when present, else the flat current-value
    // placeholder — returning data and kind together so the two can never
    // disagree about whether the chart is real.
    const seriesOrFallback = (
      series: ChartDatum[],
      currentValue: number | null,
    ): { data: ChartDatum[]; kind: MetricChartKind } =>
      series.length > 0
        ? { data: series, kind: 'series' }
        : { data: fallbackChart(currentValue), kind: 'fallback' };

    const exposureValue =
      riskCapital?.prime_exposure_usd === undefined ||
      riskCapital?.prime_exposure_usd === null
        ? null
        : parseNumericValue(riskCapital.prime_exposure_usd);

    const totalRiskCapitalValue =
      riskCapital?.total_risk_capital_usd === undefined ||
      riskCapital?.total_risk_capital_usd === null
        ? null
        : parseNumericValue(riskCapital.total_risk_capital_usd);

    const primeDebtValue = wadToUnits(primeDebtSnapshot?.debt_wad);

    // One ordinal series token per card, and deliberately no `var(..., fallback)`:
    // a fallback lets a wrong or missing token render as a plausible colour, which
    // is how two of these cards came to name the same token unnoticed.
    const charts: MetricChartSpec[] = [
      {
        // Balance reconstructed from signed USD net flows, anchored at the
        // current total. When no activity history is available the card shows
        // an empty state rather than a flat current-value line.
        key: 'allocation-activity-volume',
        data: allocationBalanceSeries,
        kind: 'series',
        stroke: 'var(--colors-chart-series-primary)',
        formatValue: formatCompactUsd,
      },
      {
        // Exposure trend from priced receipt-token balances over time; falls
        // back to the flat current value when no history is available, and
        // also when the prime spans more than one proxy — exposureSeries is
        // fetched for the primary proxy only, while the headline number next
        // to it (prime_exposure_usd) is prime-wide, so a real per-chain
        // series here would contradict its own headline figure.
        key: 'risk-capital',
        ...seriesOrFallback(
          isMultiChainPrime ? [] : exposureSeries,
          exposureValue,
        ),
        stroke: 'var(--colors-chart-series-secondary)',
        formatValue: formatCompactUsd,
      },
      {
        key: 'total-capital',
        ...seriesOrFallback(totalCapitalSeries, totalRiskCapitalValue),
        stroke: 'var(--colors-chart-series-quaternary)',
        formatValue: formatCompactUsd,
      },
      {
        key: 'prime-debt-exposure',
        ...seriesOrFallback(primeDebtSeries, primeDebtValue),
        stroke: 'var(--colors-chart-series-quinary)',
        formatValue: (value: number) => `${formatCompactNumber(value)} DAI`,
      },
    ];
    return charts.filter((chart) => chart.data.length > 0);
  }, [
    allocationBalanceSeries,
    riskCapital?.prime_exposure_usd,
    riskCapital?.total_risk_capital_usd,
    chartFromLabel,
    chartToLabel,
    exposureSeries,
    isMultiChainPrime,
    primeDebtSeries,
    primeDebtSnapshot?.debt_wad,
    totalCapitalSeries,
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
    ? getChainLabel(selectedAllocation.chain_id, chainLabels)
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
        <SidebarLayout
          collapseBelow={768}
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
                isMultiChainPrime={isMultiChainPrime}
                noticeMessage={unknownPrimeMessage}
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
                isMultiChainPrime={isMultiChainPrime}
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
        />
      </RiskDetailDrawer>
    </div>
  );
}

export default App;
