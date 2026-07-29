import {
  buildRowSearchString,
  matchesSearchQuery,
  SidebarLayout,
  type SortingState,
} from '@archon-research/design-system';
import { useEffect, useMemo, useRef, useState } from 'react';

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
// DEFAULT_RANGE_PRESET / defaultTimeRange come from the local shared barrel so
// the temporary 24h override in components/shared/index.ts applies here too;
// see that file for context.
import {
  ChainLogo,
  DEFAULT_RANGE_PRESET,
  defaultTimeRange,
  isRangePreset,
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
  getAllocations,
  getChains,
  getDataSources,
  getLatestPrimeDebtSnapshot,
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
  parseNumericValue,
  wadToUnits,
} from './lib/dashboard';
import { isAbortError, toErrorMessage } from './lib/errors';
import { logging } from './lib/logging';
import {
  PARAMS,
  setPathname as replacePathname,
  usePathname,
  useUrlParam,
} from './lib/url-params';
import type {
  Allocation,
  DataSource,
  Prime,
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
  const [isRiskCapitalLoading, setIsRiskCapitalLoading] = useState(false);
  const [riskCapitalErrorMessage, setRiskCapitalErrorMessage] = useState<
    string | null
  >(null);
  const [, setDataSources] = useState<DataSource[]>([]);
  const [localChains, setLocalChains] = useState<LocalChainRow[]>([]);
  const [localProtocols, setLocalProtocols] = useState<LocalProtocolRow[]>([]);
  const [riskCapital, setRiskCapital] = useState<PrimeRiskCapital | null>(null);
  const [primeDebtSnapshot, setPrimeDebtSnapshot] =
    useState<PrimeDebtSnapshot | null>(null);
  const [isPrimeDebtLoading, setIsPrimeDebtLoading] = useState(false);
  const [primeDebtErrorMessage, setPrimeDebtErrorMessage] = useState<
    string | null
  >(null);
  const [selectedAllocationKey, setSelectedAllocationKey] = useState<
    string | null
  >(null);
  const [isDrawerOpenParam, setIsDrawerOpenParam] = useUrlParam(
    PARAMS.drawerOpen,
  );
  const [selectedPrimeId, setSelectedPrimeId] = useUrlParam(PARAMS.prime);
  const [selectedNetwork, setSelectedNetwork] = useUrlParam(PARAMS.network);
  const [selectedProtocol, setSelectedProtocol] = useUrlParam(PARAMS.protocol);
  const [activityTokenParam, setActivityTokenParam] = useUrlParam(PARAMS.token);
  const [activityActionParam, setActivityActionParam] = useUrlParam(
    PARAMS.activityAction,
  );
  const [showAllPrimesParam, setShowAllPrimesParam] = useUrlParam(
    PARAMS.showAllPrimes,
  );
  const [pathname, setPathname] = usePathname();
  const { globalFilter, setGlobalFilter, setSorting, sorting } =
    useUrlSyncedTableState(PARAMS.sort, PARAMS.search);
  const [tokenSymbolOptions, setTokenSymbolOptions] = useState<string[]>([]);

  // Range selection persisted in the URL so it survives reloads and is
  // shareable: a preset key, plus from/to timestamps for custom ranges.
  const [rangeParam, setRangeParam] = useUrlParam(PARAMS.range);
  const [rangeFromParam, setRangeFromParam] = useUrlParam(PARAMS.rangeFrom);
  const [rangeToParam, setRangeToParam] = useUrlParam(PARAMS.rangeTo);

  const rangePreset: RangePreset = isRangePreset(rangeParam)
    ? rangeParam
    : DEFAULT_RANGE_PRESET;

  const timeRange = useMemo<TimeRange>(() => {
    if (rangePreset === 'custom') {
      // A hand-edited URL can carry unparsable or reversed custom timestamps;
      // fall back to the default rather than sending a bad range downstream.
      if (rangeFromParam && rangeToParam) {
        const fromMs = new Date(rangeFromParam).getTime();
        const toMs = new Date(rangeToParam).getTime();
        if (Number.isFinite(fromMs) && Number.isFinite(toMs) && toMs > fromMs) {
          return { from_timestamp: rangeFromParam, to_timestamp: rangeToParam };
        }
      }
      return defaultTimeRange();
    }
    return presetToRange(rangePreset);
  }, [rangePreset, rangeFromParam, rangeToParam]);

  const handleRangeChange = (preset: RangePreset, range: TimeRange) => {
    // The default preset stays out of the URL to keep it clean.
    setRangeParam(preset === DEFAULT_RANGE_PRESET ? null : preset);
    if (preset === 'custom') {
      setRangeFromParam(range.from_timestamp ?? null);
      setRangeToParam(range.to_timestamp ?? null);
    } else {
      setRangeFromParam(null);
      setRangeToParam(null);
    }
  };

  const previousPrimeIdRef = useRef<string | null>(selectedPrimeId);
  const isDrawerOpen = isDrawerOpenParam === '1';
  // Trim trailing slashes so "/activities/" links resolve the same as
  // "/activities" on hosts that append one.
  const normalizedPathname = pathname.replace(/\/+$/, '') || '/';
  const selectedView: 'allocation' | 'activities' =
    normalizedPathname === '/activities' ? 'activities' : 'allocation';
  const showAllPrimesInActivities =
    selectedView === 'activities' ? showAllPrimesParam !== '0' : false;

  useEffect(() => {
    if (
      normalizedPathname === '/allocation' ||
      normalizedPathname === '/activities'
    ) {
      return;
    }

    // Redirect unknown paths (e.g. "/") to the default view, preserving query
    // params. `replace` so the bare path never lands in the back-history.
    replacePathname('/allocation', 'replace');
  }, [normalizedPathname]);

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

  useEffect(() => {
    if (isPrimesLoading) {
      return;
    }

    if (primes.length === 0) {
      if (selectedPrimeId !== null) {
        setSelectedPrimeId(null);
      }
      return;
    }

    if (
      !selectedPrimeId ||
      !primes.some((prime) => prime.id === selectedPrimeId)
    ) {
      setSelectedPrimeId(primes[0]?.id ?? null);
    }
  }, [isPrimesLoading, selectedPrimeId, setSelectedPrimeId, primes]);

  useEffect(() => {
    if (
      previousPrimeIdRef.current !== null &&
      previousPrimeIdRef.current !== selectedPrimeId
    ) {
      setSelectedNetwork(null);
      setSelectedProtocol(null);
      setSelectedAllocationKey(null);
      setIsDrawerOpenParam(null);
    }

    previousPrimeIdRef.current = selectedPrimeId;
  }, [
    selectedPrimeId,
    setIsDrawerOpenParam,
    setSelectedNetwork,
    setSelectedProtocol,
  ]);

  useEffect(() => {
    if (!selectedPrimeId) {
      setAllocations([]);
      setAllocationsErrorMessage(null);
      setIsAllocationsLoading(false);
      return;
    }

    const controller = new AbortController();

    setAllocations([]);
    setSelectedAllocationKey(null);
    setIsAllocationsLoading(true);
    setAllocationsErrorMessage(null);

    void getAllocations(selectedPrimeId, controller.signal)
      .then((response) => {
        setAllocations(response);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.error('Failed to load allocations', {
          error,
          primeId: selectedPrimeId,
        });
        setAllocationsErrorMessage(toErrorMessage(error));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsAllocationsLoading(false);
        }
      });

    return () => controller.abort();
  }, [selectedPrimeId]);

  useEffect(() => {
    if (!selectedPrimeId) {
      setRiskCapital(null);
      setIsRiskCapitalLoading(false);
      setRiskCapitalErrorMessage(null);
      return;
    }

    const controller = new AbortController();

    setIsRiskCapitalLoading(true);
    setRiskCapital(null);
    setRiskCapitalErrorMessage(null);

    void getPrimeRiskCapital(selectedPrimeId, controller.signal)
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
          primeId: selectedPrimeId,
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
  }, [selectedPrimeId]);

  useEffect(() => {
    if (!selectedPrimeId) {
      setPrimeDebtSnapshot(null);
      setIsPrimeDebtLoading(false);
      setPrimeDebtErrorMessage(null);
      return;
    }

    const controller = new AbortController();

    setIsPrimeDebtLoading(true);
    setPrimeDebtSnapshot(null);
    setPrimeDebtErrorMessage(null);

    void getLatestPrimeDebtSnapshot(selectedPrimeId, controller.signal)
      .then((snapshot) => {
        if (!controller.signal.aborted) {
          setPrimeDebtSnapshot(snapshot);
        }
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.warn('Prime debt snapshot unavailable for selected prime', {
          error,
          primeId: selectedPrimeId,
        });
        setPrimeDebtSnapshot(null);
        setPrimeDebtErrorMessage(toErrorMessage(error));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsPrimeDebtLoading(false);
        }
      });

    return () => controller.abort();
  }, [selectedPrimeId]);

  const selectedPrime = useMemo(
    () => primes.find((prime) => prime.id === selectedPrimeId) ?? null,
    [selectedPrimeId, primes],
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
    selectedPrimeId,
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

  // Drop a stale filter only once its option source is ready — otherwise a
  // valid deep link (e.g. ?network=/?protocol=) is cleared on first render
  // before chains/protocols metadata has loaded.
  const networkOptionsLoading = isActivitiesView
    ? localChains.length === 0
    : isAllocationsLoading;
  const protocolOptionsLoading = isActivitiesView
    ? localProtocols.length === 0
    : isAllocationsLoading;

  useEffect(() => {
    if (networkOptionsLoading || !selectedNetwork) {
      return;
    }

    if (!networkOptions.some((option) => option.value === selectedNetwork)) {
      setSelectedNetwork(null);
    }
  }, [
    networkOptionsLoading,
    networkOptions,
    selectedNetwork,
    setSelectedNetwork,
  ]);

  useEffect(() => {
    if (protocolOptionsLoading || !selectedProtocol) {
      return;
    }

    if (!protocolOptions.some((option) => option.value === selectedProtocol)) {
      setSelectedProtocol(null);
    }
  }, [
    protocolOptionsLoading,
    protocolOptions,
    selectedProtocol,
    setSelectedProtocol,
  ]);

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
  const allocationBalanceSeries = useMemo<ChartDatum[]>(() => {
    if (rangePreset === 'custom' || activityBuckets.length === 0) {
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
  }, [activityBuckets, primeTotalAllocationUsd, rangePreset]);

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
      riskCapital?.exposure_usd === undefined ||
      riskCapital?.exposure_usd === null
        ? null
        : parseNumericValue(riskCapital.exposure_usd);

    const totalRiskCapitalValue =
      riskCapital?.total_risk_capital_usd === undefined ||
      riskCapital?.total_risk_capital_usd === null
        ? null
        : parseNumericValue(riskCapital.total_risk_capital_usd);

    const primeDebtValue = wadToUnits(primeDebtSnapshot?.debt_wad);

    const charts: MetricChartSpec[] = [
      {
        // Balance reconstructed from signed USD net flows, anchored at the
        // current total. When no activity history is available the card shows
        // an empty state rather than a flat current-value line.
        key: 'allocation-activity-volume',
        data: allocationBalanceSeries,
        kind: 'series',
        stroke: 'var(--colors-chart-series-primary, #60a5fa)',
        formatValue: formatCompactUsd,
      },
      {
        // Exposure trend from priced receipt-token balances over time; falls
        // back to the flat current value when no history is available.
        key: 'risk-capital',
        ...seriesOrFallback(exposureSeries, exposureValue),
        stroke: 'var(--colors-chart-series-secondary, #14b8a6)',
        formatValue: formatCompactUsd,
      },
      {
        key: 'total-capital',
        ...seriesOrFallback(totalCapitalSeries, totalRiskCapitalValue),
        stroke: 'var(--colors-chart-series-primary, #f59e0b)',
        formatValue: formatCompactUsd,
      },
      {
        key: 'prime-debt-exposure',
        ...seriesOrFallback(primeDebtSeries, primeDebtValue),
        stroke: '#f97316',
        formatValue: (value: number) => `${formatCompactNumber(value)} DAI`,
      },
    ];
    return charts.filter((chart) => chart.data.length > 0);
  }, [
    allocationBalanceSeries,
    riskCapital?.exposure_usd,
    riskCapital?.total_risk_capital_usd,
    chartFromLabel,
    chartToLabel,
    exposureSeries,
    primeDebtSeries,
    primeDebtSnapshot?.debt_wad,
    totalCapitalSeries,
  ]);

  useEffect(() => {
    if (filteredAllocations.length === 0) {
      if (selectedAllocationKey !== null) {
        setSelectedAllocationKey(null);
      }
      if (isDrawerOpen && !isAllocationsLoading) {
        setIsDrawerOpenParam(null);
      }
      return;
    }

    if (
      !selectedAllocationKey ||
      !filteredAllocations.some(
        (allocation) => getAllocationKey(allocation) === selectedAllocationKey,
      )
    ) {
      setSelectedAllocationKey(getAllocationKey(filteredAllocations[0]));
    }
  }, [
    filteredAllocations,
    isAllocationsLoading,
    isDrawerOpen,
    selectedAllocationKey,
    setIsDrawerOpenParam,
  ]);

  useEffect(() => {
    if (!isDrawerOpen) {
      return;
    }

    if (selectedAllocationKey === null) {
      return;
    }

    if (
      !filteredAllocations.some(
        (allocation) => getAllocationKey(allocation) === selectedAllocationKey,
      )
    ) {
      setIsDrawerOpenParam(null);
    }
  }, [
    filteredAllocations,
    isDrawerOpen,
    selectedAllocationKey,
    setIsDrawerOpenParam,
  ]);

  const selectedAllocation = useMemo(
    () =>
      filteredAllocations.find(
        (allocation) => getAllocationKey(allocation) === selectedAllocationKey,
      ) ?? null,
    [filteredAllocations, selectedAllocationKey],
  );

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
        '& [data-sidebar-layout] [data-part="panel"]:last-of-type > div': {
          overflow: 'auto !important',
          minHeight: '0 !important',
        },
        '& [data-sidebar-layout] [data-scope="resize-handle"][data-part="root"][data-axis="vertical"]':
          {
            right: '0 !important',
          },
        '& [data-sidebar-layout] [data-scope="resize-handle"][data-part="indicator"]':
          {
            opacity: 0,
          },
        '@media screen and (max-width: 48rem)': {
          '& [data-sidebar-layout] > div': {
            display: 'block !important',
            height: 'auto !important',
            overflow: 'visible !important',
          },
          '& [data-sidebar-layout] aside': {
            width: '100% !important',
            height: 'auto !important',
            maxHeight: '22rem',
            borderRight: 'none !important',
            borderBottom: '1px solid var(--colors-border-subtle)',
          },
          '& [data-sidebar-layout] main': {
            width: '100% !important',
            height: 'auto !important',
            minHeight: '0 !important',
          },
          '& [data-sidebar-layout] main > header': {
            minHeight: '0 !important',
            justifyContent: 'stretch !important',
          },
          '& [data-sidebar-layout] [data-scope="resize-handle"][data-part="root"]':
            {
              display: 'none !important',
            },
        },
      })}
    >
      <div data-sidebar-layout>
        <SidebarLayout
          sidebar={
            <PrimeSidebar
              primes={primes}
              selectedPrimeId={selectedPrimeId}
              isLoading={isPrimesLoading}
              errorMessage={primesErrorMessage}
              onSelectPrime={setSelectedPrimeId}
              showAllPrimes={showAllPrimesInActivities}
              canShowAllPrimes={selectedView === 'activities'}
              onShowAllPrimesChange={(value) =>
                setShowAllPrimesParam(value ? '1' : '0')
              }
            />
          }
          topBar={
            <TopBar
              hasSelectedPrime={selectedPrime !== null}
              networkOptions={networkOptions}
              onNetworkChange={setSelectedNetwork}
              onProtocolChange={setSelectedProtocol}
              protocolOptions={protocolOptions}
              selectedNetwork={selectedNetwork}
              selectedProtocol={selectedProtocol}
              selectedView={selectedView}
              onViewChange={(view) =>
                setPathname(
                  view === 'activities' ? '/activities' : '/allocation',
                )
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
                  setSelectedAllocationKey(allocationKey);
                  setIsDrawerOpenParam('1');
                }}
                primeDebtSnapshot={primeDebtSnapshot}
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
                tokenFilter={activityTokenParam}
                onTokenFilterChange={setActivityTokenParam}
                actionFilter={activityActionParam ?? undefined}
                onActionFilterChange={setActivityActionParam}
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
        onClose={() => setIsDrawerOpenParam(null)}
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
