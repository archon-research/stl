import {
  buildRowSearchString,
  matchesSearchQuery,
  SidebarLayout,
  type SortingState,
} from '@archon-research/design-system';
import { useEffect, useMemo, useRef, useState } from 'react';

import { css } from '#styled-system/css';

import type { ChartDatum } from './components/allocations/AllocationGrid';
import {
  AllocationGrid,
  type ChartResolution,
  type MetricChartSpec,
} from './components/allocations/AllocationGrid';
import { BottomPanel } from './components/allocations/BottomPanel';
import { RiskDetailDrawer } from './components/allocations/RiskDetailDrawer';
import { ActivityFeed } from './components/allocations/tabs/ActivityFeed';
import { ChainLogo, ProtocolLogo, TokenLogo } from './components/shared';
import { PrimeSidebar } from './components/shared/PrimeSidebar';
import {
  type RangePreset,
  type TimeRange,
} from './components/shared/RangePicker';
import { TopBar } from './components/shared/TopBar';
import { useUrlSyncedTableState } from './data-table/hooks';
import {
  getAllocationActivityEnvelope,
  getAllocations,
  getCapitalMetrics,
  getChains,
  getDataSources,
  getLatestPrimeDebtSnapshot,
  getPrimeDebtEnvelope,
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
  formatTokenAmount,
  formatUsdValue,
  getChainLabel,
  getAllocationKey,
  getProtocolLabel,
  parseNumericValue,
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
  AllocationActivityBucket,
  CapitalMetrics,
  DataSource,
  Prime,
  PrimeDebtBucket,
  PrimeDebtSnapshot,
  TokensResponse,
} from './types/allocation';
import type { LocalChainRow, LocalProtocolRow } from './types/local-data';

function getResolutionForRange(
  preset: RangePreset,
  range: TimeRange,
): ChartResolution {
  const presetMap: Record<Exclude<RangePreset, 'custom'>, ChartResolution> = {
    '1h': 'PT1M',
    '6h': 'PT5M',
    '24h': 'PT15M',
    '7d': 'PT1H',
    '30d': 'PT6H',
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

function toTimestampMs(timestamp: string): number {
  const value = new Date(timestamp).getTime();
  return Number.isFinite(value) ? value : 0;
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
  const [isCapitalMetricsLoading, setIsCapitalMetricsLoading] = useState(false);
  const [capitalMetricsErrorMessage, setCapitalMetricsErrorMessage] = useState<
    string | null
  >(null);
  const [, setDataSources] = useState<DataSource[]>([]);
  const [localChains, setLocalChains] = useState<LocalChainRow[]>([]);
  const [localProtocols, setLocalProtocols] = useState<LocalProtocolRow[]>([]);
  const [capitalMetricsList, setCapitalMetricsList] = useState<
    CapitalMetrics[]
  >([]);
  const [primeDebtSnapshot, setPrimeDebtSnapshot] =
    useState<PrimeDebtSnapshot | null>(null);
  const [isPrimeDebtLoading, setIsPrimeDebtLoading] = useState(false);
  const [primeDebtErrorMessage, setPrimeDebtErrorMessage] = useState<
    string | null
  >(null);
  const [activityBuckets, setActivityBuckets] = useState<
    AllocationActivityBucket[]
  >([]);
  const [debtBuckets, setDebtBuckets] = useState<PrimeDebtBucket[]>([]);
  const [isChartsLoading, setIsChartsLoading] = useState(false);
  const [chartsErrorMessage, setChartsErrorMessage] = useState<string | null>(
    null,
  );
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

  // Shared range state: drives ActivityFeed and TopBar RangePicker in sync.
  const [rangePreset, setRangePreset] = useState<RangePreset>('24h');
  const [timeRange, setTimeRange] = useState<TimeRange>({
    from_timestamp: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
    to_timestamp: new Date().toISOString(),
  });

  const handleRangeChange = (preset: RangePreset, range: TimeRange) => {
    setRangePreset(preset);
    setTimeRange(range);
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
    const controller = new AbortController();
    setIsCapitalMetricsLoading(true);
    setCapitalMetricsErrorMessage(null);

    void getCapitalMetrics(controller.signal)
      .then((metrics) => {
        setCapitalMetricsList(metrics);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.error('Failed to load capital metrics', {
          error,
        });
        setCapitalMetricsList([]);
        setCapitalMetricsErrorMessage(toErrorMessage(error));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsCapitalMetricsLoading(false);
        }
      });

    return () => controller.abort();
  }, []);

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

  const capitalMetrics = useMemo(() => {
    if (!selectedPrimeId) {
      return null;
    }

    return (
      capitalMetricsList.find(
        (metric) =>
          metric.prime_id.trim().toLowerCase() ===
          selectedPrimeId.trim().toLowerCase(),
      ) ?? null
    );
  }, [capitalMetricsList, selectedPrimeId]);

  const chartResolution = useMemo(
    () => getResolutionForRange(rangePreset, timeRange),
    [rangePreset, timeRange],
  );

  useEffect(() => {
    if (!selectedPrimeId) {
      setActivityBuckets([]);
      setDebtBuckets([]);
      setChartsErrorMessage(null);
      setIsChartsLoading(false);
      return;
    }

    const controller = new AbortController();
    setIsChartsLoading(true);
    setChartsErrorMessage(null);

    void Promise.all([
      getAllocationActivityEnvelope(
        {
          prime_id: selectedPrimeId,
          from_timestamp: timeRange.from_timestamp,
          to_timestamp: timeRange.to_timestamp,
          resolution: chartResolution,
          aggregate: true,
        },
        controller.signal,
      ),
      getPrimeDebtEnvelope(
        selectedPrimeId,
        {
          from_timestamp: timeRange.from_timestamp,
          to_timestamp: timeRange.to_timestamp,
          resolution: chartResolution,
          aggregate: true,
        },
        controller.signal,
      ),
    ])
      .then(([activityEnvelope, debtEnvelope]) => {
        const nextActivityBuckets =
          activityEnvelope.mode === 'aggregated'
            ? (activityEnvelope.data as AllocationActivityBucket[])
            : [];
        const nextDebtBuckets =
          debtEnvelope.mode === 'aggregated'
            ? (debtEnvelope.data as PrimeDebtBucket[])
            : [];

        setActivityBuckets(
          [...nextActivityBuckets].sort(
            (a, b) =>
              toTimestampMs(a.bucket_start) - toTimestampMs(b.bucket_start),
          ),
        );
        setDebtBuckets(
          [...nextDebtBuckets].sort(
            (a, b) =>
              toTimestampMs(a.bucket_start) - toTimestampMs(b.bucket_start),
          ),
        );
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.error('Failed to load chart buckets', {
          error,
          primeId: selectedPrimeId,
          chartResolution,
        });
        setActivityBuckets([]);
        setDebtBuckets([]);
        setChartsErrorMessage(toErrorMessage(error));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsChartsLoading(false);
        }
      });

    return () => controller.abort();
  }, [
    chartResolution,
    selectedPrimeId,
    timeRange.from_timestamp,
    timeRange.to_timestamp,
  ]);

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

  const allocationSummaryTotalUsd = useMemo(
    () =>
      searchFilteredAllocations.reduce((sum, allocation) => {
        const numericAmount = parseNumericValue(allocation.amount_usd);
        return numericAmount === null ? sum : sum + numericAmount;
      }, 0),
    [searchFilteredAllocations],
  );

  const activityVolumeSeries = useMemo<ChartDatum[]>(() => {
    let runningTotal = 0;

    return activityBuckets
      .map((bucket) => {
        const bucketAmount = parseNumericValue(bucket.total_tx_amount) ?? Number.NaN;
        if (!Number.isFinite(bucketAmount)) {
          return null;
        }

        runningTotal += bucketAmount;

        return {
          label: new Date(bucket.bucket_start).toLocaleString([], {
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
          }),
          value: runningTotal,
        };
      })
      .filter((point): point is ChartDatum => point !== null);
  }, [activityBuckets]);

  const primeDebtSeries = useMemo<ChartDatum[]>(
    () =>
      debtBuckets
        .map((bucket) => ({
          label: new Date(bucket.bucket_start).toLocaleString([], {
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
          }),
          value:
            bucket.debt_wad === null || bucket.debt_wad === undefined
              ? Number.NaN
              : (parseNumericValue(bucket.debt_wad) ?? Number.NaN) / 1e18,
        }))
        .filter((point) => Number.isFinite(point.value)),
    [debtBuckets],
  );

  const chartFromLabel = timeRange.from_timestamp
    ? new Date(timeRange.from_timestamp).toLocaleString([], {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
      })
    : 'Range start';

  const chartToLabel = timeRange.to_timestamp
    ? new Date(timeRange.to_timestamp).toLocaleString([], {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
      })
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

    const riskCapitalValue =
      capitalMetrics?.risk_capital === undefined ||
      capitalMetrics?.risk_capital === null
        ? null
        : parseNumericValue(capitalMetrics.risk_capital);

    const totalCapitalValue =
      capitalMetrics?.total_capital === undefined ||
      capitalMetrics?.total_capital === null
        ? null
        : parseNumericValue(capitalMetrics.total_capital);

    const primeDebtValue =
      primeDebtSnapshot?.debt_wad === undefined ||
      primeDebtSnapshot?.debt_wad === null
        ? null
        : (parseNumericValue(primeDebtSnapshot.debt_wad) ?? Number.NaN) / 1e18;

    return [
      {
        key: 'allocation-activity-volume',
        title: 'Allocation activity volume',
        subtitle: 'Cumulative tx amount from allocation activity',
        data:
          activityVolumeSeries.length > 0
            ? activityVolumeSeries
            : fallbackChart(allocationSummaryTotalUsd),
        stroke: 'var(--colors-chart-series-primary, #60a5fa)',
        fill: 'color-mix(in srgb, var(--colors-chart-area-primary, #60a5fa) 24%, transparent)',
        formatValue: (value: number) => formatUsdValue(value),
      },
      {
        key: 'risk-capital',
        title: 'Risk capital trend',
        subtitle: 'Current value repeated until historical series lands',
        data: fallbackChart(riskCapitalValue),
        stroke: 'var(--colors-chart-series-secondary, #14b8a6)',
        fill: 'color-mix(in srgb, var(--colors-chart-series-secondary, #14b8a6) 22%, transparent)',
        formatValue: (value: number) => formatUsdValue(value),
      },
      {
        key: 'total-capital',
        title: 'Total capital trend',
        subtitle: 'Current value repeated until historical series lands',
        data: fallbackChart(totalCapitalValue),
        stroke: 'var(--colors-chart-series-primary, #f59e0b)',
        fill: 'color-mix(in srgb, #f59e0b 20%, transparent)',
        formatValue: (value: number) => formatUsdValue(value),
      },
      {
        key: 'prime-debt-exposure',
        title: 'Prime debt exposure',
        subtitle: 'Aggregated debt buckets in debt units',
        data:
          primeDebtSeries.length > 0
            ? primeDebtSeries
            : fallbackChart(primeDebtValue),
        stroke: '#f97316',
        fill: 'color-mix(in srgb, #f97316 20%, transparent)',
        formatValue: (value: number) => `${value.toLocaleString()} DAI`,
      },
    ].filter((chart) => chart.data.length > 0);
  }, [
    activityVolumeSeries,
    allocationSummaryTotalUsd,
    capitalMetrics?.risk_capital,
    capitalMetrics?.total_capital,
    chartFromLabel,
    chartToLabel,
    primeDebtSeries,
    primeDebtSnapshot?.debt_wad,
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
                capitalMetrics={capitalMetrics}
                chainLabels={chainLabels}
                errorMessage={allocationsErrorMessage}
                filteredAllocations={filteredAllocations}
                topMetricsAllocations={searchFilteredAllocations}
                isLoading={isAllocationsLoading}
                isCapitalMetricsLoading={isCapitalMetricsLoading}
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
                capitalMetricsErrorMessage={capitalMetricsErrorMessage}
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
          localProtocols={localProtocols}
          selectedAllocation={selectedAllocation}
          selectedPrime={selectedPrime}
        />
      </RiskDetailDrawer>
    </div>
  );
}

export default App;
