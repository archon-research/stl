import { SidebarLayout } from '@archon-research/design-system';
import type { SortingState } from '@tanstack/react-table';
import { useEffect, useMemo, useRef, useState } from 'react';

import { css } from '#styled-system/css';

import { AllocationGrid } from './components/allocations/AllocationGrid';
import { BottomPanel } from './components/allocations/BottomPanel';
import { RiskDetailDrawer } from './components/allocations/RiskDetailDrawer';
import {
  ChainLogo,
  ProtocolLogo,
  TokenLogo,
} from './components/shared';
import { PrimeSidebar } from './components/shared/PrimeSidebar';
import { TopBar } from './components/shared/TopBar';
import { useUrlSyncedTableState } from './data-table/hooks';
import { buildRowSearchString, matchesSearchQuery } from './data-table/utils';
import {
  getAllocations,
  getCapitalMetrics,
  getChains,
  getDataSources,
  getPrimes,
  getProtocols,
} from './lib/api';
import {
  buildChainLabelLookup,
  buildNetworkOptions,
  buildProtocolOptions,
  formatTokenAmount,
  formatUsdValue,
  getChainLabel,
  getAllocationKey,
  getProtocolLabel,
} from './lib/dashboard';
import { isAbortError, toErrorMessage } from './lib/errors';
import { logging } from './lib/logging';
import { PARAMS, useUrlParam } from './lib/url-params';
import type {
  Allocation,
  CapitalMetrics,
  DataSource,
  Prime,
} from './types/allocation';
import type { LocalChainRow, LocalProtocolRow } from './types/local-data';

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
  const [, setDataSources] = useState<DataSource[]>([]);
  const [localChains, setLocalChains] = useState<LocalChainRow[]>([]);
  const [localProtocols, setLocalProtocols] = useState<LocalProtocolRow[]>([]);
  const [capitalMetrics, setCapitalMetrics] = useState<CapitalMetrics | null>(
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
  const { globalFilter, setGlobalFilter, setSorting, sorting } =
    useUrlSyncedTableState(PARAMS.sort, PARAMS.search);

  const previousPrimeIdRef = useRef<string | null>(selectedPrimeId);
  const isDrawerOpen = isDrawerOpenParam === '1';

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
      setCapitalMetrics(null);
      setIsCapitalMetricsLoading(false);
      return;
    }

    const controller = new AbortController();
    setIsCapitalMetricsLoading(true);

    if (!primes.some((prime) => prime.id === selectedPrimeId)) {
      setCapitalMetrics(null);
      setIsCapitalMetricsLoading(false);
      return () => controller.abort();
    }

    void getCapitalMetrics(controller.signal)
      .then((metrics) => {
        const selectedMetric = metrics.find(
          (metric) =>
            metric.prime_id.trim().toLowerCase() ===
            selectedPrimeId.trim().toLowerCase(),
        );

        if (!selectedMetric) {
          setCapitalMetrics(null);
          return;
        }

        setCapitalMetrics(selectedMetric);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.error('Failed to load capital metrics', {
          error,
          primeId: selectedPrimeId,
        });
        setCapitalMetrics(null);
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsCapitalMetricsLoading(false);
        }
      });

    return () => controller.abort();
  }, [selectedPrimeId, primes]);

  const selectedPrime = useMemo(
    () => primes.find((prime) => prime.id === selectedPrimeId) ?? null,
    [selectedPrimeId, primes],
  );

  const chainLabels = useMemo(
    () => buildChainLabelLookup(localChains),
    [localChains],
  );

  const networkOptions = useMemo(
    () => buildNetworkOptions(allocations, chainLabels),
    [allocations, chainLabels],
  );

  const protocolOptions = useMemo(
    () => buildProtocolOptions(allocations, localProtocols),
    [allocations, localProtocols],
  );

  useEffect(() => {
    if (isAllocationsLoading || !selectedNetwork) {
      return;
    }

    if (!networkOptions.some((option) => option.value === selectedNetwork)) {
      setSelectedNetwork(null);
    }
  }, [
    isAllocationsLoading,
    networkOptions,
    selectedNetwork,
    setSelectedNetwork,
  ]);

  useEffect(() => {
    if (isAllocationsLoading || !selectedProtocol) {
      return;
    }

    if (!protocolOptions.some((option) => option.value === selectedProtocol)) {
      setSelectedProtocol(null);
    }
  }, [
    isAllocationsLoading,
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
          allocation.protocol_name === selectedProtocol;

        return matchesNetwork && matchesProtocol;
      }),
    [searchFilteredAllocations, selectedNetwork, selectedProtocol],
  );

  useEffect(() => {
    if (filteredAllocations.length === 0) {
      if (selectedAllocationKey !== null) {
        setSelectedAllocationKey(null);
      }
      if (isDrawerOpen) {
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
    isDrawerOpen,
    selectedAllocationKey,
    setIsDrawerOpenParam,
  ]);

  useEffect(() => {
    if (!isDrawerOpen) {
      return;
    }

    if (selectedAllocationKey === null && filteredAllocations.length > 0) {
      return;
    }

    if (
      isDrawerOpen &&
      (!selectedAllocationKey ||
        !filteredAllocations.some(
          (allocation) =>
            getAllocationKey(allocation) === selectedAllocationKey,
        ))
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
        '& [data-sidebar-layout] [role="separator"]': {
          right: '0 !important',
        },
        '& [data-sidebar-layout] [role="separator"] > [aria-hidden="true"]': {
          opacity: 0,
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
            />
          }
          main={
            <AllocationGrid
              allocations={allocations}
              capitalMetrics={capitalMetrics}
              chainLabels={chainLabels}
              errorMessage={allocationsErrorMessage}
              filteredAllocations={filteredAllocations}
              topMetricsAllocations={searchFilteredAllocations}
              isLoading={isAllocationsLoading}
              isCapitalMetricsLoading={isCapitalMetricsLoading}
              localProtocols={localProtocols}
              onSelectAllocation={(allocationKey) => {
                setSelectedAllocationKey(allocationKey);
                setIsDrawerOpenParam('1');
              }}
              onSearchChange={setGlobalFilter}
              onSortingChange={setSorting}
              searchValue={globalFilter}
              selectedAllocationKey={selectedAllocationKey}
              selectedPrime={selectedPrime}
              sorting={sorting as SortingState}
            />
          }
        />
      </div>

      <RiskDetailDrawer
        detail={
          selectedAllocation
            ? `${formatTokenAmount(selectedAllocation.balance)} ${selectedAllocation.symbol} · ${formatUsdValue(selectedAllocation.amount_usd ?? null)}`
            : undefined
        }
        isOpen={isDrawerOpen}
        onClose={() => setIsDrawerOpenParam(null)}
        subtitle={
          selectedAllocation
            ? (
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
              )
            : undefined
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
