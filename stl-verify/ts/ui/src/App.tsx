import { SidebarLayout } from '@archon-research/design-system';
import type { SortingState } from '@tanstack/react-table';
import { useEffect, useMemo, useRef, useState } from 'react';

import { css } from '#styled-system/css';

import { AllocationGrid } from './components/allocations/AllocationGrid';
import { BottomPanel } from './components/allocations/BottomPanel';
import { RiskDetailDrawer } from './components/allocations/RiskDetailDrawer';
import { PrimeSidebar } from './components/shared/PrimeSidebar';
import { TopBar } from './components/shared/TopBar';
import { useUrlSyncedTableState } from './data-table/hooks';
import { buildRowSearchString, matchesSearchQuery } from './data-table/utils';
import {
  getAllocations,
  getLocalChains,
  getLocalProtocols,
  getPrimes,
} from './lib/api';
import {
  buildChainLabelLookup,
  buildNetworkOptions,
  buildProtocolOptions,
  getChainLabel,
  getAllocationKey,
  getProtocolLabel,
} from './lib/dashboard';
import { isAbortError, toErrorMessage } from './lib/errors';
import { PARAMS, useUrlParam } from './lib/url-params';
import type { Allocation, Prime } from './types/allocation';
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
  const [localChains, setLocalChains] = useState<LocalChainRow[]>([]);
  const [localProtocols, setLocalProtocols] = useState<LocalProtocolRow[]>([]);
  const [selectedAllocationKey, setSelectedAllocationKey] = useState<
    string | null
  >(null);
  const [isDrawerOpenParam, setIsDrawerOpenParam] = useUrlParam(
    PARAMS.drawerOpen,
  );
  const [selectedPrimeId, setSelectedPrimeId] = useUrlParam(PARAMS.prime);
  const [selectedNetwork, setSelectedNetwork] = useUrlParam(PARAMS.network);
  const [selectedProtocol, setSelectedProtocol] = useUrlParam(PARAMS.protocol);
  const {
    globalFilter,
    setGlobalFilter,
    setSorting,
    sorting,
  } = useUrlSyncedTableState(PARAMS.sort, PARAMS.search);

  const previousPrimeIdRef = useRef<string | null>(selectedPrimeId);
  const isDrawerOpen = isDrawerOpenParam === '1';

  useEffect(() => {
    const controller = new AbortController();

    void Promise.all([
      getLocalChains(controller.signal),
      getLocalProtocols(controller.signal),
    ])
      .then(([chains, protocols]) => {
        setLocalChains(chains);
        setLocalProtocols(protocols);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

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

        setAllocationsErrorMessage(toErrorMessage(error));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsAllocationsLoading(false);
        }
      });

    return () => controller.abort();
  }, [selectedPrimeId]);

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

  const filteredAllocations = useMemo(
    () =>
      allocations.filter((allocation) => {
        const matchesNetwork =
          selectedNetwork === null ||
          String(allocation.chain_id) === selectedNetwork;
        const matchesProtocol =
          selectedProtocol === null ||
          allocation.protocol_name === selectedProtocol;
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

        return matchesNetwork && matchesProtocol && matchesGlobalFilter;
      }),
    [
      allocations,
      chainLabels,
      globalFilter,
      localProtocols,
      selectedNetwork,
      selectedProtocol,
    ],
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
  }, [filteredAllocations, selectedAllocationKey]);

  useEffect(() => {
    if (
      isDrawerOpen &&
      (!selectedAllocationKey ||
        !filteredAllocations.some(
          (allocation) => getAllocationKey(allocation) === selectedAllocationKey,
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

  return (
    <div
      className={css({
        position: 'relative',
        '& [aria-label="Resize sidebar"]': {
          right: '0 !important',
        },
        '& [aria-label="Resize sidebar"] > [aria-hidden="true"]': {
          opacity: 0,
        },
      })}
    >
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
            chainLabels={chainLabels}
            errorMessage={allocationsErrorMessage}
            filteredAllocations={filteredAllocations}
            isLoading={isAllocationsLoading}
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

      <RiskDetailDrawer
        isOpen={isDrawerOpen}
        onClose={() => setIsDrawerOpenParam(null)}
        title={selectedAllocation ? selectedAllocation.symbol : 'Risk details'}
      >
        <BottomPanel
          allocations={allocations}
          chainLabels={chainLabels}
          errorMessage={allocationsErrorMessage}
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
