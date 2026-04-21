import { SidebarLayout } from '@archon-research/design-system';
import { useEffect, useMemo, useRef, useState } from 'react';

import { AllocationGrid } from './components/allocations/AllocationGrid';
import { BottomPanel } from './components/allocations/BottomPanel';
import { StarSidebar } from './components/shared/StarSidebar';
import { TopBar } from './components/shared/TopBar';
import { getAllocations, getStars } from './lib/api';
import {
  buildNetworkOptions,
  buildProtocolOptions,
  getAllocationKey,
} from './lib/dashboard';
import { PARAMS, useUrlParam } from './lib/url-params';
import type { AllocationPosition, Star } from './types/allocation';

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === 'AbortError';
}

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'Unknown request failure.';
}

function App() {
  const [stars, setStars] = useState<Star[]>([]);
  const [starsErrorMessage, setStarsErrorMessage] = useState<string | null>(
    null,
  );
  const [isStarsLoading, setIsStarsLoading] = useState(true);
  const [starsReloadKey, setStarsReloadKey] = useState(0);
  const [allocations, setAllocations] = useState<AllocationPosition[]>([]);
  const [allocationsErrorMessage, setAllocationsErrorMessage] = useState<
    string | null
  >(null);
  const [isAllocationsLoading, setIsAllocationsLoading] = useState(false);
  const [allocationsReloadKey, setAllocationsReloadKey] = useState(0);
  const [selectedAllocationKey, setSelectedAllocationKey] = useState<
    string | null
  >(null);
  const [selectedStarId, setSelectedStarId] = useUrlParam(PARAMS.star);
  const [selectedNetwork, setSelectedNetwork] = useUrlParam(PARAMS.network);
  const [selectedProtocol, setSelectedProtocol] = useUrlParam(PARAMS.protocol);

  const previousStarIdRef = useRef<string | null>(selectedStarId);

  useEffect(() => {
    const controller = new AbortController();

    setIsStarsLoading(true);
    setStarsErrorMessage(null);

    void getStars(controller.signal)
      .then((response) => {
        setStars(response);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        setStarsErrorMessage(toErrorMessage(error));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsStarsLoading(false);
        }
      });

    return () => controller.abort();
  }, [starsReloadKey]);

  useEffect(() => {
    if (isStarsLoading) {
      return;
    }

    if (stars.length === 0) {
      if (selectedStarId !== null) {
        setSelectedStarId(null);
      }
      return;
    }

    if (!selectedStarId || !stars.some((star) => star.id === selectedStarId)) {
      setSelectedStarId(stars[0]?.id ?? null);
    }
  }, [isStarsLoading, selectedStarId, setSelectedStarId, stars]);

  useEffect(() => {
    if (
      previousStarIdRef.current !== null &&
      previousStarIdRef.current !== selectedStarId
    ) {
      setSelectedNetwork(null);
      setSelectedProtocol(null);
      setSelectedAllocationKey(null);
    }

    previousStarIdRef.current = selectedStarId;
  }, [selectedStarId, setSelectedNetwork, setSelectedProtocol]);

  useEffect(() => {
    if (!selectedStarId) {
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

    void getAllocations(selectedStarId, controller.signal)
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
  }, [allocationsReloadKey, selectedStarId]);

  const selectedStar = useMemo(
    () => stars.find((star) => star.id === selectedStarId) ?? null,
    [selectedStarId, stars],
  );

  const networkOptions = useMemo(
    () => buildNetworkOptions(allocations),
    [allocations],
  );

  const protocolOptions = useMemo(
    () => buildProtocolOptions(allocations),
    [allocations],
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
          selectedProtocol === null || allocation.name === selectedProtocol;

        return matchesNetwork && matchesProtocol;
      }),
    [allocations, selectedNetwork, selectedProtocol],
  );

  useEffect(() => {
    if (filteredAllocations.length === 0) {
      if (selectedAllocationKey !== null) {
        setSelectedAllocationKey(null);
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

  const selectedAllocation = useMemo(
    () =>
      filteredAllocations.find(
        (allocation) => getAllocationKey(allocation) === selectedAllocationKey,
      ) ?? null,
    [filteredAllocations, selectedAllocationKey],
  );

  const activeNetworkLabel =
    networkOptions.find((option) => option.value === selectedNetwork)?.label ??
    null;

  const activeProtocolLabel =
    protocolOptions.find((option) => option.value === selectedProtocol)
      ?.label ?? null;

  return (
    <SidebarLayout
      sidebar={
        <StarSidebar
          stars={stars}
          selectedStarId={selectedStarId}
          isLoading={isStarsLoading}
          errorMessage={starsErrorMessage}
          onRetry={() => setStarsReloadKey((value) => value + 1)}
          onSelectStar={setSelectedStarId}
        />
      }
      topBar={
        <TopBar
          allocationCount={allocations.length}
          filteredAllocationCount={filteredAllocations.length}
          networkOptions={networkOptions}
          onNetworkChange={setSelectedNetwork}
          onProtocolChange={setSelectedProtocol}
          protocolOptions={protocolOptions}
          selectedNetwork={selectedNetwork}
          selectedProtocol={selectedProtocol}
          selectedStar={selectedStar}
          starCount={stars.length}
        />
      }
      main={
        <AllocationGrid
          activeNetworkLabel={activeNetworkLabel}
          activeProtocolLabel={activeProtocolLabel}
          allocations={allocations}
          errorMessage={allocationsErrorMessage}
          filteredAllocations={filteredAllocations}
          isLoading={isAllocationsLoading}
          onRetry={() => setAllocationsReloadKey((value) => value + 1)}
          onSelectAllocation={setSelectedAllocationKey}
          selectedAllocationKey={selectedAllocationKey}
          selectedStar={selectedStar}
        />
      }
      bottomPanel={
        <BottomPanel
          selectedAllocation={selectedAllocation}
          selectedStar={selectedStar}
        />
      }
    />
  );
}

export default App;
