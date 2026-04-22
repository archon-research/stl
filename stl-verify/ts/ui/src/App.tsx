import { SidebarLayout } from '@archon-research/design-system';
import { useEffect, useMemo, useRef, useState } from 'react';

import { AllocationGrid } from './components/allocations/AllocationGrid';
import { BottomPanel } from './components/allocations/BottomPanel';
import { StarSidebar } from './components/shared/StarSidebar';
import { TopBar } from './components/shared/TopBar';
import {
  getAllocations,
  getLocalChains,
  getLocalProtocols,
  getStars,
} from './lib/api';
import {
  buildChainLabelLookup,
  buildNetworkOptions,
  buildProtocolOptions,
  getAllocationKey,
} from './lib/dashboard';
import { isAbortError, toErrorMessage } from './lib/errors';
import { PARAMS, useUrlParam } from './lib/url-params';
import type { AllocationPosition, Star } from './types/allocation';
import type { LocalChainRow, LocalProtocolRow } from './types/local-data';

function App() {
  const [stars, setStars] = useState<Star[]>([]);
  const [starsErrorMessage, setStarsErrorMessage] = useState<string | null>(
    null,
  );
  const [isStarsLoading, setIsStarsLoading] = useState(true);
  const [allocations, setAllocations] = useState<AllocationPosition[]>([]);
  const [allocationsErrorMessage, setAllocationsErrorMessage] = useState<
    string | null
  >(null);
  const [isAllocationsLoading, setIsAllocationsLoading] = useState(false);
  const [localChains, setLocalChains] = useState<LocalChainRow[]>([]);
  const [localProtocols, setLocalProtocols] = useState<LocalProtocolRow[]>([]);
  const [selectedAllocationKey, setSelectedAllocationKey] = useState<
    string | null
  >(null);
  const [selectedStarId, setSelectedStarId] = useUrlParam(PARAMS.star);
  const [selectedNetwork, setSelectedNetwork] = useUrlParam(PARAMS.network);
  const [selectedProtocol, setSelectedProtocol] = useUrlParam(PARAMS.protocol);

  const previousStarIdRef = useRef<string | null>(selectedStarId);

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
  }, []);

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
  }, [selectedStarId]);

  const selectedStar = useMemo(
    () => stars.find((star) => star.id === selectedStarId) ?? null,
    [selectedStarId, stars],
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

  return (
    <SidebarLayout
      sidebar={
        <StarSidebar
          stars={stars}
          selectedStarId={selectedStarId}
          isLoading={isStarsLoading}
          errorMessage={starsErrorMessage}
          onSelectStar={setSelectedStarId}
        />
      }
      topBar={
        <TopBar
          hasSelectedStar={selectedStar !== null}
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
          onSelectAllocation={setSelectedAllocationKey}
          selectedAllocationKey={selectedAllocationKey}
          selectedStar={selectedStar}
        />
      }
      bottomPanel={
        <BottomPanel
          chainLabels={chainLabels}
          localProtocols={localProtocols}
          selectedAllocation={selectedAllocation}
          selectedStar={selectedStar}
        />
      }
    />
  );
}

export default App;
