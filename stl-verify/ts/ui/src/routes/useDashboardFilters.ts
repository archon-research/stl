import { useSearch } from '@tanstack/react-router';
import { useEffect, useMemo } from 'react';

import { useAllocationRows } from '../features/allocations/useAllocationRows';
import {
  useChainLabels,
  useLocalChains,
  useLocalProtocols,
} from '../shared/hooks/useRegistries';
import { useUpdateSearch } from '../shared/hooks/useUpdateSearch';
import {
  buildNetworkOptions,
  buildNetworkOptionsFromMetadata,
  buildProtocolOptions,
  buildProtocolOptionsFromMetadata,
  type FilterOption,
  type PrimeGroup,
} from '../shared/lib/dashboard';
import type { DashboardView } from './navigation';

export type DashboardFilters = {
  networkOptions: FilterOption[];
  protocolOptions: FilterOption[];
};

/**
 * The network and protocol chips the top bar offers, and the pruning of a
 * selection they no longer contain.
 *
 * Activities spans every prime, so its options come from the global registries;
 * allocations scope to the selected prime's holdings, which is why the shell
 * reads the prime's rows even though the grid is what draws them.
 */
export function useDashboardFilters(
  view: DashboardView,
  primeGroup: PrimeGroup | null,
): DashboardFilters {
  const search = useSearch({ from: '__root__' });
  const updateSearch = useUpdateSearch();
  const chainLabels = useChainLabels();
  const localChains = useLocalChains();
  const localProtocols = useLocalProtocols();
  const { allocations, isLoaded: areAllocationsLoaded } =
    useAllocationRows(primeGroup);

  const isActivitiesView = view === 'activities';
  const selectedNetwork = search.network ?? null;
  const selectedProtocol = search.protocol ?? null;

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
  // or another prime's rows read as "no such option" and wipe ?network=. The
  // rows are keyed by proxy, so a prime's own answer is the only one that can
  // be in hand for it.
  const networkOptionsLoading = isActivitiesView
    ? localChains.length === 0
    : !areAllocationsLoaded;
  const protocolOptionsLoading = isActivitiesView
    ? localProtocols.length === 0
    : !areAllocationsLoaded;

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

  return { networkOptions, protocolOptions };
}
