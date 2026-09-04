import { useSearch } from '@tanstack/react-router';
import { useEffect, useMemo } from 'react';

import { useAllocationRows } from '../features/allocations/useAllocationRows';
import {
  hasCompleteRows,
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
  // Whether an empty chip list is empty because its registry failed. The two
  // look identical in the top bar, and only one of them is a fact about the
  // data.
  networkOptionsFailed: boolean;
  protocolOptionsFailed: boolean;
};

/**
 * The network and protocol chips the top bar offers, and the pruning of a
 * selection they no longer contain.
 *
 * Activities spans every prime, so its options come from the global registries;
 * allocations scope to the selected prime's holdings, which is why the shell
 * reads the prime's rows even though the grid is what draws them.
 *
 * Pruning is the destructive half and is held to a higher bar: it only runs
 * once the options are known to be the whole set. A registry that failed
 * supplies no such set, and pruning against it would delete a reader's
 * `?network=` on the strength of a list that was never fetched — leaving the
 * feed silently widened to every chain, with a reload no longer carrying the
 * filter back.
 */
export function useDashboardFilters(
  view: DashboardView,
  primeGroup: PrimeGroup | null,
): DashboardFilters {
  const search = useSearch({ from: '__root__' });
  const updateSearch = useUpdateSearch();
  const chainLabels = useChainLabels();
  const chains = useLocalChains();
  const protocols = useLocalProtocols();

  const isActivitiesView = view === 'activities';

  // Passed no prime on the activities view, which fans out to no queries: that
  // view's chips come from the registries below, so fetching the prime's rows
  // for it would be a request whose answer is discarded.
  const { allocations, isLoaded: areAllocationsLoaded } = useAllocationRows(
    isActivitiesView ? null : primeGroup,
  );
  const selectedNetwork = search.network ?? null;
  const selectedProtocol = search.protocol ?? null;

  const networkOptions = useMemo(
    () =>
      isActivitiesView
        ? buildNetworkOptionsFromMetadata(chains.rows)
        : buildNetworkOptions(allocations, chainLabels),
    [allocations, chainLabels, chains.rows, isActivitiesView],
  );

  const protocolOptions = useMemo(
    () =>
      isActivitiesView
        ? buildProtocolOptionsFromMetadata(protocols.rows)
        : buildProtocolOptions(allocations, protocols.rows),
    [allocations, isActivitiesView, protocols.rows],
  );

  // Pruning deletes `?network=` and nothing puts it back, so it needs a list
  // that is complete — not merely one that has stopped loading.
  const canPruneNetwork = isActivitiesView
    ? hasCompleteRows(chains)
    : areAllocationsLoaded;
  const canPruneProtocol = isActivitiesView
    ? hasCompleteRows(protocols)
    : areAllocationsLoaded;

  useEffect(() => {
    if (!canPruneNetwork || !selectedNetwork) {
      return;
    }

    if (!networkOptions.some((option) => option.value === selectedNetwork)) {
      updateSearch({ network: undefined });
    }
  }, [canPruneNetwork, networkOptions, selectedNetwork, updateSearch]);

  useEffect(() => {
    if (!canPruneProtocol || !selectedProtocol) {
      return;
    }

    if (!protocolOptions.some((option) => option.value === selectedProtocol)) {
      updateSearch({ protocol: undefined });
    }
  }, [canPruneProtocol, protocolOptions, selectedProtocol, updateSearch]);

  // Registry-driven only: the allocations view builds its chips from the
  // prime's own rows, which a failed registry leaves labelled but present.
  return {
    networkOptions,
    protocolOptions,
    networkOptionsFailed: isActivitiesView && chains.isError,
    protocolOptionsFailed: isActivitiesView && protocols.isError,
  };
}
