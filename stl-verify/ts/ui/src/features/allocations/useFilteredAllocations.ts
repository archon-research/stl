import {
  buildRowSearchString,
  matchesSearchQuery,
} from '@archon-research/design-system';
import { useMemo } from 'react';

import {
  allocationNetworkKey,
  DIRECT_PROTOCOL_FILTER_VALUE,
  getChainLabel,
  getProtocolLabel,
  type ChainLabelLookup,
} from '../../shared/lib/dashboard';
import type { Allocation } from '../../shared/types/allocation';
import type { LocalProtocolRow } from '../../shared/types/local-data';

export type AllocationFilters = {
  allocations: Allocation[];
  chainLabels: ChainLabelLookup;
  localProtocols: LocalProtocolRow[];
  globalFilter: string;
  selectedNetwork: string | null;
  selectedProtocol: string | null;
};

export type FilteredAllocations = {
  // Search applied but not the chips. The top metrics read this so a network
  // chip narrows the table without restating the prime's totals.
  searchFilteredAllocations: Allocation[];
  filteredAllocations: Allocation[];
};

/**
 * Narrows the prime's rows the way the screen does: free-text search first,
 * then the network and protocol chips.
 */
export function useFilteredAllocations({
  allocations,
  chainLabels,
  localProtocols,
  globalFilter,
  selectedNetwork,
  selectedProtocol,
}: AllocationFilters): FilteredAllocations {
  const searchFilteredAllocations = useMemo(
    () =>
      allocations.filter((allocation) =>
        matchesSearchQuery(
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
        ),
      ),
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

  return { searchFilteredAllocations, filteredAllocations };
}
