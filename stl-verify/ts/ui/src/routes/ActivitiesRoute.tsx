import { toSearchOption } from '@archon-research/router-kit';
import { useSearch } from '@tanstack/react-router';

import { ActivityFeed } from '../features/activity/ActivityFeed';
import { useChainLabels, useTokenSymbols } from '../shared/hooks/useRegistries';
import { useUpdateSearch } from '../shared/hooks/useUpdateSearch';
import { ACTIVITY_ACTIONS } from '../shared/lib/search-params';
import { usePrimeSelection } from './prime-selection';
import { useTimeRange } from './time-range';

/**
 * The activities view: the same feed the risk drawer hosts, in page mode.
 *
 * It spans every prime when asked to, so it reads the global registries rather
 * than anything scoped to the selected prime's holdings.
 */
export function ActivitiesRoute() {
  const { selectedPrime } = usePrimeSelection();
  const { rangePreset, timeRange, onRangeChange } = useTimeRange();
  const search = useSearch({ from: '/activities' });
  const updateSearch = useUpdateSearch();
  const chainLabels = useChainLabels();
  const tokenSymbolOptions = useTokenSymbols();

  return (
    <ActivityFeed
      isEnabled
      mode="page"
      chainLabels={chainLabels}
      selectedNetwork={search.network ?? null}
      selectedProtocol={search.protocol ?? null}
      showAllPrimes={search.allp !== '0'}
      selectedPrime={selectedPrime}
      tokenOptions={tokenSymbolOptions}
      tokenFilter={search.token ?? null}
      onTokenFilterChange={(value) =>
        updateSearch({ token: value ?? undefined })
      }
      actionFilter={search.aa}
      onActionFilterChange={(value) =>
        updateSearch({ aa: toSearchOption(value, ACTIVITY_ACTIONS) })
      }
      externalRangePreset={rangePreset}
      externalTimeRange={timeRange}
      onRangeChange={onRangeChange}
    />
  );
}
