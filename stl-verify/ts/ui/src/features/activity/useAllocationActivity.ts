import type { RangePreset, TimeRange } from '@archon-research/design-system';
import { useQuery } from '@tanstack/react-query';
import { useMemo, useState } from 'react';

import { DIRECT_PROTOCOL_FILTER_VALUE } from '../../shared/lib/dashboard';
import {
  DEFAULT_RANGE_PRESET,
  defaultTimeRange,
} from '../../shared/lib/default-range';
import { toQueryErrorMessage } from '../../shared/lib/errors';
import { activityQuery } from '../../shared/lib/queries';
import type {
  Allocation,
  AllocationActivityResponse,
  Prime,
} from '../../shared/types/allocation';
import { getRealTxHash } from './activityColumns';

type ActivityFilters = {
  from_timestamp?: string;
  to_timestamp?: string;
  limit?: number;
  rangePreset: RangePreset;
};

// Stable fallback for a query that has not answered: a literal `[]` would give
// the search memo below a fresh array to compare on every render.
const NO_EVENTS: AllocationActivityResponse = [];

// Shared by requestFilters and the page-mode chain-mismatch guard so the two
// cannot parse the network param differently.
//
// `null` is a filter this view cannot express, distinct from `undefined`, no
// filter: reading an unindexed chain as unfiltered answers with every chain's
// flows behind a visibly-active single-network chip.
function parseNetworkChainId(
  selectedNetwork: string | null | undefined,
): number | undefined | null {
  if (!selectedNetwork || selectedNetwork.length === 0) {
    return undefined;
  }

  const parsed = Number(selectedNetwork);
  return Number.isFinite(parsed) ? parsed : null;
}

type ActivityFeedState = {
  isPageMode: boolean;
  events: AllocationActivityResponse;
  filteredEvents: AllocationActivityResponse;
  isLoading: boolean;
  error: string | null;
  effectivePreset: RangePreset;
  effectiveRange: TimeRange;
  updateRangePreset: (preset: RangePreset, range: TimeRange) => void;
  uniqueTokenOptions: string[];
  hasActiveFilters: boolean;
  clearFilters: () => void;
  rowLimit: number;
  // The network the filter names when STL has no chain id for it. Page mode has
  // no receipt token to read the same fact off, so the empty state can only name
  // the chain if the parse result is carried out here.
  unindexedNetwork: string | null;
};

/**
 * Everything this hook reads off the feed's props.
 */
export type UseAllocationActivityProps = {
  isEnabled: boolean;
  mode?: 'drawer' | 'page';
  actionFilter?: string;
  // Page mode: action/token filters are URL-backed and controlled by the parent
  // so they survive reloads and power deep links (e.g. "View in Activities").
  onActionFilterChange?: (value: string | null) => void;
  tokenFilter?: string | null;
  onTokenFilterChange?: (value: string | null) => void;
  selectedNetwork?: string | null;
  selectedProtocol?: string | null;
  selectedPrime: Prime | null;
  selectedReceiptToken?: Allocation | null;
  searchQuery?: string;
  showAllPrimes?: boolean;
  tokenOptions?: string[];
  // External range control: provided by parent-owned top bar picker.
  externalRangePreset?: RangePreset;
  externalTimeRange?: TimeRange;
  onRangeChange?: (preset: RangePreset, range: TimeRange) => void;
};

/**
 * Everything the activity view needs from the server and from filter state:
 * the scope guards that decide whether a request is meaningful at all, the
 * range plumbing (local in drawer mode, parent-owned in page mode), the fetch
 * lifecycle, and the client-side search narrowing on top of the fetched rows.
 */
export function useAllocationActivity({
  actionFilter,
  onActionFilterChange,
  tokenFilter = null,
  onTokenFilterChange,
  isEnabled,
  mode = 'drawer',
  selectedNetwork,
  selectedProtocol,
  selectedPrime,
  selectedReceiptToken = null,
  searchQuery = '',
  showAllPrimes = false,
  tokenOptions = [],
  externalRangePreset,
  externalTimeRange,
  onRangeChange: onExternalRangeChange,
}: UseAllocationActivityProps): ActivityFeedState {
  const isPageMode = mode === 'page';
  const networkChainId = parseNetworkChainId(selectedNetwork);
  const unindexedNetwork =
    networkChainId === null ? (selectedNetwork ?? null) : null;
  const [filters, setFilters] = useState<ActivityFilters>(() => {
    const initialRange = defaultTimeRange();
    return {
      limit: 50,
      rangePreset: DEFAULT_RANGE_PRESET,
      from_timestamp: initialRange.from_timestamp,
      to_timestamp: initialRange.to_timestamp,
    };
  });
  // The parent (page mode) owns the range and passes it via props; the local
  // `filters` range is only the source of truth in standalone/drawer mode.
  const isRangeControlled =
    externalTimeRange !== undefined && onExternalRangeChange !== undefined;
  const uniqueTokenOptions = useMemo(() => {
    const symbols = new Set(tokenOptions);
    // Keep a deep-linked token selectable even if it isn't in the catalog list.
    if (tokenFilter) {
      symbols.add(tokenFilter);
    }
    return Array.from(symbols).sort((a, b) => a.localeCompare(b));
  }, [tokenOptions, tokenFilter]);
  const updateRangePreset = (preset: RangePreset, range: TimeRange) => {
    if (isRangeControlled) {
      onExternalRangeChange?.(preset, range);
    } else {
      setFilters((previous) => ({
        ...previous,
        rangePreset: preset,
        from_timestamp: range.from_timestamp,
        to_timestamp: range.to_timestamp,
      }));
    }
  };

  // When the parent drives range via props, use those values over local state.
  const effectivePreset = isRangeControlled
    ? (externalRangePreset ?? DEFAULT_RANGE_PRESET)
    : filters.rangePreset;
  const effectiveRange = useMemo<TimeRange>(() => {
    if (isRangeControlled && externalTimeRange) {
      return externalTimeRange;
    }
    // filters is always seeded with a range; fall back defensively so the
    // strict TimeRange (non-optional timestamps) always holds.
    const fallback = defaultTimeRange();
    return {
      from_timestamp: filters.from_timestamp ?? fallback.from_timestamp,
      to_timestamp: filters.to_timestamp ?? fallback.to_timestamp,
    };
  }, [
    isRangeControlled,
    externalTimeRange,
    filters.from_timestamp,
    filters.to_timestamp,
  ]);

  // Page mode: action/token come from controlled props (URL-backed); the date
  // range stays local. The range is always seeded with a default, so a
  // non-default preset — not the mere presence of timestamps — is what marks
  // the range as an active filter for the "clear" affordance.
  const hasActiveFilters = Boolean(
    actionFilter || tokenFilter || effectivePreset !== DEFAULT_RANGE_PRESET,
  );

  const clearFilters = () => {
    onActionFilterChange?.(null);
    onTokenFilterChange?.(null);
    const nextRange = defaultTimeRange();
    if (isRangeControlled) {
      onExternalRangeChange?.(DEFAULT_RANGE_PRESET, nextRange);
    }
    setFilters({
      limit: filters.limit ?? 50,
      rangePreset: DEFAULT_RANGE_PRESET,
      from_timestamp: nextRange.from_timestamp,
      to_timestamp: nextRange.to_timestamp,
    });
  };

  const requestFilters = useMemo(() => {
    if (isPageMode) {
      return {
        prime_id: showAllPrimes ? undefined : (selectedPrime?.id ?? undefined),
        chain_id: networkChainId ?? undefined,
        protocol_name:
          selectedProtocol && selectedProtocol !== DIRECT_PROTOCOL_FILTER_VALUE
            ? selectedProtocol
            : undefined,
        token_symbol: tokenFilter || undefined,
        action_type: actionFilter || undefined,
        from_timestamp: effectiveRange.from_timestamp,
        to_timestamp: effectiveRange.to_timestamp,
        limit: filters.limit ?? 50,
      };
    }

    return {
      prime_id: selectedPrime?.id,
      chain_id: selectedReceiptToken?.chain_id ?? undefined,
      token_symbol: selectedReceiptToken?.symbol,
      action_type: actionFilter,
      limit: filters.limit ?? 50,
    };
  }, [
    actionFilter,
    effectiveRange,
    filters,
    isPageMode,
    networkChainId,
    selectedPrime?.id,
    selectedProtocol,
    selectedReceiptToken?.chain_id,
    selectedReceiptToken?.symbol,
    showAllPrimes,
    tokenFilter,
  ]);

  // Don't fetch without a scope: drawer always needs a prime; page mode needs
  // one too unless "show all primes" is on (otherwise prime_id is undefined
  // and we'd issue an unfiltered request the UI never asked for). A row on a
  // chain STL has no id for cannot be scoped at all.
  const missingScope = isPageMode
    ? (!showAllPrimes && !selectedPrime) || networkChainId === null
    : !selectedPrime || selectedReceiptToken?.chain_id === null;

  const canLoadActivity = isEnabled && !missingScope;

  const activityResult = useQuery({
    ...activityQuery(requestFilters),
    enabled: canLoadActivity,
  });

  // Read through the gate, not just fetched behind it. The scope fields this
  // query drops when it cannot be asked are `undefined`, which the key
  // sanitizer strips — so a disabled query lands on the *unscoped* request's
  // cache entry and would otherwise render another scope's rows.
  const events = canLoadActivity
    ? (activityResult.data ?? NO_EVENTS)
    : NO_EVENTS;
  const isLoading = canLoadActivity && activityResult.isPending;
  const error = canLoadActivity
    ? toQueryErrorMessage(activityResult.error)
    : null;

  const filteredEvents = useMemo(() => {
    if (!searchQuery) {
      return events;
    }

    const lowerQuery = searchQuery.toLowerCase();
    return events.filter(
      (event) =>
        event.token_symbol?.toLowerCase().includes(lowerQuery) ||
        event.protocol_name?.toLowerCase().includes(lowerQuery) ||
        event.action_type?.toLowerCase().includes(lowerQuery) ||
        getRealTxHash(event)?.toLowerCase().includes(lowerQuery),
    );
  }, [events, searchQuery]);

  return {
    isPageMode,
    events,
    filteredEvents,
    isLoading,
    error,
    effectivePreset,
    effectiveRange,
    updateRangePreset,
    uniqueTokenOptions,
    hasActiveFilters,
    clearFilters,
    rowLimit: filters.limit ?? 50,
    unindexedNetwork,
  };
}
