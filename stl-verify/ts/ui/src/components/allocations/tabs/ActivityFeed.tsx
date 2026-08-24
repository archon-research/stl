import {
  AsyncStateRenderer,
  DataTable,
  type DataTableProps,
  defineIdentifiedColumns,
  EmptyState,
  ErrorState,
  numericColumnMeta,
  SkeletonStack,
  StyledSelect,
  useDataTable,
} from '@archon-research/design-system';
import {
  type ChangeEvent,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from 'react';

import { css, cx } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { getActionColorClass, getActionIcon } from '../../../lib/activity';
import {
  ApiRequestError,
  getAllocationActivity,
  getProtocolEvents,
  getTxProtocolEvents,
} from '../../../lib/api';
import {
  type ChainLabelLookup,
  DIRECT_PROTOCOL_FILTER_VALUE,
  formatDateTime,
  formatTokenAmount,
  formatFreshnessLabel,
  getChainLabel,
  parseNumericValue,
} from '../../../lib/dashboard';
import { isAbortError, toErrorMessage } from '../../../lib/errors';
import { logging } from '../../../lib/logging';
import type {
  Allocation,
  AllocationActivity,
  AllocationActivityResponse,
  Prime,
  ProtocolEvent,
} from '../../../types/allocation';
import {
  ChainLogo,
  DEFAULT_RANGE_PRESET,
  defaultTimeRange,
  PageShell,
  ProtocolLogo,
  RangePicker,
  type RangePreset,
  tableHeaderTypographyClassName,
  type TimeRange,
  TokenAddress,
} from '../../shared';
import { unindexedChainMessage } from './TabStatePanels';

type ActivityFeedProps = {
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
  chainLabels?: ChainLabelLookup;
  // External range control: provided by parent-owned top bar picker.
  externalRangePreset?: RangePreset;
  externalTimeRange?: TimeRange;
  onRangeChange?: (preset: RangePreset, range: TimeRange) => void;
};

type ActivityFilters = {
  from_timestamp?: string;
  to_timestamp?: string;
  limit?: number;
  rangePreset: RangePreset;
};

const ACTION_FILTER_OPTIONS = [
  { label: 'All actions', value: '' },
  { label: 'In', value: 'in' },
  { label: 'Out', value: 'out' },
  { label: 'Sweep', value: 'sweep' },
];

const filterFieldClassName = css({ display: 'grid', gap: '1', minWidth: 0 });
const filterLabelClassName = css({
  fontSize: 'xs',
  textTransform: 'uppercase',
  letterSpacing: '0.1em',
  color: 'text.muted',
});

function normalizeFilterValue(value: string): string | undefined {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function isSweepEvent(event: AllocationActivity): boolean {
  return event.action_type?.toLowerCase() === 'sweep';
}

function getRealTxHash(event: AllocationActivity): string | null {
  // Defensive client-side guard for stale API responses already loaded before
  // the backend nulls synthetic sweep tx_hash values. `||` also maps an empty
  // string to null so such a row cannot expand into a doomed lookup.
  return isSweepEvent(event) ? null : event.tx_hash || null;
}

function formatEventData(eventData: ProtocolEvent['event_data']): string {
  if (eventData === null) {
    return 'No event data payload.';
  }

  try {
    return JSON.stringify(eventData, null, 2);
  } catch {
    return 'Unable to serialize event payload.';
  }
}

function buildActivityEventKey(event: AllocationActivity): string {
  return [
    event.chain_id,
    event.tx_hash ?? 'no-tx',
    event.log_index ?? 'no-log-index',
    event.protocol_name ?? 'no-protocol',
    event.action_type ?? 'no-action',
    event.block_number,
    event.created_at,
  ].join(':');
}

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

function ProtocolEventCard({ event }: { event: ProtocolEvent }) {
  return (
    <div
      className={css({
        borderWidth: '1px',
        borderStyle: 'solid',
        borderColor: 'border.subtle',
        borderRadius: 'sm',
        bg: 'surface.default',
        padding: '2.5',
        display: 'grid',
        gap: '1',
      })}
    >
      <div
        className={flex({
          align: 'center',
          gap: '2',
          wrap: 'wrap',
        })}
      >
        <span
          className={css({
            fontSize: 'xs',
            color: 'text.strong',
            fontWeight: 'semibold',
          })}
        >
          {event.protocol_name}
        </span>
        <span
          className={css({
            fontSize: 'xs',
            color: 'text.muted',
          })}
        >
          •
        </span>
        <span
          className={css({
            fontSize: 'xs',
            color: 'text.default',
          })}
        >
          {event.event_name}
        </span>
        <span
          className={css({
            fontSize: 'xs',
            color: 'text.muted',
          })}
        >
          log #{event.log_index}
        </span>
        <span
          className={css({
            fontSize: 'xs',
            color: 'text.muted',
          })}
        >
          block {event.block_number} v{event.block_version}
        </span>
      </div>

      <div
        className={flex({
          gap: '2',
          wrap: 'wrap',
          align: 'center',
        })}
      >
        <span
          className={css({
            fontSize: 'xs',
            color: 'text.default',
          })}
        >
          {formatDateTime(event.created_at)}
        </span>
        <span
          className={css({
            fontSize: 'xs',
            color: 'text.muted',
          })}
        >
          •
        </span>
        <TokenAddress
          address={event.contract_address}
          chainId={event.chain_id}
        />
      </div>

      <pre
        className={css({
          margin: 0,
          borderRadius: 'sm',
          bg: 'surface.subtle',
          padding: '2',
          fontFamily: 'mono',
          fontSize: 'xs',
          color: 'text.default',
          overflowX: 'auto',
          maxHeight: '10rem',
        })}
      >
        {formatEventData(event.event_data)}
      </pre>
    </div>
  );
}

/**
 * Protocol events for one transaction, fetched when the row's detail panel
 * mounts. Expansion mounts and unmounts this component, so the effect's cleanup
 * is the whole cancellation story: collapsing a row (or unmounting the feed)
 * aborts an in-flight request, and no request is issued for a row nobody opened.
 *
 * `getTxProtocolEvents` is the dedicated endpoint; the generic
 * `getProtocolEvents` filter is the fallback for deployments that lack it,
 * gated on 404 so a real outage of the dedicated endpoint still surfaces.
 */
const FALLBACK_TX_EVENT_LIMIT = 200;

function TxProtocolEventsPanel({ txHash }: { txHash: string }) {
  const [events, setEvents] = useState<ProtocolEvent[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [truncated, setTruncated] = useState(false);

  useEffect(() => {
    const abortController = new AbortController();
    setEvents(null);
    setError(null);
    setTruncated(false);

    async function fetchTxEvents() {
      try {
        setEvents(await getTxProtocolEvents(txHash, abortController.signal));
      } catch (err) {
        if (isAbortError(err)) {
          return;
        }

        if (!(err instanceof ApiRequestError) || err.status !== 404) {
          const errorMessage = toErrorMessage(err);
          setError(errorMessage);
          logging.error('Failed to fetch tx protocol events', {
            error: err,
            errorMessage,
            txHash,
          });
          return;
        }

        try {
          const fallbackEvents = await getProtocolEvents(
            { tx_hash: txHash, limit: FALLBACK_TX_EVENT_LIMIT },
            abortController.signal,
          );
          setTruncated(fallbackEvents.length >= FALLBACK_TX_EVENT_LIMIT);
          setEvents(fallbackEvents);
        } catch (fallbackErr) {
          if (isAbortError(fallbackErr)) {
            return;
          }

          const errorMessage = toErrorMessage(fallbackErr);
          setError(errorMessage);
          logging.error('Failed to fetch tx protocol events', {
            error: err,
            fallbackError: fallbackErr,
            errorMessage,
            txHash,
          });
        }
      }
    }

    void fetchTxEvents();

    return () => abortController.abort();
  }, [txHash]);

  return (
    <div className={css({ display: 'grid', gap: '2' })}>
      <div
        className={css({
          fontSize: 'xs',
          color: 'text.strong',
          fontWeight: 'semibold',
        })}
      >
        Protocol Events For TX
      </div>
      <AsyncStateRenderer
        isLoading={events === null && error === null}
        error={error}
        isEmpty={events !== null && events.length === 0}
        loadingView={<SkeletonStack count={2} itemHeight={40} />}
        errorView={
          <span className={css({ fontSize: 'xs', color: 'text.warning' })}>
            Failed to load protocol events: {error}
          </span>
        }
        emptyView={
          <EmptyState
            title="No Protocol Events"
            description="No protocol events were indexed for this transaction."
            size="compact"
            stretch
          />
        }
      >
        {events?.map((protocolEvent) => (
          <ProtocolEventCard
            key={`${protocolEvent.tx_hash}:${protocolEvent.log_index}:${protocolEvent.protocol_name}`}
            event={protocolEvent}
          />
        ))}
        {truncated ? (
          <span className={css({ fontSize: 'xs', color: 'text.muted' })}>
            Limited to the first {FALLBACK_TX_EVENT_LIMIT} protocol events for
            this transaction.
          </span>
        ) : null}
      </AsyncStateRenderer>
    </div>
  );
}

const activityStrongCellClassName = css({
  fontSize: 'sm',
  fontWeight: 'semibold',
  color: 'text.strong',
  whiteSpace: 'nowrap',
});

const activityMetaCellClassName = css({
  fontSize: 'xs',
  color: 'text.muted',
  whiteSpace: 'nowrap',
});

const activityInlineCellClassName = css({
  display: 'inline-flex',
  alignItems: 'center',
  gap: '1.5',
  fontSize: 'sm',
  color: 'text.default',
  whiteSpace: 'nowrap',
});

// A circular glyph so action direction stays readable as a shape, not only as
// a colour.
const actionBadgeClassName = css({
  width: '6',
  height: '6',
  borderRadius: 'full',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  bg: 'surface.subtle',
  flexShrink: 0,
});

function ActivityTimeCell({ event }: { event: AllocationActivity }) {
  return (
    <div>
      <div className={activityStrongCellClassName}>
        {formatFreshnessLabel(event.created_at)}
      </div>
      <div className={activityMetaCellClassName}>
        {formatDateTime(event.created_at)}
      </div>
    </div>
  );
}

function ActivityActionCell({ event }: { event: AllocationActivity }) {
  const actionColorClassName = getActionColorClass(event.action_type);

  return (
    <div className={flex({ align: 'center', gap: '2' })}>
      <span className={cx(actionBadgeClassName, actionColorClassName)}>
        {getActionIcon(event.action_type)}
      </span>
      <span
        className={cx(
          css({
            fontSize: 'sm',
            fontWeight: 'semibold',
            textTransform: 'capitalize',
          }),
          actionColorClassName,
        )}
      >
        {event.action_type}
      </span>
    </div>
  );
}

function ActivityProtocolCell({ event }: { event: AllocationActivity }) {
  if (!event.protocol_name) {
    return <span className={activityMetaCellClassName}>—</span>;
  }

  return (
    <span className={activityInlineCellClassName}>
      <ProtocolLogo protocolName={event.protocol_name} size="4" />
      {event.protocol_name}
    </span>
  );
}

function ActivityChainCell({
  event,
  chainLabels,
}: {
  event: AllocationActivity;
  chainLabels?: ChainLabelLookup;
}) {
  const chainLabel = getChainLabel(event.chain_id, chainLabels);

  return (
    <span className={activityInlineCellClassName}>
      <ChainLogo chainId={event.chain_id} label={chainLabel} size="4" />
      {chainLabel}
    </span>
  );
}

function createActivityColumns(chainLabels?: ChainLabelLookup) {
  return defineIdentifiedColumns<AllocationActivity>(
    {
      id: 'created_at',
      header: 'Time',
      accessorFn: (event) => new Date(event.created_at).getTime(),
      cell: ({ row }) => <ActivityTimeCell event={row.original} />,
    },
    {
      id: 'token_symbol',
      header: 'Token',
      accessorFn: (event) => event.token_symbol ?? '',
      cell: ({ row }) => (
        <span className={activityStrongCellClassName}>
          {row.original.token_symbol || 'Unknown'}
        </span>
      ),
    },
    {
      id: 'action_type',
      header: 'Action',
      accessorFn: (event) => event.action_type ?? '',
      cell: ({ row }) => <ActivityActionCell event={row.original} />,
    },
    {
      id: 'protocol_name',
      header: 'Protocol',
      accessorFn: (event) => event.protocol_name ?? '',
      cell: ({ row }) => <ActivityProtocolCell event={row.original} />,
    },
    {
      id: 'tx_amount',
      header: 'Amount',
      // Sorts on the numeric value, displays the formatted one. The token
      // symbol is not repeated here — it is already this row's Token column.
      accessorFn: (event) => parseNumericValue(event.tx_amount) ?? 0,
      cell: ({ row }) => formatTokenAmount(row.original.tx_amount),
      meta: { ...numericColumnMeta },
    },
    {
      id: 'block_number',
      header: 'Block',
      accessorFn: (event) => event.block_number,
      meta: { ...numericColumnMeta },
    },
    {
      id: 'chain_id',
      header: 'Chain',
      accessorFn: (event) => getChainLabel(event.chain_id, chainLabels),
      cell: ({ row }) => (
        <ActivityChainCell event={row.original} chainLabels={chainLabels} />
      ),
    },
    {
      id: 'tx_hash',
      header: 'Tx',
      // Sweeps are internal reallocations with no real transaction;
      // `TokenAddress` renders the em-dash placeholder for a null address.
      accessorFn: (event) => getRealTxHash(event) ?? '',
      cell: ({ row }) => (
        <TokenAddress
          address={getRealTxHash(row.original)}
          chainId={row.original.chain_id}
          type="tx"
        />
      ),
      enableSorting: false,
    },
  );
}

// Row identity: expansion state, and the DataTable's per-row React key, both
// key off it. `buildActivityEventKey` alone is not guaranteed unique — a sweep
// carries neither tx_hash nor log_index, so two sweeps of different tokens in
// one block collide — and a duplicate id would fuse two rows in the row model.
// A per-key occurrence counter breaks that tie. Unlike the raw array index, it
// leaves the id of a unique event untouched when the search filter narrows the
// list, so an expanded row that survives the filter stays expanded; it cannot
// mis-target a detail panel across a refetch because the content key in front
// of it has to match as well, and only an event identical in every keyed field
// can do that. Sweeps are also exactly the rows that cannot expand (no
// transaction to inspect).
function buildActivityRowIds(
  events: readonly AllocationActivity[],
): Map<AllocationActivity, string> {
  const occurrences = new Map<string, number>();
  const ids = new Map<AllocationActivity, string>();

  for (const event of events) {
    const key = buildActivityEventKey(event);
    const seen = occurrences.get(key) ?? 0;
    occurrences.set(key, seen + 1);
    ids.set(event, seen === 0 ? key : `${key}:${seen}`);
  }

  return ids;
}

function useActivityTable(
  events: AllocationActivityResponse,
  chainLabels?: ChainLabelLookup,
): DataTableProps<AllocationActivity>['table'] {
  const columns = useMemo(
    () => createActivityColumns(chainLabels),
    [chainLabels],
  );

  const rowIds = useMemo(() => buildActivityRowIds(events), [events]);
  const getRowId = useCallback(
    (event: AllocationActivity) =>
      rowIds.get(event) ?? buildActivityEventKey(event),
    [rowIds],
  );

  return useDataTable(events, columns, {
    enableSorting: true,
    // The API returns newest-first and the header says so ("Latest activity"),
    // so the initial view is the sort the data already carries.
    defaultSorting: [{ id: 'created_at', desc: true }],
    getRowId,
    // A sweep has no transaction to inspect, so its row gets no expander.
    getRowCanExpand: (row) => getRealTxHash(row.original) !== null,
  });
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
 * Everything the activity view needs from the server and from filter state:
 * the scope guards that decide whether a request is meaningful at all, the
 * range plumbing (local in drawer mode, parent-owned in page mode), the fetch
 * lifecycle, and the client-side search narrowing on top of the fetched rows.
 */
function useAllocationActivity({
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
}: ActivityFeedProps): ActivityFeedState {
  const isPageMode = mode === 'page';
  const networkChainId = parseNetworkChainId(selectedNetwork);
  const unindexedNetwork =
    networkChainId === null ? (selectedNetwork ?? null) : null;
  const [events, setEvents] = useState<AllocationActivityResponse>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
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

  useEffect(() => {
    // Don't fetch without a scope: drawer always needs a prime; page mode needs
    // one too unless "show all primes" is on (otherwise prime_id is undefined
    // and we'd issue an unfiltered request the UI never asked for). A row on a
    // chain STL has no id for cannot be scoped at all.
    const missingScope = isPageMode
      ? (!showAllPrimes && !selectedPrime) || networkChainId === null
      : !selectedPrime || selectedReceiptToken?.chain_id === null;

    if (!isEnabled || missingScope) {
      // Emptying the rows unmounts every open detail panel, and each one aborts
      // its own in-flight tx-events request on the way out.
      setEvents([]);
      setError(null);
      setIsLoading(false);
      return;
    }

    const abortController = new AbortController();

    async function fetchActivity() {
      setIsLoading(true);
      setError(null);

      try {
        const result = await getAllocationActivity(
          requestFilters,
          abortController.signal,
        );
        setEvents(result);
      } catch (err) {
        if (isAbortError(err)) {
          return;
        }

        const errorMsg = toErrorMessage(err);
        setError(errorMsg);
        logging.error('Failed to fetch allocation activity', {
          error: err,
          errorMessage: errorMsg,
          filters: requestFilters,
        });
      } finally {
        if (!abortController.signal.aborted) {
          setIsLoading(false);
        }
      }
    }

    void fetchActivity();

    return () => abortController.abort();
  }, [
    isEnabled,
    isPageMode,
    networkChainId,
    requestFilters,
    selectedPrime,
    selectedReceiptToken?.chain_id,
    showAllPrimes,
  ]);

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

type ActivityPageHeaderProps = {
  isPageMode: boolean;
  showAllPrimes: boolean;
  latestActivityAt: string | null;
  rangePreset: RangePreset;
  range: TimeRange;
  onRangeChange: (preset: RangePreset, range: TimeRange) => void;
};

function ActivityPageHeader({
  isPageMode,
  showAllPrimes,
  latestActivityAt,
  rangePreset,
  range,
  onRangeChange,
}: ActivityPageHeaderProps) {
  return (
    <div
      className={flex({
        align: 'flex-start',
        justify: 'space-between',
        gap: { base: '3', md: '4' },
        wrap: 'wrap',
      })}
    >
      <div
        className={css({
          display: 'grid',
          gap: '1',
          minWidth: { base: '0', md: '18rem' },
          flex: '1 1 20rem',
        })}
      >
        <h1
          className={css({
            m: 0,
            fontSize: { base: '3xl', md: '4xl' },
            lineHeight: 'tight',
            color: 'text.strong',
          })}
        >
          Activities
        </h1>
        {showAllPrimes ? (
          <span className={css({ fontSize: 'sm', color: 'text.muted' })}>
            Across all primes
          </span>
        ) : null}

        {!isPageMode ? (
          <div className={css({ display: 'grid', gap: '1' })}>
            <span className={filterLabelClassName}>Time range</span>
            <RangePicker
              preset={rangePreset}
              range={range}
              onChange={onRangeChange}
            />
          </div>
        ) : null}
      </div>
      {latestActivityAt ? (
        <div
          className={css({
            display: 'flex',
            flexDirection: 'column',
            alignItems: { base: 'flex-start', md: 'flex-end' },
            gap: '0.5',
          })}
        >
          <span
            className={css({
              fontSize: 'sm',
              fontWeight: 'semibold',
              color: 'text.strong',
            })}
          >
            Latest activity {formatFreshnessLabel(latestActivityAt)}
          </span>
          <span className={css({ fontSize: 'xs', color: 'text.muted' })}>
            {formatDateTime(latestActivityAt)}
          </span>
        </div>
      ) : null}
    </div>
  );
}

type ActivityFilterBarProps = {
  actionFilter?: string;
  onActionFilterChange?: (value: string | null) => void;
  tokenFilter: string | null;
  onTokenFilterChange?: (value: string | null) => void;
  tokenOptions: string[];
  hasActiveFilters: boolean;
  onClearFilters: () => void;
};

function ActivityFilterBar({
  actionFilter,
  onActionFilterChange,
  tokenFilter,
  onTokenFilterChange,
  tokenOptions,
  hasActiveFilters,
  onClearFilters,
}: ActivityFilterBarProps) {
  return (
    <div className={css({ display: 'grid', gap: '3' })}>
      <div
        className={css({
          display: 'grid',
          gridTemplateColumns: {
            base: '1fr',
            sm: 'repeat(2, minmax(0, 1fr))',
            lg: 'repeat(4, minmax(0, 1fr))',
          },
          gap: '3',
          alignItems: 'end',
        })}
      >
        <label className={filterFieldClassName}>
          <span className={filterLabelClassName}>Action</span>
          <StyledSelect
            aria-label="Filter activity by action"
            value={actionFilter ?? ''}
            onChange={(event: ChangeEvent<HTMLSelectElement>) =>
              onActionFilterChange?.(event.target.value || null)
            }
          >
            {ACTION_FILTER_OPTIONS.map((option) => (
              <option key={option.value || 'all'} value={option.value}>
                {option.label}
              </option>
            ))}
          </StyledSelect>
        </label>
        {tokenOptions.length > 0 ? (
          <label className={filterFieldClassName}>
            <span className={filterLabelClassName}>Token</span>
            <StyledSelect
              aria-label="Filter activity by token symbol"
              value={tokenFilter ?? ''}
              onChange={(event: ChangeEvent<HTMLSelectElement>) =>
                onTokenFilterChange?.(
                  normalizeFilterValue(event.target.value) ?? null,
                )
              }
            >
              <option value="">All tokens</option>
              {tokenOptions.map((symbol) => (
                <option key={symbol} value={symbol}>
                  {symbol}
                </option>
              ))}
            </StyledSelect>
          </label>
        ) : null}
      </div>
      {hasActiveFilters ? (
        <div
          className={css({
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            gap: '3',
            fontSize: 'xs',
            color: 'text.muted',
          })}
        >
          <span>Server filters active</span>
          <button
            type="button"
            onClick={onClearFilters}
            className={css({
              h: '8',
              borderRadius: 'md',
              borderWidth: '1px',
              borderStyle: 'solid',
              borderColor: 'border.subtle',
              bg: 'surface.default',
              color: 'text.default',
              px: '3',
              fontSize: 'xs',
              cursor: 'pointer',
              _hover: { bg: 'surface.hover' },
            })}
          >
            Clear filters
          </button>
        </div>
      ) : null}
    </div>
  );
}

type ActivityTableProps = {
  table: DataTableProps<AllocationActivity>['table'];
  isLoading: boolean;
  visibleEventCount: number;
  rowLimit: number;
  emptyDescription: string;
};

function ActivityTable({
  table,
  isLoading,
  visibleEventCount,
  rowLimit,
  emptyDescription,
}: ActivityTableProps) {
  return (
    <div className={css({ display: 'grid', gap: '2' })}>
      {visibleEventCount === 0 ? (
        <EmptyState
          title="No Activity Found"
          description={emptyDescription}
          stretch
        />
      ) : (
        <div className={tableHeaderTypographyClassName}>
          <DataTable
            table={table}
            isLoading={isLoading}
            density="compact"
            renderDetailPanel={(event) => {
              const txHash = getRealTxHash(event);
              return txHash === null ? null : (
                <TxProtocolEventsPanel txHash={txHash} />
              );
            }}
          />
        </div>
      )}

      <div
        className={css({
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          gap: '3',
          px: '1',
          fontSize: 'xs',
          color: 'text.default',
        })}
      >
        <span>Showing {visibleEventCount} events</span>
        {visibleEventCount >= rowLimit ? (
          <span className={css({ color: 'text.muted' })}>
            Limited to most recent {rowLimit}
          </span>
        ) : null}
      </div>
    </div>
  );
}

type ActivityResultsProps = ActivityTableProps & {
  error: string | null;
  // Rows fetched before the search filter narrows them: only a first load with
  // nothing on screen yet shows the skeleton, a refetch keeps the current rows.
  totalEventCount: number;
  emptyDescription: string;
};

function ActivityResults({
  table,
  isLoading,
  error,
  totalEventCount,
  visibleEventCount,
  rowLimit,
  emptyDescription,
}: ActivityResultsProps) {
  return (
    <AsyncStateRenderer
      isLoading={isLoading && totalEventCount === 0}
      error={error}
      isEmpty={false}
      loadingView={<SkeletonStack count={3} />}
      errorView={
        <ErrorState
          title="Error Loading Activity"
          description="An error occurred while loading the activity feed."
          errorMessage={error ?? undefined}
          tone="critical"
          size="inline"
        />
      }
      emptyView={
        <EmptyState
          title="No Activity Found"
          description={emptyDescription}
          stretch
        />
      }
    >
      <ActivityTable
        table={table}
        isLoading={isLoading}
        visibleEventCount={visibleEventCount}
        rowLimit={rowLimit}
        emptyDescription={emptyDescription}
      />
    </AsyncStateRenderer>
  );
}

export function ActivityFeed(props: ActivityFeedProps) {
  const {
    actionFilter,
    onActionFilterChange,
    tokenFilter = null,
    onTokenFilterChange,
    isEnabled,
    selectedPrime,
    showAllPrimes = false,
    chainLabels,
    selectedReceiptToken = null,
  } = props;
  const {
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
    rowLimit,
    unindexedNetwork,
  } = useAllocationActivity(props);

  const table = useActivityTable(filteredEvents, chainLabels);

  if (!isEnabled) {
    return (
      <EmptyState
        title={isPageMode ? 'Activity Unavailable' : 'Open Activity Tab'}
        description={
          isPageMode
            ? 'Activity view is currently unavailable.'
            : 'Activity loads when the drawer is open and the Activity tab is selected.'
        }
        stretch
      />
    );
  }

  if (!isPageMode && !selectedPrime) {
    return (
      <EmptyState
        title="No Prime Selected"
        description="Select a prime to view its activity feed."
        stretch
      />
    );
  }

  const latestActivityAt = events[0]?.created_at ?? null;

  // A chain STL has no id for suppresses the request entirely, so "nothing
  // matched your filters" would be the wrong reason. The drawer reads that off
  // the selected receipt token; page mode has only the network filter.
  // Wrapped, not a bare name: the chain being unindexed is known even when it
  // cannot be named, and the message says "this chain" for that case.
  const unindexedFilter =
    selectedReceiptToken?.chain_id === null
      ? { network: selectedReceiptToken.network }
      : unindexedNetwork === null
        ? null
        : { network: unindexedNetwork };
  const emptyDescription =
    unindexedFilter === null
      ? 'No allocation activity events match your filters.'
      : unindexedChainMessage(unindexedFilter.network, 'activity');

  const activityResults = (
    <ActivityResults
      table={table}
      isLoading={isLoading}
      error={error}
      totalEventCount={events.length}
      visibleEventCount={filteredEvents.length}
      rowLimit={rowLimit}
      emptyDescription={emptyDescription}
    />
  );

  if (!isPageMode) {
    return activityResults;
  }

  return (
    <PageShell>
      <div className={css({ display: 'grid', gap: '5' })}>
        <ActivityPageHeader
          isPageMode={isPageMode}
          showAllPrimes={showAllPrimes}
          latestActivityAt={latestActivityAt}
          rangePreset={effectivePreset}
          range={effectiveRange}
          onRangeChange={updateRangePreset}
        />
        <ActivityFilterBar
          actionFilter={actionFilter}
          onActionFilterChange={onActionFilterChange}
          tokenFilter={tokenFilter}
          onTokenFilterChange={onTokenFilterChange}
          tokenOptions={uniqueTokenOptions}
          hasActiveFilters={hasActiveFilters}
          onClearFilters={clearFilters}
        />
        <div
          className={css({
            display: 'flex',
            flexDirection: 'column',
            minHeight: '24rem',
          })}
        >
          {activityResults}
        </div>
      </div>
    </PageShell>
  );
}
