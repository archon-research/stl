import {
  AsyncStateRenderer,
  EmptyState,
  ErrorState,
  SkeletonStack,
  StyledSelect,
} from '@archon-research/design-system';
import { ArrowDownRight, ArrowRightLeft, ArrowUpLeft } from 'lucide-react';
import { type ChangeEvent, useEffect, useMemo, useRef, useState } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import {
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
  type TimeRange,
  TokenAddress,
} from '../../shared';

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
  // the backend nulls synthetic sweep tx_hash values.
  return isSweepEvent(event) ? null : (event.tx_hash ?? null);
}

function getActionIcon(actionType: string | null | undefined) {
  switch (actionType?.toLowerCase()) {
    case 'in':
      return <ArrowDownRight className={css({ width: '4', height: '4' })} />;
    case 'out':
      return <ArrowUpLeft className={css({ width: '4', height: '4' })} />;
    case 'sweep':
      return <ArrowRightLeft className={css({ width: '4', height: '4' })} />;
    default:
      return null;
  }
}

function getActionColor(actionType: string | null | undefined): string {
  switch (actionType?.toLowerCase()) {
    case 'in':
      return 'text.success';
    case 'out':
      return 'text.warning';
    case 'sweep':
      return 'text.interactive';
    default:
      return 'text.default';
  }
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

function buildTxCacheKey(txHash: string, chainId: number): string {
  return `${chainId}:${txHash.toLowerCase()}`;
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
            color: 'text.subtle',
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
            color: 'text.subtle',
          })}
        >
          log #{event.log_index}
        </span>
        <span
          className={css({
            fontSize: 'xs',
            color: 'text.subtle',
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
            color: 'text.subtle',
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

function ActivityEventRow({
  event,
  isExpanded,
  onSelectTx,
  chainLabels,
}: {
  event: AllocationActivity;
  isExpanded: boolean;
  onSelectTx: (event: AllocationActivity) => void;
  chainLabels?: ChainLabelLookup;
}) {
  const actionColor = getActionColor(event.action_type);
  const actionIcon = getActionIcon(event.action_type);
  const txHash = getRealTxHash(event);

  return (
    <div
      className={css({
        padding: '3',
        borderBottom: '1px solid token(colors.surface.subtle)',
        display: 'flex',
        alignItems: 'center',
        gap: '3',
        _hover: {
          bg: 'surface.subtle',
        },
      })}
    >
      <div
        className={css({
          width: '8',
          height: '8',
          borderRadius: 'full',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          bg: 'surface.subtle',
          color: actionColor,
          flexShrink: 0,
        })}
      >
        {actionIcon}
      </div>

      <div className={flex({ direction: 'column', gap: '1', flex: 1 })}>
        <div className={flex({ gap: '2', align: 'center', wrap: 'wrap' })}>
          <span
            className={css({
              fontSize: 'sm',
              fontWeight: 'semibold',
              color: 'text.strong',
            })}
          >
            {event.token_symbol || 'Unknown'}
          </span>
          <span
            className={css({
              fontSize: 'sm',
              color: actionColor,
              fontWeight: 'semibold',
              textTransform: 'capitalize',
            })}
          >
            {event.action_type}
          </span>
          {event.protocol_name ? (
            <span
              className={css({
                fontSize: 'xs',
                color: 'text.default',
                bg: 'surface.subtle',
                padding: '1 2',
                borderRadius: 'md',
                display: 'inline-flex',
                alignItems: 'center',
                gap: '1',
                whiteSpace: 'nowrap',
              })}
            >
              <ProtocolLogo protocolName={event.protocol_name} size="4" />
              {event.protocol_name}
            </span>
          ) : null}
        </div>
        <div className={flex({ gap: '2', align: 'center', wrap: 'wrap' })}>
          <span className={css({ fontSize: 'xs', color: 'text.default' })}>
            {formatTokenAmount(event.tx_amount)} {event.token_symbol ?? ''}
          </span>
          <span className={css({ fontSize: 'xs', color: 'text.subtle' })}>
            •
          </span>
          <span className={css({ fontSize: 'xs', color: 'text.default' })}>
            Block {event.block_number}
          </span>
          <span className={css({ fontSize: 'xs', color: 'text.subtle' })}>
            •
          </span>
          <span
            className={css({
              display: 'inline-flex',
              alignItems: 'center',
              gap: '1',
              fontSize: 'xs',
              color: 'text.default',
              whiteSpace: 'nowrap',
            })}
          >
            <ChainLogo chainId={event.chain_id} size="4" />
            {getChainLabel(event.chain_id, chainLabels)}
          </span>
          {txHash ? (
            <>
              <span className={css({ fontSize: 'xs', color: 'text.subtle' })}>
                •
              </span>
              <TokenAddress
                address={txHash}
                chainId={event.chain_id}
                type="tx"
              />
            </>
          ) : null}
        </div>
      </div>

      <span
        className={css({
          fontSize: 'xs',
          color: 'text.subtle',
          whiteSpace: 'nowrap',
          textAlign: 'right',
        })}
      >
        {formatFreshnessLabel(event.created_at)}
        {txHash ? (
          <button
            type="button"
            onClick={() => onSelectTx(event)}
            className={css({
              display: 'block',
              mt: '0.5',
              fontSize: '2xs',
              color: 'interactive.accent',
              bg: 'transparent',
              border: 'none',
              cursor: 'pointer',
            })}
          >
            {isExpanded ? 'Hide tx events' : 'Inspect tx events'}
          </button>
        ) : null}
      </span>
    </div>
  );
}

export function ActivityFeed({
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
  chainLabels,
  externalRangePreset,
  externalTimeRange,
  onRangeChange: onExternalRangeChange,
}: ActivityFeedProps) {
  const isPageMode = mode === 'page';
  const txRequestControllersRef = useRef<Record<string, AbortController>>({});

  const [events, setEvents] = useState<AllocationActivityResponse>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedEventKey, setSelectedEventKey] = useState<string | null>(null);
  const [txEventsByHash, setTxEventsByHash] = useState<
    Record<string, ProtocolEvent[]>
  >({});
  const [txEventErrorsByHash, setTxEventErrorsByHash] = useState<
    Record<string, string>
  >({});
  const [txEventsLoadingByHash, setTxEventsLoadingByHash] = useState<
    Record<string, boolean>
  >({});
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
  // Page mode: action/token come from controlled props (URL-backed); the date
  // range stays local. hasActiveFilters drives the "clear" affordance.
  const hasActiveFilters = Boolean(
    actionFilter ||
    tokenFilter ||
    filters.from_timestamp ||
    filters.to_timestamp,
  );

  const resetTxInspectionState = () => {
    Object.values(txRequestControllersRef.current).forEach((controller) => {
      controller.abort();
    });
    txRequestControllersRef.current = {};
    setSelectedEventKey(null);
    setTxEventsByHash({});
    setTxEventErrorsByHash({});
    setTxEventsLoadingByHash({});
  };

  const updateActionFilter = (value: string | null) => {
    onActionFilterChange?.(value);
    resetTxInspectionState();
  };

  const updateTokenFilter = (value: string | null) => {
    onTokenFilterChange?.(value);
    resetTxInspectionState();
  };

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
    resetTxInspectionState();
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
    resetTxInspectionState();
  };

  const requestFilters = useMemo(() => {
    if (isPageMode) {
      const parsedChainId =
        selectedNetwork && selectedNetwork.length > 0
          ? Number(selectedNetwork)
          : undefined;

      return {
        prime_id: showAllPrimes ? undefined : (selectedPrime?.id ?? undefined),
        chain_id:
          parsedChainId && Number.isFinite(parsedChainId)
            ? parsedChainId
            : undefined,
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
      chain_id: selectedReceiptToken?.chain_id,
      token_symbol: selectedReceiptToken?.symbol,
      action_type: actionFilter,
      limit: filters.limit ?? 50,
    };
  }, [
    actionFilter,
    effectiveRange,
    filters,
    isPageMode,
    selectedNetwork,
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
    // and we'd issue an unfiltered request the UI never asked for).
    const missingScope = isPageMode
      ? !showAllPrimes && !selectedPrime
      : !selectedPrime;

    if (!isEnabled || missingScope) {
      Object.values(txRequestControllersRef.current).forEach((controller) => {
        controller.abort();
      });
      txRequestControllersRef.current = {};
      setEvents([]);
      setError(null);
      setIsLoading(false);
      setSelectedEventKey(null);
      setTxEventsByHash({});
      setTxEventErrorsByHash({});
      setTxEventsLoadingByHash({});
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
  }, [isEnabled, isPageMode, requestFilters, selectedPrime, showAllPrimes]);

  useEffect(() => {
    return () => {
      Object.values(txRequestControllersRef.current).forEach((controller) => {
        controller.abort();
      });
      txRequestControllersRef.current = {};
    };
  }, []);

  const handleSelectTx = (event: AllocationActivity) => {
    const txHash = getRealTxHash(event);

    if (!txHash) {
      return;
    }

    const eventKey = buildActivityEventKey(event);
    const txCacheKey = buildTxCacheKey(txHash, event.chain_id);

    if (selectedEventKey === eventKey) {
      txRequestControllersRef.current[txCacheKey]?.abort();
      delete txRequestControllersRef.current[txCacheKey];
      setTxEventsLoadingByHash((previous) => {
        if (!previous[txCacheKey]) {
          return previous;
        }

        const { [txCacheKey]: _, ...rest } = previous;
        return rest;
      });
      setSelectedEventKey(null);
      return;
    }

    setSelectedEventKey(eventKey);

    if (txEventsByHash[txCacheKey] || txEventErrorsByHash[txCacheKey]) {
      return;
    }

    if (txEventsLoadingByHash[txCacheKey]) {
      return;
    }

    setTxEventsLoadingByHash((previous) => ({
      ...previous,
      [txCacheKey]: true,
    }));
    setTxEventErrorsByHash((previous) => {
      if (!previous[txCacheKey]) {
        return previous;
      }

      const { [txCacheKey]: _, ...rest } = previous;
      return rest;
    });

    const abortController = new AbortController();
    txRequestControllersRef.current[txCacheKey] = abortController;

    void getTxProtocolEvents(txHash, abortController.signal)
      .then((result) => {
        setTxEventsByHash((previous) => ({
          ...previous,
          [txCacheKey]: result,
        }));
      })
      .catch(async (err) => {
        if (isAbortError(err)) {
          return;
        }

        try {
          const fallbackResult = await getProtocolEvents(
            {
              tx_hash: txHash,
              limit: 200,
            },
            abortController.signal,
          );

          setTxEventsByHash((previous) => ({
            ...previous,
            [txCacheKey]: fallbackResult,
          }));
          return;
        } catch (fallbackErr) {
          if (isAbortError(fallbackErr)) {
            return;
          }

          const errorMsg = toErrorMessage(fallbackErr);
          setTxEventErrorsByHash((previous) => ({
            ...previous,
            [txCacheKey]: errorMsg,
          }));
          logging.error('Failed to fetch tx protocol events', {
            error: err,
            fallbackError: fallbackErr,
            errorMessage: errorMsg,
            txHash,
          });
        }
      })
      .finally(() => {
        if (txRequestControllersRef.current[txCacheKey] === abortController) {
          delete txRequestControllersRef.current[txCacheKey];
        }

        setTxEventsLoadingByHash((previous) => {
          if (!previous[txCacheKey]) {
            return previous;
          }

          const { [txCacheKey]: _, ...rest } = previous;
          return rest;
        });
      });
  };

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

  const activityHeader = (
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
              preset={effectivePreset}
              range={effectiveRange}
              onChange={updateRangePreset}
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

  const activityFilters = (
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
              updateActionFilter(event.target.value || null)
            }
          >
            {ACTION_FILTER_OPTIONS.map((option) => (
              <option key={option.value || 'all'} value={option.value}>
                {option.label}
              </option>
            ))}
          </StyledSelect>
        </label>
        {uniqueTokenOptions.length > 0 ? (
          <label className={filterFieldClassName}>
            <span className={filterLabelClassName}>Token</span>
            <StyledSelect
              aria-label="Filter activity by token symbol"
              value={tokenFilter ?? ''}
              onChange={(event: ChangeEvent<HTMLSelectElement>) =>
                updateTokenFilter(
                  normalizeFilterValue(event.target.value) ?? null,
                )
              }
            >
              <option value="">All tokens</option>
              {uniqueTokenOptions.map((symbol) => (
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
            color: 'text.subtle',
          })}
        >
          <span>Server filters active</span>
          <button
            type="button"
            onClick={clearFilters}
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
              _hover: { bg: 'interactive.hover' },
            })}
          >
            Clear filters
          </button>
        </div>
      ) : null}
    </div>
  );

  const feedBody = (
    <div
      className={css({
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        borderRadius: 'lg',
        overflow: 'hidden',
      })}
    >
      {filteredEvents.length === 0 ? (
        <EmptyState
          title="No Activity Found"
          description="No allocation activity events match your filters."
          stretch
        />
      ) : null}

      {filteredEvents.length > 0 ? (
        <div
          className={css({
            flex: 1,
            overflowY: 'auto',
            borderRadius: 'lg',
            border: '1px solid token(colors.surface.subtle)',
            bg: 'surface.default',
          })}
        >
          {filteredEvents.map((event, idx) => {
            const eventKey = buildActivityEventKey(event);
            const txHash = getRealTxHash(event);
            const txCacheKey = txHash
              ? buildTxCacheKey(txHash, event.chain_id)
              : null;
            const isExpanded = selectedEventKey === eventKey;
            const txEvents = txCacheKey
              ? txEventsByHash[txCacheKey]
              : undefined;
            const txError = txCacheKey
              ? txEventErrorsByHash[txCacheKey]
              : undefined;
            const isTxLoading =
              txCacheKey !== null && txEventsLoadingByHash[txCacheKey] === true;

            return (
              <div key={`${eventKey}:${idx}`}>
                <ActivityEventRow
                  event={event}
                  isExpanded={isExpanded}
                  onSelectTx={handleSelectTx}
                  chainLabels={chainLabels}
                />

                {isExpanded && txHash ? (
                  <div
                    className={css({
                      marginX: '3',
                      marginBottom: '3',
                      borderWidth: '1px',
                      borderStyle: 'solid',
                      borderColor: 'border.subtle',
                      borderRadius: 'md',
                      bg: 'surface.subtle',
                      padding: '3',
                      display: 'grid',
                      gap: '2',
                    })}
                  >
                    <div
                      className={css({
                        fontSize: 'xs',
                        color: 'text.strong',
                        fontWeight: 'semibold',
                      })}
                    >
                      Protocol Events For TX
                    </div>

                    {isTxLoading ? (
                      <span
                        className={css({
                          fontSize: 'xs',
                          color: 'text.default',
                        })}
                      >
                        Loading protocol events...
                      </span>
                    ) : null}

                    {!isTxLoading && txError ? (
                      <span
                        className={css({
                          fontSize: 'xs',
                          color: 'text.warning',
                        })}
                      >
                        Failed to load protocol events: {txError}
                      </span>
                    ) : null}

                    {!isTxLoading &&
                    !txError &&
                    txEvents &&
                    txEvents.length === 0 ? (
                      <EmptyState
                        title="No Protocol Events"
                        description="No protocol events were indexed for this transaction."
                        size="compact"
                        stretch
                      />
                    ) : null}

                    {!isTxLoading && !txError && txEvents && txEvents.length > 0
                      ? txEvents.map((protocolEvent) => (
                          <ProtocolEventCard
                            key={`${protocolEvent.tx_hash}:${protocolEvent.log_index}:${protocolEvent.protocol_name}`}
                            event={protocolEvent}
                          />
                        ))
                      : null}
                  </div>
                ) : null}
              </div>
            );
          })}
        </div>
      ) : null}

      <div
        className={css({
          padding: '3',
          borderTop: '1px solid token(colors.surface.subtle)',
          bg: 'surface.subtle',
          fontSize: 'xs',
          color: 'text.default',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
        })}
      >
        <span>Showing {filteredEvents.length} events</span>
        {filteredEvents.length >= (filters.limit || 50) ? (
          <span className={css({ color: 'text.subtle' })}>
            Limited to most recent {filters.limit || 50}
          </span>
        ) : null}
      </div>
    </div>
  );

  const feedArea = (
    <AsyncStateRenderer
      isLoading={isLoading && events.length === 0}
      error={error}
      isEmpty={false}
      loadingView={<SkeletonStack count={3} />}
      errorView={
        <ErrorState
          title="Error Loading Activity"
          description="An error occurred while loading the activity feed."
          errorMessage={error ?? undefined}
        />
      }
      emptyView={
        <EmptyState
          title="No Activity Found"
          description="No allocation activity events match your filters."
          stretch
        />
      }
    >
      {feedBody}
    </AsyncStateRenderer>
  );

  if (!isPageMode) {
    return feedArea;
  }

  return (
    <PageShell>
      <div className={css({ display: 'grid', gap: '5' })}>
        {activityHeader}
        {activityFilters}
        <div
          className={css({
            display: 'flex',
            flexDirection: 'column',
            minHeight: '24rem',
          })}
        >
          {feedArea}
        </div>
      </div>
    </PageShell>
  );
}
