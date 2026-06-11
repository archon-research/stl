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
  formatDateTime,
  formatTokenAmount,
  formatFreshnessLabel,
} from '../../../lib/dashboard';
import { isAbortError, toErrorMessage } from '../../../lib/errors';
import { logging } from '../../../lib/logging';
import type {
  Allocation,
  AllocationCategory,
  AllocationActivity,
  AllocationActivityResponse,
  Prime,
  ProtocolEvent,
} from '../../../types/allocation';
import { ChainLogo, ProtocolLogo } from '../../shared';
import { TokenAddress } from '../../shared';

type ActivityFeedProps = {
  isEnabled: boolean;
  mode?: 'drawer' | 'page';
  actionFilter?: string;
  primeOptions?: Prime[];
  protocolOptions?: string[];
  selectedPrime: Prime | null;
  selectedReceiptToken?: Allocation | null;
  selectedCategory?: AllocationCategory | '';
  searchQuery?: string;
  tokenOptions?: string[];
};

type ActivityFilters = {
  prime_id?: string;
  protocol_name?: string;
  action_type?: string;
  token_symbol?: string;
  from_timestamp?: string;
  to_timestamp?: string;
  limit?: number;
};

const ACTION_FILTER_OPTIONS = [
  { label: 'All actions', value: '' },
  { label: 'In', value: 'in' },
  { label: 'Out', value: 'out' },
  { label: 'Sweep', value: 'sweep' },
];

function normalizeFilterValue(value: string): string | undefined {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function toDateTimeLocalValue(value: string | undefined): string {
  if (!value) {
    return '';
  }

  const date = new Date(value);
  const localDate = new Date(date.getTime() - date.getTimezoneOffset() * 60000);
  return localDate.toISOString().slice(0, 16);
}

function fromDateTimeLocalValue(value: string): string | undefined {
  return value ? new Date(value).toISOString() : undefined;
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
}: {
  event: AllocationActivity;
  isExpanded: boolean;
  onSelectTx: (event: AllocationActivity) => void;
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
            Chain {event.chain_id}
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
  isEnabled,
  mode = 'drawer',
  primeOptions = [],
  protocolOptions = [],
  selectedCategory = '',
  selectedPrime,
  selectedReceiptToken = null,
  searchQuery = '',
  tokenOptions = [],
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
  const [filters, setFilters] = useState<ActivityFilters>({
    prime_id: selectedPrime?.id,
    limit: 50,
  });
  const uniqueProtocolOptions = useMemo(
    () => Array.from(new Set(protocolOptions)).sort((a, b) => a.localeCompare(b)),
    [protocolOptions],
  );
  const uniqueTokenOptions = useMemo(
    () => Array.from(new Set(tokenOptions)).sort((a, b) => a.localeCompare(b)),
    [tokenOptions],
  );
  const hasActiveFilters = Boolean(
    filters.prime_id ||
    filters.protocol_name ||
    filters.action_type ||
    filters.token_symbol ||
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

  const updateFilter = (
    key: keyof ActivityFilters,
    value: string | undefined,
  ) => {
    setFilters((previous) => ({
      ...previous,
      [key]: value,
    }));
    resetTxInspectionState();
  };

  const clearFilters = () => {
    setFilters({ limit: filters.limit ?? 50 });
    resetTxInspectionState();
  };

  useEffect(() => {
    if (isPageMode) {
      return;
    }

    setFilters((previous) => ({
      ...previous,
      prime_id: selectedPrime?.id,
      token_symbol: selectedReceiptToken?.symbol,
      action_type: actionFilter,
      protocol_name: undefined,
      from_timestamp: undefined,
      to_timestamp: undefined,
      chain_id: selectedReceiptToken?.chain_id,
      limit: previous.limit ?? 50,
    }));
  }, [
    actionFilter,
    isPageMode,
    selectedCategory,
    selectedPrime?.id,
    selectedReceiptToken?.chain_id,
    selectedReceiptToken?.symbol,
  ]);

  const requestFilters = useMemo(() => {
    if (isPageMode) {
      return {
        ...filters,
        prime_id: filters.prime_id || undefined,
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
    filters,
    isPageMode,
    selectedPrime?.id,
    selectedReceiptToken?.chain_id,
    selectedReceiptToken?.symbol,
  ]);

  useEffect(() => {
    if (!isEnabled || (!isPageMode && !selectedPrime)) {
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
  }, [isEnabled, isPageMode, requestFilters, selectedPrime]);

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

  return (
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
      <div
        className={css({
          display: 'flex',
          flexDirection: 'column',
          height: '100%',
          borderRadius: 'lg',
          overflow: 'hidden',
        })}
      >
        {isPageMode ? (
          <div
            className={css({
              borderRadius: 'lg',
              border: '1px solid token(colors.surface.subtle)',
              bg: 'surface.default',
              p: '3',
              display: 'grid',
              gap: '3',
              mb: '3',
            })}
          >
          <div
            className={css({
              display: 'grid',
              gridTemplateColumns: {
                base: '1fr',
                md: 'repeat(3, minmax(0, 1fr))',
              },
              gap: '2',
              alignItems: 'end',
            })}
          >
            <StyledSelect
              aria-label="Filter activity by prime"
              value={filters.prime_id ?? ''}
              onChange={(event: ChangeEvent<HTMLSelectElement>) =>
                updateFilter('prime_id', event.target.value || undefined)
              }
            >
              <option value="">All primes</option>
              {primeOptions.map((prime) => (
                <option key={prime.id} value={prime.id}>
                  {prime.name}
                </option>
              ))}
            </StyledSelect>
            {uniqueProtocolOptions.length > 0 ? (
              <StyledSelect
                aria-label="Filter activity by protocol"
                value={filters.protocol_name ?? ''}
                onChange={(event: ChangeEvent<HTMLSelectElement>) =>
                  updateFilter(
                    'protocol_name',
                    normalizeFilterValue(event.target.value),
                  )
                }
              >
                <option value="">All protocols</option>
                {uniqueProtocolOptions.map((protocolName) => (
                  <option key={protocolName} value={protocolName}>
                    {protocolName}
                  </option>
                ))}
              </StyledSelect>
            ) : null}
            {uniqueTokenOptions.length > 0 ? (
              <StyledSelect
                aria-label="Filter activity by token symbol"
                value={filters.token_symbol ?? ''}
                onChange={(event: ChangeEvent<HTMLSelectElement>) =>
                  updateFilter(
                    'token_symbol',
                    normalizeFilterValue(event.target.value),
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
            ) : null}
          </div>

          <div
            className={css({
              display: 'grid',
              gridTemplateColumns: {
                base: '1fr',
                md: 'repeat(3, minmax(0, 1fr))',
              },
              gap: '2',
              alignItems: 'end',
            })}
          >
            <StyledSelect
              aria-label="Filter activity by action"
              value={filters.action_type ?? ''}
              onChange={(event: ChangeEvent<HTMLSelectElement>) =>
                updateFilter('action_type', event.target.value || undefined)
              }
            >
              {ACTION_FILTER_OPTIONS.map((option) => (
                <option key={option.value || 'all'} value={option.value}>
                  {option.label}
                </option>
              ))}
            </StyledSelect>
            <input
              aria-label="Filter activity from timestamp"
              type="datetime-local"
              value={toDateTimeLocalValue(filters.from_timestamp)}
              onChange={(event: ChangeEvent<HTMLInputElement>) =>
                updateFilter(
                  'from_timestamp',
                  fromDateTimeLocalValue(event.target.value),
                )
              }
              className={css({
                width: 'full',
                h: '9',
                borderRadius: 'md',
                borderWidth: '1px',
                borderStyle: 'solid',
                borderColor: 'border.subtle',
                bg: 'surface.default',
                color: 'text.default',
                px: '3',
                fontSize: 'sm',
              })}
            />
            <input
              aria-label="Filter activity to timestamp"
              type="datetime-local"
              value={toDateTimeLocalValue(filters.to_timestamp)}
              onChange={(event: ChangeEvent<HTMLInputElement>) =>
                updateFilter(
                  'to_timestamp',
                  fromDateTimeLocalValue(event.target.value),
                )
              }
              className={css({
                width: 'full',
                h: '9',
                borderRadius: 'md',
                borderWidth: '1px',
                borderStyle: 'solid',
                borderColor: 'border.subtle',
                bg: 'surface.default',
                color: 'text.default',
                px: '3',
                fontSize: 'sm',
              })}
            />
          </div>

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
            <span>
              {hasActiveFilters
                ? 'Server filters active'
                : 'Showing latest activity'}
            </span>
            {hasActiveFilters ? (
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
            ) : null}
          </div>
          </div>
        ) : null}

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
                txCacheKey !== null &&
                txEventsLoadingByHash[txCacheKey] === true;

              return (
                <div key={`${eventKey}:${idx}`}>
                  <ActivityEventRow
                    event={event}
                    isExpanded={isExpanded}
                    onSelectTx={handleSelectTx}
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

                      {!isTxLoading &&
                      !txError &&
                      txEvents &&
                      txEvents.length > 0
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
    </AsyncStateRenderer>
  );
}
