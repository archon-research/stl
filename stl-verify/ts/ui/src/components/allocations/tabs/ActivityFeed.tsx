import {
  EmptyState,
  ErrorState,
  SkeletonStack,
} from '@archon-research/design-system';
import { ArrowDownRight, ArrowRightLeft, ArrowUpLeft } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';

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
  AllocationActivity,
  AllocationActivityResponse,
  Prime,
  ProtocolEvent,
} from '../../../types/allocation';
import { ChainLogo, ProtocolLogo } from '../../shared';
import { TokenAddress } from '../../shared';

type ActivityFeedProps = {
  isEnabled: boolean;
  selectedPrime: Prime | null;
  searchQuery?: string;
};

type ActivityFilters = {
  protocol_name?: string;
  action_type?: string;
  from_timestamp?: string;
  to_timestamp?: string;
  limit?: number;
};

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
  const txHash = event.tx_hash;

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
          {event.tx_hash ? (
            <>
              <span className={css({ fontSize: 'xs', color: 'text.subtle' })}>
                •
              </span>
              <TokenAddress
                address={event.tx_hash}
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
  isEnabled,
  selectedPrime,
  searchQuery = '',
}: ActivityFeedProps) {
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
  const [filters] = useState<ActivityFilters>({
    limit: 50,
  });

  useEffect(() => {
    if (!isEnabled || !selectedPrime) {
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

    const primeId = selectedPrime.id;
    const abortController = new AbortController();

    async function fetchActivity() {
      setIsLoading(true);
      setError(null);

      try {
        const result = await getAllocationActivity(
          {
            prime_id: primeId,
            ...filters,
          },
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
          primeId,
          filters,
        });
      } finally {
        setIsLoading(false);
      }
    }

    void fetchActivity();

    return () => abortController.abort();
  }, [filters, isEnabled, selectedPrime]);

  useEffect(() => {
    return () => {
      Object.values(txRequestControllersRef.current).forEach((controller) => {
        controller.abort();
      });
      txRequestControllersRef.current = {};
    };
  }, []);

  const handleSelectTx = (event: AllocationActivity) => {
    if (!event.tx_hash) {
      return;
    }

    const eventKey = buildActivityEventKey(event);
    const txCacheKey = buildTxCacheKey(event.tx_hash, event.chain_id);

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

    void getTxProtocolEvents(event.tx_hash, abortController.signal)
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
              tx_hash: event.tx_hash ?? undefined,
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
            txHash: event.tx_hash,
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
        event.tx_hash?.toLowerCase().includes(lowerQuery),
    );
  }, [events, searchQuery]);

  if (!isEnabled) {
    return (
      <EmptyState
        title="Open Activity Tab"
        description="Activity loads when the drawer is open and the Activity tab is selected."
        stretch
      />
    );
  }

  if (!selectedPrime) {
    return (
      <EmptyState
        title="No Prime Selected"
        description="Select a prime to view its activity feed."
        stretch
      />
    );
  }

  if (isLoading && events.length === 0) {
    return <SkeletonStack count={3} />;
  }

  if (error) {
    return (
      <ErrorState
        title="Error Loading Activity"
        description="An error occurred while loading the activity feed."
        errorMessage={error}
      />
    );
  }

  if (filteredEvents.length === 0) {
    return (
      <EmptyState
        title="No Activity Found"
        description="No allocation activity events match your filters."
        stretch
      />
    );
  }

  return (
    <div
      className={css({
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        borderRadius: 'lg',
        overflow: 'hidden',
      })}
    >
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
          const txHash = event.tx_hash;
          const txCacheKey = txHash
            ? buildTxCacheKey(txHash, event.chain_id)
            : null;
          const isExpanded = selectedEventKey === eventKey;
          const txEvents = txCacheKey ? txEventsByHash[txCacheKey] : undefined;
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
                      className={css({ fontSize: 'xs', color: 'text.default' })}
                    >
                      Loading protocol events...
                    </span>
                  ) : null}

                  {!isTxLoading && txError ? (
                    <span
                      className={css({ fontSize: 'xs', color: 'text.warning' })}
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
}
