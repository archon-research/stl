import {
  AsyncStateRenderer,
  EmptyState,
  SkeletonStack,
} from '@archon-research/design-system';
import { isHttpRequestError } from '@archon-research/http-client-react';
import { useQuery } from '@tanstack/react-query';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { formatDateTime } from '../../shared/lib/dashboard';
import { toQueryErrorMessage } from '../../shared/lib/errors';
import {
  FALLBACK_TX_EVENT_LIMIT,
  txProtocolEventsFallbackQuery,
  txProtocolEventsQuery,
} from '../../shared/lib/queries';
import type { ProtocolEvent } from '../../shared/types/allocation';
import { TokenAddress } from '../../shared/ui';

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
          margin: '0',
          borderRadius: 'sm',
          bg: 'surface.subtle',
          padding: '2',
          fontFamily: 'mono',
          fontSize: 'xs',
          color: 'text.default',
          overflowX: 'auto',
          maxHeight: '40',
        })}
      >
        {formatEventData(event.event_data)}
      </pre>
    </div>
  );
}

/**
 * Protocol events for one transaction, fetched when the row's detail panel
 * mounts.
 *
 * A settled transaction's decoded events do not change, so the query holds them
 * for an hour: re-expanding a row it already showed issues no request at all.
 * Expansion mounts and unmounts this component, so `gcTime` is what carries the
 * entry across a collapse.
 *
 * `txProtocolEventsQuery` is the dedicated endpoint; the generic
 * `/v1/protocol-events` filter is the fallback for deployments that lack it,
 * gated on 404 so a real outage of the dedicated endpoint still surfaces.
 */
export function TxProtocolEventsPanel({ txHash }: { txHash: string }) {
  const dedicated = useQuery(txProtocolEventsQuery(txHash));

  const isMissingEndpoint =
    isHttpRequestError(dedicated.error) && dedicated.error.status === 404;

  const fallback = useQuery({
    ...txProtocolEventsFallbackQuery(txHash),
    enabled: isMissingEndpoint,
  });

  const events = dedicated.data ?? fallback.data ?? null;
  // Only the fallback takes a row cap, so only its result can be cut short.
  const truncated =
    fallback.data !== undefined &&
    fallback.data.length >= FALLBACK_TX_EVENT_LIMIT;
  const error = isMissingEndpoint
    ? toQueryErrorMessage(fallback.error)
    : toQueryErrorMessage(dedicated.error);

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
