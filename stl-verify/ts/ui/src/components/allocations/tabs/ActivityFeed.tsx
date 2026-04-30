import { SkeletonStack } from '@archon-research/design-system';
import { ArrowDownRight, ArrowRightLeft, ArrowUpLeft } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { getAllocationActivity } from '../../../lib/api';
import { formatTokenAmount } from '../../../lib/dashboard';
import { isAbortError, toErrorMessage } from '../../../lib/errors';
import { logging } from '../../../lib/logging';
import type {
  AllocationActivity,
  AllocationActivityResponse,
  Prime,
} from '../../../types/allocation';
import { EmptyState, ErrorState } from '../../shared';

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

function formatTimestamp(isoString: string): string {
  const date = new Date(isoString);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  const diffHours = Math.floor(diffMs / 3600000);
  const diffDays = Math.floor(diffMs / 86400000);

  if (diffMins < 60) {
    return `${diffMins}m ago`;
  }
  if (diffHours < 24) {
    return `${diffHours}h ago`;
  }
  if (diffDays < 7) {
    return `${diffDays}d ago`;
  }

  return date.toLocaleDateString();
}

function ActivityEventRow({ event }: { event: AllocationActivity }) {
  const actionColor = getActionColor(event.action_type);
  const actionIcon = getActionIcon(event.action_type);

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
        <div className={flex({ gap: '2', align: 'center' })}>
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
              })}
            >
              {event.protocol_name}
            </span>
          ) : null}
        </div>
        <div className={flex({ gap: '2', align: 'center' })}>
          <span className={css({ fontSize: 'xs', color: 'text.default' })}>
            {formatTokenAmount(event.tx_amount)} {event.token_symbol ?? ''}
          </span>
          <span className={css({ fontSize: 'xs', color: 'text.subtle' })}>•</span>
          <span className={css({ fontSize: 'xs', color: 'text.default' })}>
            Block {event.block_number}
          </span>
          {event.tx_hash ? (
            <>
              <span className={css({ fontSize: 'xs', color: 'text.subtle' })}>•</span>
              <a
                href={`https://etherscan.io/tx/${event.tx_hash}`}
                target="_blank"
                rel="noopener noreferrer"
                className={css({
                  fontSize: 'xs',
                  color: 'interactive.accent',
                  textDecoration: 'none',
                  _hover: { textDecoration: 'underline' },
                })}
              >
                {event.tx_hash.slice(0, 8)}...
              </a>
            </>
          ) : null}
        </div>
      </div>

      <span
        className={css({
          fontSize: 'xs',
          color: 'text.subtle',
          whiteSpace: 'nowrap',
        })}
      >
        {formatTimestamp(event.created_at)}
      </span>
    </div>
  );
}

export function ActivityFeed({
  isEnabled,
  selectedPrime,
  searchQuery = '',
}: ActivityFeedProps) {
  const [events, setEvents] = useState<AllocationActivityResponse>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [filters] = useState<ActivityFilters>({
    limit: 50,
  });

  useEffect(() => {
    if (!isEnabled || !selectedPrime) {
      setEvents([]);
      setError(null);
      setIsLoading(false);
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
        event.tx_hash.toLowerCase().includes(lowerQuery),
    );
  }, [events, searchQuery]);

  if (!isEnabled) {
    return (
      <EmptyState
        title="Open Activity Tab"
        description="Activity loads when the drawer is open and the Activity tab is selected."
      />
    );
  }

  if (!selectedPrime) {
    return (
      <EmptyState
        title="No Prime Selected"
        description="Select a prime to view its activity feed."
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
        {filteredEvents.map((event, idx) => (
          <ActivityEventRow key={`${event.tx_hash}:${event.log_index}:${idx}`} event={event} />
        ))}
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
