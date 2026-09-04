import {
  type DataTableProps,
  defineIdentifiedColumns,
  numericColumnMeta,
  useDataTable,
} from '@archon-research/design-system';
import { useCallback, useMemo } from 'react';

import { css, cx } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import {
  type ChainLabelLookup,
  formatDateTime,
  formatFreshnessLabel,
  formatTokenAmount,
  getChainLabel,
  parseNumericValue,
} from '../../shared/lib/dashboard';
import type {
  AllocationActivity,
  AllocationActivityResponse,
} from '../../shared/types/allocation';
import { ChainLogo, ProtocolLogo, TokenAddress } from '../../shared/ui';
import { getActionColorClass, getActionIcon } from './action-styles';

function isSweepEvent(event: AllocationActivity): boolean {
  return event.action_type?.toLowerCase() === 'sweep';
}

export function getRealTxHash(event: AllocationActivity): string | null {
  // Defensive client-side guard for stale API responses already loaded before
  // the backend nulls synthetic sweep tx_hash values. `||` also maps an empty
  // string to null so such a row cannot expand into a doomed lookup.
  return isSweepEvent(event) ? null : event.tx_hash || null;
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

export function useActivityTable(
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
