import {
  Badge,
  KeyValueTable,
  Panel,
  StatusPill,
} from '@archon-research/design-system';
import { useQuery } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';

import { css } from '#styled-system/css';

import { asCell } from '../form/text.ts';
import { api } from '../lib/api.ts';
import type { CurationResource } from '../schema/registry.ts';

/**
 * One node: its current row, its edges, and every append behind it.
 *
 * The history panel is the reason this view exists rather than an edit form. In
 * a bitemporal, append-only store the interesting question about a record is not
 * "what does it say" but "what has it said, when was that true, and when did we
 * learn it" — and a UI that only ever showed the current row would make the
 * second clock invisible, which is the failure ADR-0007 §4 is built to prevent.
 */
export function NodeDetailView({
  resource,
  nodeId,
}: {
  resource: CurationResource;
  nodeId: string;
}) {
  const node = useQuery(
    api.queryOptions(
      'get',
      '/v1/secstore/nodes/{node_id}',
      { params: { path: { node_id: nodeId } } },
      { tags: ['nodes'] },
    ),
  );

  const history = useQuery(
    api.queryOptions(
      'get',
      '/v1/secstore/nodes/{node_id}/history',
      { params: { path: { node_id: nodeId } } },
      { tags: ['nodes'] },
    ),
  );

  const outbound = useQuery(
    api.queryOptions(
      'get',
      '/v1/secstore/edges',
      { params: { query: { src_id: nodeId } } },
      { tags: ['edges'] },
    ),
  );

  const inbound = useQuery(
    api.queryOptions(
      'get',
      '/v1/secstore/edges',
      { params: { query: { dst_id: nodeId } } },
      { tags: ['edges'] },
    ),
  );

  if (node.isPending) {
    return <Panel title="Loading">Resolving the current row…</Panel>;
  }

  if (node.isError) {
    return (
      <Panel title="Not found">
        {nodeId} does not resolve to a current node. It may exist only as a
        tombstoned window, which history would still show.
      </Panel>
    );
  }

  const row = node.data;

  return (
    <div className={stack}>
      <div className={headerRow}>
        <div>
          <h1 className={heading}>{nodeId}</h1>
          <p className={subheading}>
            {resource.singular} · valid from {row.valid_from} to {row.valid_to}
          </p>
        </div>
        <div
          className={css({ display: 'flex', gap: '2', alignItems: 'center' })}
        >
          <StatusPill
            name="status"
            value={row.status}
            tone={row.status === 'ACTIVE' ? 'success' : 'neutral'}
          />
          <Badge>pv {row.processing_version}</Badge>
        </div>
      </div>

      <Panel title="Attributes" density="compact">
        <KeyValueTable
          rows={Object.entries(row.attrs).map(([key, value]) => ({
            key,
            label: key,
            value: asCell(value),
            mono: typeof value !== 'boolean',
          }))}
        />
      </Panel>

      <Panel title="Relationships out" density="compact">
        <EdgeList
          rows={(outbound.data ?? []).map((e) => ({
            key: `${e.edge_id}:${e.record_id}`,
            relType: e.rel_type,
            other: e.dst_id,
            weight: e.rel_weight,
            basis: e.weight_basis,
            from: e.valid_from,
            to: e.valid_to,
          }))}
          emptyMessage="No edges out. For a security that means no classification, no issuer and no underlying."
        />
      </Panel>

      <Panel title="Relationships in" density="compact">
        <EdgeList
          rows={(inbound.data ?? []).map((e) => ({
            key: `${e.edge_id}:${e.record_id}`,
            relType: e.rel_type,
            other: e.src_id,
            weight: e.rel_weight,
            basis: e.weight_basis,
            from: e.valid_from,
            to: e.valid_to,
          }))}
          emptyMessage="Nothing points here."
        />
      </Panel>

      <Panel title={`Appends (${history.data?.length ?? 0})`} density="compact">
        <p className={note}>
          Newest first. Within one valid window the winner is
          <code> processing_version</code> then knowledge time — never a wall
          clock.
        </p>
        <div className={tableWrap}>
          <table className={table}>
            <thead>
              <tr>
                {[
                  'record_id',
                  'pv',
                  'valid',
                  'ingested',
                  'actor',
                  'reason',
                  'supersedes',
                  'hash',
                ].map((column) => (
                  <th key={column} className={th}>
                    {column}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {(history.data ?? []).map((append) => (
                <tr key={append.record_id} className={tr}>
                  <td className={`${td} ${mono}`}>{append.record_id}</td>
                  <td className={td}>{append.processing_version}</td>
                  <td className={td}>
                    {append.valid_from} → {append.valid_to}
                  </td>
                  <td className={`${td} ${mono}`}>
                    {append.ingested_at.slice(0, 19)}
                  </td>
                  <td className={`${td} ${mono}`}>{append.actor}</td>
                  <td className={td}>
                    {append.change_reason_code}
                    {append.approved_by !== null &&
                      ` · ok: ${append.approved_by}`}
                  </td>
                  <td className={`${td} ${mono}`}>
                    {append.supersedes_record_id ?? '—'}
                  </td>
                  <td className={`${td} ${mono}`}>{append.content_hash}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Panel>

      <Link
        to="/$resourceKey"
        params={{ resourceKey: resource.key }}
        className={css({ fontSize: 'sm' })}
      >
        ← {resource.label}
      </Link>
    </div>
  );
}

function EdgeList({
  rows,
  emptyMessage,
}: {
  rows: readonly {
    key: string;
    relType: string;
    other: string;
    weight: string | null;
    basis: string | null;
    from: string;
    to: string;
  }[];
  emptyMessage: string;
}) {
  if (rows.length === 0) {
    return <p className={note}>{emptyMessage}</p>;
  }

  return (
    <div className={tableWrap}>
      <table className={table}>
        <tbody>
          {rows.map((row) => (
            <tr key={row.key} className={tr}>
              <td className={td}>{row.relType}</td>
              <td className={`${td} ${mono}`}>{row.other}</td>
              <td className={`${td} ${mono}`}>
                {row.weight === null ? '' : `${row.weight} ${row.basis ?? ''}`}
              </td>
              <td className={td}>
                {row.from} → {row.to}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

const stack = css({
  display: 'flex',
  flexDirection: 'column',
  gap: '4',
  maxWidth: '7xl',
});
const headerRow = css({
  display: 'flex',
  alignItems: 'flex-start',
  justifyContent: 'space-between',
  gap: '4',
});
const heading = css({
  fontSize: 'xl',
  fontWeight: 'semibold',
  fontFamily: 'mono',
});
const subheading = css({ fontSize: 'sm', color: 'text.muted' });
const note = css({ fontSize: 'xs', color: 'text.muted' });
const tableWrap = css({ overflowX: 'auto' });
const table = css({
  width: 'full',
  borderCollapse: 'collapse',
  fontSize: 'sm',
});
const th = css({
  textAlign: 'left',
  padding: '2',
  fontSize: '2xs',
  textTransform: 'uppercase',
  color: 'text.muted',
  borderBottomWidth: '1px',
  borderBottomStyle: 'solid',
  borderColor: 'border.subtle',
});
const tr = css({ _hover: { bg: 'surface.subtle' } });
const td = css({
  padding: '2',
  borderBottomWidth: '1px',
  borderBottomStyle: 'solid',
  borderColor: 'border.subtle',
});
const mono = css({ fontFamily: 'mono', fontSize: 'xs' });
