import {
  Badge,
  KeyValueTable,
  StatusPill,
} from '@archon-research/design-system';
import { useQuery } from '@tanstack/react-query';

import { css } from '#styled-system/css';

import { asCell } from '../form/text.ts';
import { api } from '../lib/api.ts';
import type { EdgeRow, NodeRow } from '../lib/contract.ts';
import type { CurationResource } from '../schema/registry.ts';
import { type Column, DataTable } from './DataTable.tsx';
import { PageFrame, PageSection } from './PageFrame.tsx';

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

  const crumbs = [
    { label: 'Curate' },
    {
      label: resource.label,
      to: '/$resourceKey',
      params: { resourceKey: resource.key },
    },
    { label: nodeId },
  ];

  if (node.isPending) {
    return (
      <PageFrame crumbs={crumbs} title={nodeId}>
        <PageSection>
          <p className={mutedClassName}>Resolving the current row…</p>
        </PageSection>
      </PageFrame>
    );
  }

  if (node.isError) {
    return (
      <PageFrame crumbs={crumbs} title={nodeId}>
        <PageSection title="Not found">
          <p className={mutedClassName}>
            {nodeId} does not resolve to a current node. It may exist only as a
            tombstoned window, which history would still show.
          </p>
        </PageSection>
      </PageFrame>
    );
  }

  const row = node.data;

  return (
    <PageFrame
      crumbs={crumbs}
      title={<span className={monoTitleClassName}>{nodeId}</span>}
      description={`${resource.singular} · valid from ${row.valid_from} to ${row.valid_to}`}
      meta={
        <>
          <StatusPill
            name="status"
            value={row.status}
            tone={row.status === 'ACTIVE' ? 'success' : 'neutral'}
          />
          <Badge>pv {row.processing_version}</Badge>
        </>
      }
    >
      <PageSection title="Attributes">
        <KeyValueTable
          rows={Object.entries(row.attrs).map(([key, value]) => ({
            key,
            label: key,
            value: asCell(value),
            mono: typeof value !== 'boolean',
          }))}
        />
      </PageSection>

      <PageSection title="Relationships out" bleed>
        <EdgeList
          rows={outbound.data ?? []}
          otherId={(e) => e.dst_id}
          label="Relationships out"
          emptyMessage={outboundEmptyMessage(resource.recordType)}
        />
      </PageSection>

      <PageSection title="Relationships in" bleed>
        <EdgeList
          rows={inbound.data ?? []}
          otherId={(e) => e.src_id}
          label="Relationships in"
          emptyMessage="Nothing points here."
        />
      </PageSection>

      <PageSection
        title={`Appends (${history.data?.length ?? 0})`}
        description="Newest first. Within one valid window the winner is processing_version then knowledge time — never a wall clock."
        bleed
      >
        <DataTable
          label="Appends"
          rows={history.data ?? []}
          rowKey={(a) => String(a.record_id)}
          emptyMessage="No appends."
          columns={HISTORY_COLUMNS}
        />
      </PageSection>
    </PageFrame>
  );
}

const HISTORY_COLUMNS: Column<NodeRow>[] = [
  {
    key: 'record_id',
    header: 'Record',
    mono: true,
    numeric: true,
    render: (a) => a.record_id,
    sortValue: (a) => a.record_id,
  },
  {
    key: 'processing_version',
    header: 'PV',
    numeric: true,
    render: (a) => a.processing_version,
    sortValue: (a) => a.processing_version,
  },
  {
    key: 'valid',
    header: 'Valid',
    render: (a) => `${a.valid_from} → ${a.valid_to}`,
    sortValue: (a) => a.valid_from,
  },
  {
    key: 'ingested_at',
    header: 'Ingested',
    mono: true,
    render: (a) => a.ingested_at.slice(0, 19),
    sortValue: (a) => a.ingested_at,
  },
  {
    key: 'actor',
    header: 'Actor',
    mono: true,
    render: (a) => a.actor,
    sortValue: (a) => a.actor,
  },
  {
    key: 'reason',
    header: 'Reason',
    render: (a) =>
      a.approved_by === null
        ? a.change_reason_code
        : `${a.change_reason_code} · ok: ${a.approved_by}`,
    sortValue: (a) => a.change_reason_code,
  },
  {
    key: 'supersedes_record_id',
    header: 'Supersedes',
    mono: true,
    numeric: true,
    render: (a) => a.supersedes_record_id ?? '—',
    sortValue: (a) => a.supersedes_record_id,
  },
  {
    key: 'content_hash',
    header: 'Hash',
    mono: true,
    render: (a) => a.content_hash,
    sortValue: (a) => a.content_hash,
  },
];

/**
 * What "no edges out" means depends on the kind.
 *
 * The message named securities regardless, so an entity with no outbound edges
 * was told it had no classification, issuer or underlying — three things an
 * entity never has.
 */
function outboundEmptyMessage(recordType: string | undefined): string {
  switch (recordType) {
    case 'SECURITY':
      return 'No edges out: no classification, no issuer and no underlying.';
    case 'ENTITY':
      return 'No edges out: no parent, no affiliates and no governing rule set.';
    case 'CONCEPT':
      return 'No edges out: this concept sits at the root of its taxonomy.';
    default:
      return 'No edges out.';
  }
}

function EdgeList({
  rows,
  otherId,
  label,
  emptyMessage,
}: {
  rows: readonly EdgeRow[];
  otherId: (edge: EdgeRow) => string;
  label: string;
  emptyMessage: string;
}) {
  const columns: Column<EdgeRow>[] = [
    {
      key: 'rel_type',
      header: 'Type',
      render: (e) => e.rel_type,
      sortValue: (e) => e.rel_type,
    },
    {
      key: 'other',
      header: 'Node',
      mono: true,
      render: (e) => otherId(e),
      sortValue: (e) => otherId(e),
    },
    {
      key: 'weight',
      header: 'Weight',
      mono: true,
      numeric: true,
      render: (e) =>
        e.rel_weight === null ? '—' : `${e.rel_weight} ${e.weight_basis ?? ''}`,
      sortValue: (e) => (e.rel_weight === null ? null : Number(e.rel_weight)),
    },
    {
      key: 'window',
      header: 'Window',
      render: (e) => `${e.valid_from} → ${e.valid_to}`,
      sortValue: (e) => e.valid_from,
    },
  ];

  return (
    <DataTable
      label={label}
      columns={columns}
      rows={rows}
      rowKey={(e) => `${e.edge_id}:${e.record_id}`}
      emptyMessage={emptyMessage}
    />
  );
}

const monoTitleClassName = css({ fontFamily: 'mono', letterSpacing: 'normal' });
const mutedClassName = css({
  fontSize: 'sm',
  color: 'text.muted',
  margin: '0',
});
