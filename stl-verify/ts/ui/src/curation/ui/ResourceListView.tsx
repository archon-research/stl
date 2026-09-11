import { Button, SearchInput } from '@archon-research/design-system';
import { useQuery } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { useState } from 'react';

import { css } from '#styled-system/css';

import { asCell } from '../form/text.ts';
import { api } from '../lib/api.ts';
import type { EdgeRow, NodeRow } from '../lib/contract.ts';
import type { CurationResource } from '../schema/registry.ts';
import { type Column, DataTable } from './DataTable.tsx';
import { PageFrame, PageSection } from './PageFrame.tsx';

/**
 * One list view for every resource, driven by the registry's column set.
 *
 * The reads differ by store, which is why the query is a small branch rather
 * than one generic call: nodes, edges and the two registers are four endpoints
 * with four shapes, and pretending otherwise would mean inventing a shared
 * response envelope the API does not have.
 */
export function ResourceListView({ resource }: { resource: CurationResource }) {
  const [term, setTerm] = useState('');

  return (
    <PageFrame
      crumbs={[{ label: 'Curate' }, { label: resource.label }]}
      title={resource.label}
      description={resource.description}
      actions={
        <Link to="/$resourceKey/new" params={{ resourceKey: resource.key }}>
          <Button emphasis="solid" colorPalette="blue">
            New {resource.singular}
          </Button>
        </Link>
      }
    >
      <div className={filterClassName}>
        <SearchInput
          value={term}
          onValueChange={setTerm}
          placeholder="Filter"
        />
      </div>

      <PageSection bleed>
        {resource.store === 'node' && (
          <NodeRows resource={resource} term={term} />
        )}
        {resource.store === 'edge' && <EdgeRows term={term} />}
        {resource.store === 'instrument_register' && (
          <InstrumentRows term={term} />
        )}
        {resource.store === 'alias_register' && <AliasRows term={term} />}
      </PageSection>
    </PageFrame>
  );
}

function NodeRows({
  resource,
  term,
}: {
  resource: CurationResource;
  term: string;
}) {
  const rows = useQuery(
    api.queryOptions(
      'get',
      '/v1/secstore/nodes',
      {
        params: {
          query: {
            ...(resource.recordType !== undefined && {
              record_type: resource.recordType,
            }),
            ...(term !== '' && { q: term }),
            limit: 500,
          },
        },
      },
      { tags: ['nodes'] },
    ),
  );

  if (rows.isPending) {
    return <p className={statusClassName}>Resolving current rows…</p>;
  }

  if (rows.isError) {
    return <p className={statusClassName}>{rows.error.message}</p>;
  }

  const columns: Column<NodeRow>[] = resource.columns.map((column, index) => ({
    key: column.key,
    header: column.header,
    ...(column.mono === true && { mono: true }),
    sortValue: (row) =>
      sortableCell(readCell(row, column.key, column.fromAttrs)),
    render: (row) => {
      const value = asCell(readCell(row, column.key, column.fromAttrs));

      // The first column is the row's identity, so it is the link. Anywhere
      // else a link would compete with the value it sits next to.
      return index === 0 ? (
        <Link
          to="/$resourceKey/$nodeId"
          params={{ resourceKey: resource.key, nodeId: row.id }}
          className={rowLinkClassName}
        >
          {value}
        </Link>
      ) : (
        value
      );
    },
  }));

  return (
    <DataTable
      label={resource.label}
      columns={columns}
      rows={rows.data}
      rowKey={(row) => `${row.id}:${row.record_id}`}
      emptyMessage={`No ${resource.label.toLowerCase()} match at the current as-of date.`}
    />
  );
}

/**
 * Narrows a jsonb attribute to something orderable.
 *
 * A boolean sorts as its rendered text so the two groups stay adjacent, and an
 * object has no meaningful order — it sorts as absent rather than as
 * `[object Object]`, which would collate every one of them together anyway.
 */
function sortableCell(value: unknown): string | number | null | undefined {
  if (typeof value === 'string' || typeof value === 'number') {
    return value;
  }

  if (typeof value === 'boolean') {
    return value ? 'yes' : 'no';
  }

  return null;
}

function readCell(
  row: NodeRow,
  key: string,
  fromAttrs: boolean | undefined,
): unknown {
  // Spread rather than asserted: a registry column names a key by string, and
  // `{ ...row }` is assignable to an index signature without claiming the row is
  // something it is not.
  const columns: Record<string, unknown> = { ...row };

  return fromAttrs === true ? row.attrs[key] : columns[key];
}

function EdgeRows({ term }: { term: string }) {
  const rows = useQuery(
    api.queryOptions(
      'get',
      '/v1/secstore/edges',
      { params: { query: { limit: 500 } } },
      { tags: ['edges'] },
    ),
  );

  if (rows.isPending) {
    return <p className={statusClassName}>Resolving current edges…</p>;
  }

  if (rows.isError) {
    return <p className={statusClassName}>{rows.error.message}</p>;
  }

  const filtered =
    term === ''
      ? rows.data
      : rows.data.filter((e) =>
          `${e.rel_type} ${e.src_id} ${e.dst_id}`
            .toLowerCase()
            .includes(term.toLowerCase()),
        );

  const columns: Column<EdgeRow>[] = [
    {
      key: 'rel_type',
      header: 'Type',
      render: (e) => e.rel_type,
      sortValue: (e) => e.rel_type,
    },
    {
      key: 'src_id',
      header: 'Source',
      mono: true,
      render: (e) => e.src_id,
      sortValue: (e) => e.src_id,
    },
    {
      key: 'dst_id',
      header: 'Destination',
      mono: true,
      render: (e) => e.dst_id,
      sortValue: (e) => e.dst_id,
    },
    {
      key: 'rel_weight',
      header: 'Weight',
      mono: true,
      numeric: true,
      render: (e) => e.rel_weight ?? '—',
      // Sorted as a number, not as the exact-decimal string it is stored as:
      // ordering is a display concern, and the stored text is what is sent.
      sortValue: (e) => (e.rel_weight === null ? null : Number(e.rel_weight)),
    },
    {
      key: 'weight_basis',
      header: 'Basis',
      render: (e) => e.weight_basis ?? '—',
      sortValue: (e) => e.weight_basis,
    },
    {
      key: 'valid_from',
      header: 'From',
      render: (e) => e.valid_from,
      sortValue: (e) => e.valid_from,
    },
    {
      key: 'valid_to',
      header: 'To',
      render: (e) => e.valid_to,
      sortValue: (e) => e.valid_to,
    },
  ];

  return (
    <DataTable
      label="Relationships"
      columns={columns}
      rows={filtered}
      rowKey={(e) => `${e.edge_id}:${e.record_id}`}
      emptyMessage="No relationships match."
    />
  );
}

function InstrumentRows({ term }: { term: string }) {
  const rows = useQuery(
    api.queryOptions(
      'get',
      '/v1/secstore/registers/instrument',
      { params: { query: { ...(term !== '' && { q: term }), limit: 200 } } },
      { tags: ['registers'] },
    ),
  );

  if (rows.isPending || rows.isError) {
    return (
      <p className={statusClassName}>
        {rows.isError ? rows.error.message : 'Reading the register…'}
      </p>
    );
  }

  return (
    <DataTable
      label="Instrument register"
      rows={rows.data}
      rowKey={(row) => row.instrument_key}
      emptyMessage="No instrument keys match."
      columns={[
        {
          key: 'instrument_key',
          header: 'Key',
          mono: true,
          render: (r) => r.instrument_key,
          sortValue: (r) => r.instrument_key,
        },
        {
          key: 'key_namespace',
          header: 'Namespace',
          render: (r) => r.key_namespace,
          sortValue: (r) => r.key_namespace,
        },
        {
          key: 'security_id',
          header: 'Security',
          mono: true,
          render: (r) => r.security_id,
          sortValue: (r) => r.security_id,
        },
        {
          key: 'chain_id',
          header: 'Chain',
          numeric: true,
          render: (r) => asCell(r.chain_id),
          sortValue: (r) => r.chain_id,
        },
        {
          key: 'valid_from',
          header: 'From',
          render: (r) => r.valid_from,
          sortValue: (r) => r.valid_from,
        },
      ]}
    />
  );
}

function AliasRows({ term }: { term: string }) {
  const rows = useQuery(
    api.queryOptions(
      'get',
      '/v1/secstore/registers/alias',
      { params: { query: { ...(term !== '' && { q: term }), limit: 200 } } },
      { tags: ['registers'] },
    ),
  );

  if (rows.isPending || rows.isError) {
    return (
      <p className={statusClassName}>
        {rows.isError ? rows.error.message : 'Reading the register…'}
      </p>
    );
  }

  return (
    <DataTable
      label="Alias register"
      rows={rows.data}
      rowKey={(row) => `${row.id_scheme}:${row.id_value}`}
      emptyMessage="No aliases match."
      columns={[
        {
          key: 'id_scheme',
          header: 'Scheme',
          render: (r) => r.id_scheme,
          sortValue: (r) => r.id_scheme,
        },
        {
          key: 'id_value',
          header: 'Value',
          mono: true,
          render: (r) => r.id_value,
          sortValue: (r) => r.id_value,
        },
        {
          key: 'node_id',
          header: 'Node',
          mono: true,
          render: (r) => r.node_id,
          sortValue: (r) => r.node_id,
        },
        {
          key: 'valid_from',
          header: 'From',
          render: (r) => r.valid_from,
          sortValue: (r) => r.valid_from,
        },
        {
          key: 'valid_to',
          header: 'To',
          render: (r) => r.valid_to,
          sortValue: (r) => r.valid_to,
        },
      ]}
    />
  );
}

const filterClassName = css({ maxWidth: 'sm', minWidth: '0' });

const statusClassName = css({
  fontSize: 'sm',
  color: 'text.muted',
  px: '4',
  py: '6',
  textAlign: 'center',
});

const rowLinkClassName = css({
  fontFamily: 'mono',
  fontSize: 'xs',
  color: 'text.link',
  textDecoration: 'none',
  _hover: { textDecoration: 'underline' },
  _focusVisible: {
    outlineWidth: '2px',
    outlineStyle: 'solid',
    outlineColor: 'interactive.accent',
    outlineOffset: '[2px]',
    borderRadius: 'sm',
  },
});
