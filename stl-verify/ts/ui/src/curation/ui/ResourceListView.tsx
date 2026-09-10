import {
  Button,
  EmptyState,
  Panel,
  SearchInput,
} from '@archon-research/design-system';
import { useQuery } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { useState } from 'react';

import { css } from '#styled-system/css';

import { asCell } from '../form/text.ts';
import { api } from '../lib/api.ts';
import type { EdgeRow, NodeRow } from '../lib/contract.ts';
import type { CurationResource } from '../schema/registry.ts';

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
    <div className={stack}>
      <div className={headerRow}>
        <div>
          <h1 className={heading}>{resource.label}</h1>
          <p className={subheading}>{resource.description}</p>
        </div>
        <Link to="/$resourceKey/new" params={{ resourceKey: resource.key }}>
          <Button>New {resource.singular}</Button>
        </Link>
      </div>

      <div className={css({ maxWidth: 'sm' })}>
        <SearchInput
          value={term}
          onValueChange={setTerm}
          placeholder="Filter"
        />
      </div>

      {resource.store === 'node' && (
        <NodeRows resource={resource} term={term} />
      )}
      {resource.store === 'edge' && <EdgeRows term={term} />}
      {resource.store === 'instrument_register' && (
        <InstrumentRows term={term} />
      )}
      {resource.store === 'alias_register' && <AliasRows term={term} />}
    </div>
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
            limit: 200,
          },
        },
      },
      { tags: ['nodes'] },
    ),
  );

  if (rows.isPending) {
    return <Panel title="Loading">Resolving current rows…</Panel>;
  }

  if (rows.isError) {
    return <Panel title="Failed">{rows.error.message}</Panel>;
  }

  if (rows.data.length === 0) {
    return (
      <EmptyState
        title={`No ${resource.label.toLowerCase()}`}
        description="Nothing matches at the current as-of date."
      />
    );
  }

  return (
    <Table
      columns={resource.columns.map((c) => c.header)}
      rows={rows.data.map((row) => ({
        key: `${row.id}:${row.record_id}`,
        href: { resourceKey: resource.key, nodeId: row.id },
        cells: resource.columns.map((column) => ({
          value: cellValue(row, column.key, column.fromAttrs),
          mono: column.mono === true,
        })),
      }))}
    />
  );
}

function cellValue(
  row: NodeRow,
  key: string,
  fromAttrs: boolean | undefined,
): string {
  // Spread rather than asserted: a registry column names a key by string, and
  // `{ ...row }` is assignable to an index signature without claiming the row is
  // something it is not.
  const columns: Record<string, unknown> = { ...row };

  return asCell(fromAttrs === true ? row.attrs[key] : columns[key]);
}

function EdgeRows({ term }: { term: string }) {
  const rows = useQuery(
    api.queryOptions(
      'get',
      '/v1/secstore/edges',
      { params: { query: { limit: 500 } } },
      {
        tags: ['edges'],
      },
    ),
  );

  if (rows.isPending) {
    return <Panel title="Loading">Resolving current edges…</Panel>;
  }

  if (rows.isError) {
    return <Panel title="Failed">{rows.error.message}</Panel>;
  }

  const filtered =
    term === ''
      ? rows.data
      : rows.data.filter((e) =>
          `${e.rel_type} ${e.src_id} ${e.dst_id}`
            .toLowerCase()
            .includes(term.toLowerCase()),
        );

  return (
    <Table
      columns={[
        'Type',
        'Source',
        'Destination',
        'Weight',
        'Basis',
        'From',
        'To',
      ]}
      rows={filtered.slice(0, 200).map((edge: EdgeRow) => ({
        key: `${edge.edge_id}:${edge.record_id}`,
        cells: [
          { value: edge.rel_type, mono: false },
          { value: edge.src_id, mono: true },
          { value: edge.dst_id, mono: true },
          { value: edge.rel_weight ?? '—', mono: true },
          { value: edge.weight_basis ?? '—', mono: false },
          { value: edge.valid_from, mono: false },
          { value: edge.valid_to, mono: false },
        ],
      }))}
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
      <Panel title={rows.isError ? 'Failed' : 'Loading'}>
        {rows.isError ? rows.error.message : 'Reading the register…'}
      </Panel>
    );
  }

  return (
    <Table
      columns={['Key', 'Namespace', 'Security', 'Chain', 'From']}
      rows={rows.data.map((row) => ({
        key: row.instrument_key,
        cells: [
          { value: row.instrument_key, mono: true },
          { value: row.key_namespace, mono: false },
          { value: row.security_id, mono: true },
          { value: String(row.chain_id ?? '—'), mono: false },
          { value: row.valid_from, mono: false },
        ],
      }))}
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
      <Panel title={rows.isError ? 'Failed' : 'Loading'}>
        {rows.isError ? rows.error.message : 'Reading the register…'}
      </Panel>
    );
  }

  return (
    <Table
      columns={['Scheme', 'Value', 'Node', 'From', 'To']}
      rows={rows.data.map((row) => ({
        key: `${row.id_scheme}:${row.id_value}`,
        cells: [
          { value: row.id_scheme, mono: false },
          { value: row.id_value, mono: true },
          { value: row.node_id, mono: true },
          { value: row.valid_from, mono: false },
          { value: row.valid_to, mono: false },
        ],
      }))}
    />
  );
}

type TableRow = {
  key: string;
  href?: { resourceKey: string; nodeId: string };
  cells: readonly { value: string; mono: boolean }[];
};

/**
 * A plain table.
 *
 * `DataTable` from the design system is the right destination for this — it is
 * the tanstack-table wrapper the rest of the app uses — but it wants a column
 * definition per resource, and mapping the registry's column set onto it is a
 * job with its own design questions (sorting a jsonb attribute, a link column).
 * A plain table keeps the spike's subject the forms, and the swap is local to
 * this file.
 */
function Table({
  columns,
  rows,
}: {
  columns: readonly string[];
  rows: readonly TableRow[];
}) {
  return (
    <div className={tableWrap}>
      <table className={table}>
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column} className={th}>
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.key} className={tr}>
              {row.cells.map((cell, index) => (
                <td
                  key={`${row.key}:${index}`}
                  className={cell.mono ? `${td} ${mono}` : td}
                >
                  {index === 0 && row.href !== undefined ? (
                    <Link
                      to="/$resourceKey/$nodeId"
                      params={{
                        resourceKey: row.href.resourceKey,
                        nodeId: row.href.nodeId,
                      }}
                      className={link}
                    >
                      {cell.value}
                    </Link>
                  ) : (
                    cell.value
                  )}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

const stack = css({ display: 'flex', flexDirection: 'column', gap: '4' });
const headerRow = css({
  display: 'flex',
  alignItems: 'flex-start',
  justifyContent: 'space-between',
  gap: '4',
});
const heading = css({ fontSize: 'xl', fontWeight: 'semibold' });
const subheading = css({
  fontSize: 'sm',
  color: 'text.muted',
  maxWidth: '4xl',
});
const tableWrap = css({
  overflowX: 'auto',
  borderWidth: '1px',
  borderStyle: 'solid',
  borderColor: 'border.subtle',
  borderRadius: 'md',
});
const table = css({
  width: 'full',
  borderCollapse: 'collapse',
  fontSize: 'sm',
});
const th = css({
  textAlign: 'left',
  padding: '3',
  fontSize: 'xs',
  textTransform: 'uppercase',
  letterSpacing: 'wide',
  color: 'text.muted',
  borderBottomWidth: '1px',
  borderBottomStyle: 'solid',
  borderColor: 'border.subtle',
});
const tr = css({ _hover: { bg: 'surface.subtle' } });
const td = css({
  padding: '3',
  borderBottomWidth: '1px',
  borderBottomStyle: 'solid',
  borderColor: 'border.subtle',
});
const mono = css({ fontFamily: 'mono', fontSize: 'xs' });
const link = css({ color: 'text.default', textDecoration: 'underline' });
