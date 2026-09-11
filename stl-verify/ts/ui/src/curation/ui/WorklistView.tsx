import { Badge } from '@archon-research/design-system';
import { useQuery } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';

import { css } from '#styled-system/css';

import { api } from '../lib/api.ts';
import type { NodeValidityRow } from '../lib/contract.ts';
import { type Column, DataTable } from './DataTable.tsx';
import { PageFrame, PageSection } from './PageFrame.tsx';

/**
 * The `node_validity` worklist.
 *
 * The view that makes the severity model visible, and the one a
 * create-form-only UI would leave out. Its existence is the reason EXPECTED
 * shape gaps are not form errors: an incompletely curated row is *supposed* to
 * be storable, and this is the queue it lands in — the stewardship-by-exception
 * pattern ADR-0007 §11 takes from MDM practice, rather than blocking the write
 * and losing the work.
 */
export function WorklistView() {
  const rows = useQuery(
    api.queryOptions(
      'get',
      '/v1/secstore/node-validity',
      { params: { query: { limit: 500 } } },
      { tags: ['validity', 'nodes', 'edges'] },
    ),
  );

  const required = (rows.data ?? []).filter((r) => r.severity === 'REQUIRED');
  const expected = (rows.data ?? []).filter((r) => r.severity === 'EXPECTED');

  return (
    <PageFrame
      crumbs={[{ label: 'Workflows' }, { label: 'Validity worklist' }]}
      title="Validity worklist"
      description="Unmet shape obligations over the current graph. REQUIRED would have blocked the append; EXPECTED means the row is stored, flagged, and out of metrics until curated."
      meta={
        rows.data === undefined ? undefined : (
          <>
            <Badge>{required.length} required</Badge>
            <Badge>{expected.length} expected</Badge>
          </>
        )
      }
    >
      <PageSection bleed>
        {rows.isPending && (
          <p className={statusClassName}>
            Evaluating shapes over the current graph…
          </p>
        )}
        {rows.isError && (
          <p className={statusClassName}>{rows.error.message}</p>
        )}
        {rows.data !== undefined && (
          <DataTable
            label="Unmet shape obligations"
            rows={rows.data}
            rowKey={(r) => `${r.node_id}:${r.shape_id}:${r.target}`}
            emptyMessage="Every current node satisfies its active shapes."
            columns={COLUMNS}
          />
        )}
      </PageSection>
    </PageFrame>
  );
}

const COLUMNS: Column<NodeValidityRow>[] = [
  {
    key: 'node_id',
    header: 'Node',
    mono: true,
    sortValue: (r) => r.node_id,
    render: (r) => (
      <Link
        to="/$resourceKey/$nodeId"
        params={{
          resourceKey: resourceKeyFor(r.record_type),
          nodeId: r.node_id,
        }}
        className={linkClassName}
      >
        {r.node_id}
      </Link>
    ),
  },
  {
    key: 'record_type',
    header: 'Kind',
    render: (r) => r.record_type,
    sortValue: (r) => r.record_type,
  },
  {
    key: 'shape_id',
    header: 'Shape',
    mono: true,
    render: (r) => r.shape_id,
    sortValue: (r) => r.shape_id,
  },
  {
    key: 'severity',
    header: 'Severity',
    render: (r) => (
      <Badge variant="subtle" colorPalette={severityPalette(r.severity)}>
        {r.severity}
      </Badge>
    ),
    // REQUIRED before EXPECTED before ADVISORY: severity order, not alphabetical,
    // because the point of sorting this column is to bring blockers to the top.
    sortValue: (r) => SEVERITY_RANK[r.severity],
  },
  {
    key: 'target',
    header: 'Missing',
    mono: true,
    render: (r) => `${r.target} (${r.kind})`,
    sortValue: (r) => r.target,
  },
  {
    key: 'message',
    header: 'What it means',
    render: (r) => r.message,
    sortValue: (r) => r.message,
  },
];

const SEVERITY_RANK: Record<string, number> = {
  REQUIRED: 0,
  EXPECTED: 1,
  ADVISORY: 2,
};

/**
 * Severity as a hue, so the tier reads without parsing the word.
 *
 * `Badge` rather than `StatusPill`: the pill renders `name: value`, and in a
 * column already headed "Severity" that reads "SEVERITY EXPECTED" on every row.
 * Badge takes the value alone and still resolves its hue through the same
 * dark-aware role tokens.
 */
function severityPalette(severity: string): 'red' | 'amber' | 'neutral' {
  switch (severity) {
    case 'REQUIRED':
      return 'red';
    case 'EXPECTED':
      return 'amber';
    default:
      return 'neutral';
  }
}

function resourceKeyFor(recordType: string): string {
  switch (recordType) {
    case 'SECURITY':
      return 'securities';
    case 'ENTITY':
      return 'entities';
    case 'CONCEPT':
      return 'concepts';
    case 'SOURCE':
      return 'sources';
    default:
      return 'accounts';
  }
}

const statusClassName = css({
  fontSize: 'sm',
  color: 'text.muted',
  px: '4',
  py: '6',
  textAlign: 'center',
});

const linkClassName = css({
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
