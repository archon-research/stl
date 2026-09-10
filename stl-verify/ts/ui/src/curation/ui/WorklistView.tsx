import { Badge, EmptyState, Panel } from '@archon-research/design-system';
import { useQuery } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';

import { css } from '#styled-system/css';

import { api } from '../lib/api.ts';

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

  if (rows.isPending) {
    return (
      <Panel title="Loading">Evaluating shapes over the current graph…</Panel>
    );
  }

  if (rows.isError) {
    return <Panel title="Failed">{rows.error.message}</Panel>;
  }

  if (rows.data.length === 0) {
    return (
      <EmptyState
        title="Nothing flagged"
        description="Every current node satisfies its active shapes."
      />
    );
  }

  const required = rows.data.filter((r) => r.severity === 'REQUIRED');
  const expected = rows.data.filter((r) => r.severity === 'EXPECTED');

  return (
    <div className={stack}>
      <div>
        <h1 className={heading}>Validity worklist</h1>
        <p className={subheading}>
          Unmet shape obligations over the current graph. REQUIRED would have
          blocked the append; EXPECTED means the row is stored, flagged, and out
          of metrics until curated.
        </p>
      </div>

      <div className={counts}>
        <Badge>{required.length} required</Badge>
        <Badge>{expected.length} expected</Badge>
      </div>

      <div className={tableWrap}>
        <table className={table}>
          <thead>
            <tr>
              {[
                'Node',
                'Kind',
                'Shape',
                'Severity',
                'Missing',
                'What it means',
              ].map((column) => (
                <th key={column} className={th}>
                  {column}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.data.map((row) => (
              <tr
                key={`${row.node_id}:${row.shape_id}:${row.target}`}
                className={tr}
              >
                <td className={`${td} ${mono}`}>
                  <Link
                    to="/$resourceKey/$nodeId"
                    params={{
                      resourceKey: resourceKeyFor(row.record_type),
                      nodeId: row.node_id,
                    }}
                    className={link}
                  >
                    {row.node_id}
                  </Link>
                </td>
                <td className={td}>{row.record_type}</td>
                <td className={`${td} ${mono}`}>{row.shape_id}</td>
                <td className={td}>{row.severity}</td>
                <td className={`${td} ${mono}`}>
                  {row.target} <span className={dim}>({row.kind})</span>
                </td>
                <td className={td}>{row.message}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
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

const stack = css({ display: 'flex', flexDirection: 'column', gap: '4' });
const heading = css({ fontSize: 'xl', fontWeight: 'semibold' });
const subheading = css({
  fontSize: 'sm',
  color: 'text.muted',
  maxWidth: '4xl',
});
const counts = css({ display: 'flex', gap: '2' });
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
const dim = css({ color: 'text.muted' });
const link = css({ color: 'text.default', textDecoration: 'underline' });
