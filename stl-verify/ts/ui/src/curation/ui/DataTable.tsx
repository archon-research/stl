import { ChevronDown, ChevronsUpDown, ChevronUp } from 'lucide-react';
import { type ReactNode, useMemo, useState } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

/**
 * The table every listing uses, sortable by column.
 *
 * Sorting is client-side over the rows already fetched, which is honest for the
 * sizes here (the largest listing is 352 concepts) and dishonest the moment a
 * read is paginated — at which point the sort has to move into the query, since
 * sorting one page of a larger set orders the wrong thing. Named here because
 * that is the change the first `limit`-exceeding resource will force.
 *
 * Three-state cycle: ascending, descending, then back to the source order. The
 * third state matters because the source order is itself meaningful — the node
 * reads come back id-ordered and the append history comes back newest-first, so
 * "undo my sort" is a thing a reader wants and a two-state toggle cannot offer.
 */

export type Column<TRow> = {
  key: string;
  header: string;
  /** The cell content. */
  render: (row: TRow) => ReactNode;
  /** The value sorted on. Omit to make the column unsortable. */
  sortValue?: (row: TRow) => string | number | null | undefined;
  mono?: boolean;
  /** Right-aligns the column, for figures. */
  numeric?: boolean;
};

export type DataTableProps<TRow> = {
  columns: readonly Column<TRow>[];
  rows: readonly TRow[];
  rowKey: (row: TRow) => string;
  emptyMessage?: string;
  /** Caption for assistive tech; the visible heading is the section's. */
  label: string;
};

type SortState = { key: string; direction: 'asc' | 'desc' } | null;

export function DataTable<TRow>({
  columns,
  rows,
  rowKey,
  emptyMessage = 'Nothing to show.',
  label,
}: DataTableProps<TRow>) {
  const [sort, setSort] = useState<SortState>(null);

  const sorted = useMemo(() => {
    if (sort === null) {
      return rows;
    }

    const column = columns.find((c) => c.key === sort.key);
    if (column?.sortValue === undefined) {
      return rows;
    }

    const read = column.sortValue;
    const factor = sort.direction === 'asc' ? 1 : -1;

    // Copied before sorting: the array belongs to the query cache, and sorting
    // in place would reorder what every other reader of that cache sees.
    return [...rows].sort((a, b) => factor * compare(read(a), read(b)));
  }, [rows, columns, sort]);

  const toggle = (key: string) => {
    setSort((current) => {
      if (current === null || current.key !== key) {
        return { key, direction: 'asc' };
      }

      return current.direction === 'asc' ? { key, direction: 'desc' } : null;
    });
  };

  if (rows.length === 0) {
    return <p className={emptyClassName}>{emptyMessage}</p>;
  }

  return (
    <div className={scrollClassName}>
      <table className={tableClassName}>
        <caption className={captionClassName}>{label}</caption>
        <thead>
          <tr>
            {columns.map((column) => {
              const sortable = column.sortValue !== undefined;
              const active = sort?.key === column.key;

              return (
                <th
                  key={column.key}
                  scope="col"
                  className={
                    column.numeric === true
                      ? `${thClassName} ${numericClassName}`
                      : thClassName
                  }
                  aria-sort={
                    active
                      ? sort.direction === 'asc'
                        ? 'ascending'
                        : 'descending'
                      : sortable
                        ? 'none'
                        : undefined
                  }
                >
                  {sortable ? (
                    <button
                      type="button"
                      onClick={() => toggle(column.key)}
                      className={sortButtonClassName}
                      data-active={active ? 'true' : undefined}
                    >
                      {column.header}
                      <SortGlyph
                        active={active}
                        direction={active ? sort.direction : undefined}
                      />
                    </button>
                  ) : (
                    column.header
                  )}
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {sorted.map((row) => (
            <tr key={rowKey(row)} className={trClassName}>
              {columns.map((column) => (
                <td key={column.key} className={cellClassName(column)}>
                  {column.render(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function SortGlyph({
  active,
  direction,
}: {
  active: boolean;
  direction: 'asc' | 'desc' | undefined;
}) {
  if (!active) {
    return (
      <ChevronsUpDown
        size={12}
        aria-hidden="true"
        className={glyphIdleClassName}
      />
    );
  }

  return direction === 'asc' ? (
    <ChevronUp size={12} aria-hidden="true" className={glyphActiveClassName} />
  ) : (
    <ChevronDown
      size={12}
      aria-hidden="true"
      className={glyphActiveClassName}
    />
  );
}

/**
 * Orders two cell values.
 *
 * Absent sorts last in both directions rather than winning the ascending end —
 * a column of mostly-empty optional attributes is otherwise a screen of blanks
 * before anything readable. Numbers compare numerically; everything else
 * compares with `localeCompare` and numeric collation, so `pv 2` sorts before
 * `pv 10`.
 */
function compare(
  a: string | number | null | undefined,
  b: string | number | null | undefined,
): number {
  const aMissing = a === null || a === undefined || a === '';
  const bMissing = b === null || b === undefined || b === '';

  if (aMissing || bMissing) {
    return aMissing && bMissing ? 0 : aMissing ? 1 : -1;
  }

  if (typeof a === 'number' && typeof b === 'number') {
    return a - b;
  }

  return String(a).localeCompare(String(b), undefined, { numeric: true });
}

function cellClassName<TRow>(column: Column<TRow>): string {
  const parts = [tdClassName];
  if (column.mono === true) {
    parts.push(monoClassName);
  }
  if (column.numeric === true) {
    parts.push(numericClassName);
  }

  return parts.join(' ');
}

const scrollClassName = css({ overflowX: 'auto', minWidth: '0' });

const tableClassName = css({
  width: 'full',
  borderCollapse: 'collapse',
  fontSize: 'sm',
});

/** Present for assistive tech; the section header carries the visible name. */
const captionClassName = css({
  position: 'absolute',
  width: '[1px]',
  height: '[1px]',
  padding: '0',
  margin: '[-1px]',
  overflow: 'hidden',
  clipPath: 'inset(50%)',
  whiteSpace: 'nowrap',
});

const thClassName = css({
  textAlign: 'left',
  padding: '0',
  borderBottomWidth: '1px',
  borderBottomStyle: 'solid',
  borderColor: 'border.subtle',
  bg: 'surface.subtle',
  fontSize: '2xs',
  fontWeight: 'semibold',
  textTransform: 'uppercase',
  letterSpacing: 'wider',
  color: 'text.muted',
  whiteSpace: 'nowrap',
  position: 'sticky',
  top: '0',
  zIndex: '[1]',
});

const sortButtonClassName = flex({
  align: 'center',
  gap: '1.5',
  width: 'full',
  appearance: 'none',
  background: 'transparent',
  borderWidth: '0',
  px: '4',
  py: '2.5',
  font: 'inherit',
  fontSize: '2xs',
  fontWeight: 'semibold',
  textTransform: 'uppercase',
  letterSpacing: 'wider',
  color: 'text.muted',
  cursor: 'pointer',
  transitionProperty: 'colors',
  transitionDuration: 'fast',
  _hover: { color: 'text.default' },
  '&[data-active]': { color: 'text.strong' },
  _focusVisible: {
    outlineWidth: '2px',
    outlineStyle: 'solid',
    outlineColor: 'interactive.accent',
    outlineOffset: '[-2px]',
  },
});

const glyphIdleClassName = css({ color: 'border.strong', flexShrink: 0 });
const glyphActiveClassName = css({
  color: 'interactive.accent',
  flexShrink: 0,
});

const trClassName = css({
  _hover: { bg: 'interactive.hover' },
});

const tdClassName = css({
  px: '4',
  py: '2.5',
  borderBottomWidth: '1px',
  borderBottomStyle: 'solid',
  borderColor: 'border.subtle',
  verticalAlign: 'top',
  color: 'text.default',
});

const monoClassName = css({ fontFamily: 'mono', fontSize: 'xs' });

const numericClassName = css({
  textAlign: 'right',
  fontVariantNumeric: 'tabular-nums',
});

const emptyClassName = css({
  fontSize: 'sm',
  color: 'text.muted',
  px: '4',
  py: '6',
  textAlign: 'center',
});
