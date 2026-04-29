import { SkeletonRows } from '@archon-research/design-system';
import { flexRender, type Table } from '@tanstack/react-table';
import type { ReactNode } from 'react';

import { css } from '#styled-system/css';

type DataTableProps<TData> = {
  table: Table<TData>;
  isLoading: boolean;
  onRowClick?: (row: TData) => void;
  getRowKey?: (row: TData) => string;
  selectedRowKey?: string | null;
  skeletonConfig?: {
    rows?: number;
    columns?: number;
    firstColumnTall?: boolean;
  };
  renderCell?: (cell: ReactNode) => ReactNode;
  className?: string;
  minWidth?: string;
};

export function DataTable<TData>({
  table,
  isLoading,
  onRowClick,
  getRowKey,
  selectedRowKey,
  skeletonConfig = { rows: 3, columns: 3, firstColumnTall: true },
  renderCell,
  className,
  minWidth = '48rem',
}: DataTableProps<TData>) {
  return (
    <div
      className={
        className ??
        css({
          overflowX: 'auto',
          borderRadius: 'md',
          borderStyle: 'solid',
          borderWidth: '1px',
          borderColor: 'border.subtle',
        })
      }
    >
      <table
        className={css({
          width: '100%',
          minWidth,
          borderCollapse: 'collapse',
          bg: 'surface.default',
        })}
      >
        <thead>
          {table.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id} className={css({ bg: 'surface.subtle' })}>
              {headerGroup.headers.map((header) => {
                const sorted = header.column.getIsSorted();
                const canSort = header.column.getCanSort();
                const ariaSort = canSort
                  ? sorted === 'asc'
                    ? 'ascending'
                    : sorted === 'desc'
                      ? 'descending'
                      : 'none'
                  : undefined;

                return (
                  <th
                    key={header.id}
                    aria-sort={ariaSort}
                    className={css({
                      px: '4',
                      py: '3',
                      textAlign: 'left',
                      fontSize: 'xs',
                      fontWeight: 'semibold',
                      letterSpacing: '0.08em',
                      textTransform: 'uppercase',
                      color: 'text.muted',
                    })}
                  >
                    {header.isPlaceholder ? null : (
                      canSort ? (
                        <button
                          type="button"
                          onClick={header.column.getToggleSortingHandler()}
                          className={css({
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: '1.5',
                            border: 'none',
                            bg: 'transparent',
                            p: 0,
                            font: 'inherit',
                            color: 'inherit',
                            cursor: 'pointer',
                          })}
                        >
                          <span>
                            {flexRender(
                              header.column.columnDef.header,
                              header.getContext(),
                            )}
                          </span>
                          <span>
                            {sorted === 'asc'
                              ? '↑'
                              : sorted === 'desc'
                                ? '↓'
                                : '↕'}
                          </span>
                        </button>
                      ) : (
                        <span>
                          {flexRender(
                            header.column.columnDef.header,
                            header.getContext(),
                          )}
                        </span>
                      )
                    )}
                  </th>
                );
              })}
            </tr>
          ))}
        </thead>
        <tbody>
          {isLoading && table.getRowModel().rows.length === 0
            ? SkeletonRows(skeletonConfig)
            : table.getRowModel().rows.map((row) => {
                const rowKey = getRowKey
                  ? getRowKey(row.original)
                  : String(row.id);
                const isSelected =
                  selectedRowKey !== undefined && rowKey === selectedRowKey;
                const isClickable = onRowClick !== undefined;

                return (
                  <tr
                    key={rowKey}
                    aria-selected={isSelected || undefined}
                    tabIndex={isClickable ? 0 : undefined}
                    onClick={
                      isClickable ? () => onRowClick(row.original) : undefined
                    }
                    onKeyDown={
                      isClickable
                        ? (event) => {
                            if (event.key === 'Enter' || event.key === ' ') {
                              event.preventDefault();
                              onRowClick(row.original);
                            }
                          }
                        : undefined
                    }
                    className={css({
                      cursor: isClickable ? 'pointer' : 'default',
                      bg: isSelected
                        ? 'interactive.selected'
                        : 'surface.default',
                      transitionDuration: 'fast',
                      transitionProperty: 'background-color',
                      _hover: isClickable
                        ? { bg: 'interactive.hover' }
                        : undefined,
                      _focusVisible: isClickable
                        ? { bg: 'interactive.hover' }
                        : undefined,
                    })}
                  >
                    {row.getVisibleCells().map((cell) => (
                      <td
                        key={cell.id}
                        className={css({
                          borderBottomWidth: '1px',
                          borderBottomStyle: 'solid',
                          borderBottomColor: 'border.subtle',
                          px: '4',
                          py: '3.5',
                        })}
                      >
                        {renderCell
                          ? renderCell(
                              flexRender(
                                cell.column.columnDef.cell,
                                cell.getContext(),
                              ),
                            )
                          : flexRender(
                              cell.column.columnDef.cell,
                              cell.getContext(),
                            )}
                      </td>
                    ))}
                  </tr>
                );
              })}
        </tbody>
      </table>
    </div>
  );
}
