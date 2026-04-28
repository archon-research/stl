import {
  LoadingIndicator,
  SkeletonRows,
  SkeletonStack,
} from '@archon-research/design-system';
import { useEffect, useMemo, useState } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { useDataTable } from '../../../data-table/hooks';
import { getRiskBreakdown } from '../../../lib/api';
import {
  formatMultiplier,
  formatPercentValue,
  formatRatioPercent,
  formatUsdValue,
  parseNumericValue,
} from '../../../lib/dashboard';
import { isAbortError, toErrorMessage } from '../../../lib/errors';
import type { Allocation, RiskBreakdown } from '../../../types/allocation';

type RiskBreakdownTabProps = {
  selectedReceiptToken: Allocation | null;
};

type RiskItem = RiskBreakdown['items'][number];

function RiskTable({ items, isLoading }: { items: RiskItem[]; isLoading: boolean }) {
  const columns = useMemo(
    () => [
      {
        id: 'symbol',
        header: 'Symbol',
        accessorKey: 'symbol',
        cell: (info: any) => info.getValue() as string,
      },
      {
        id: 'amount',
        header: 'Amount',
        accessorKey: 'amount',
        cell: (info: any) => {
          const value = info.getValue();
          return typeof value === 'string'
            ? parseFloat(value).toFixed(2)
            : (value as number).toFixed(2);
        },
      },
      {
        id: 'price_usd',
        header: 'Price USD',
        accessorKey: 'price_usd',
        cell: (info: any) => formatUsdValue(info.getValue()),
      },
      {
        id: 'amount_usd',
        header: 'Amount USD',
        accessorKey: 'amount_usd',
        cell: (info: any) => formatUsdValue(info.getValue()),
      },
      {
        id: 'backing_pct',
        header: 'Backing %',
        accessorKey: 'backing_pct',
        cell: (info: any) => formatPercentValue(info.getValue()),
      },
      {
        id: 'lt',
        header: 'Liquidation Threshold',
        accessorKey: 'liquidation_threshold',
        cell: (info: any) => formatRatioPercent(info.getValue()),
      },
      {
        id: 'bonus',
        header: 'Liquidation Bonus',
        accessorKey: 'liquidation_bonus',
        cell: (info: any) => formatMultiplier(info.getValue()),
      },
    ],
    [],
  );

  const table = useDataTable(items, columns, {
    enableSorting: true,
  });

  return (
    <div
      className={css({
        overflowX: 'auto',
        borderRadius: 'md',
        borderStyle: 'solid',
        borderWidth: '1px',
        borderColor: 'border.subtle',
      })}
    >
      <table
        className={css({
          width: '100%',
          minWidth: '76rem',
          borderCollapse: 'collapse',
        })}
      >
        <thead>
          <tr className={css({ bg: 'surface.subtle' })}>
            {table.getHeaderGroups().map((headerGroup) =>
              headerGroup.headers.map((header) => {
                const headerLabel =
                  typeof header.column.columnDef.header === 'string'
                    ? header.column.columnDef.header
                    : typeof header.column.columnDef.header === 'function'
                    ? (header.column.columnDef.header as any)()
                    : null;

                return (
                  <th
                    key={header.id}
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
                    {header.isPlaceholder ? null : headerLabel}
                  </th>
                );
              }),
            )}
          </tr>
        </thead>
        <tbody>
          {isLoading && items.length === 0
            ? SkeletonRows({ rows: 5, columns: 7, firstColumnTall: false })
            : table.getRowModel().rows.map((row) => (
                <tr
                  key={row.original.token_id}
                  className={css({
                    borderBottomWidth: '1px',
                    borderBottomStyle: 'solid',
                    borderBottomColor: 'border.subtle',
                  })}
                >
                  {row.getVisibleCells().map((cell) => {
                    const cellContent = cell.column.columnDef.cell
                        ? (cell.column.columnDef.cell as any)({
                            ...cell.getContext(),
                          })
                      : cell.getValue();

                    return (
                      <td
                        key={cell.id}
                        className={css({
                          px: '4',
                          py: '3',
                        })}
                      >
                        <p
                          className={css({
                            m: 0,
                            fontSize: 'sm',
                            color: 'text.strong',
                          })}
                        >
                            {cellContent as React.ReactNode}
                        </p>
                      </td>
                    );
                  })}
                </tr>
              ))}
        </tbody>
      </table>
    </div>
  );
}

function SummaryMetric({
  detail,
  label,
  value,
}: {
  detail?: string;
  label: string;
  value: string;
}) {
  return (
    <div
      className={css({
        borderRadius: 'md',
        borderStyle: 'solid',
        borderWidth: '1px',
        borderColor: 'border.subtle',
        bg: 'surface.default',
        p: '3',
      })}
    >
      <p
        className={css({
          m: 0,
          fontSize: 'xs',
          textTransform: 'uppercase',
          letterSpacing: '0.12em',
          color: 'text.muted',
        })}
      >
        {label}
      </p>
      <p
        className={css({
          m: 0,
          mt: '2',
          fontSize: 'lg',
          fontWeight: 'semibold',
          color: 'text.strong',
        })}
      >
        {value}
      </p>
      {detail ? (
        <p
          className={css({
            m: 0,
            mt: '1',
            fontSize: 'xs',
            color: 'text.muted',
          })}
        >
          {detail}
        </p>
      ) : null}
    </div>
  );
}

export function RiskBreakdownTab({
  selectedReceiptToken,
}: RiskBreakdownTabProps) {
  const [breakdown, setBreakdown] = useState<RiskBreakdown | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (!selectedReceiptToken) {
      setBreakdown(null);
      setErrorMessage(null);
      setIsLoading(false);
      return;
    }

    const controller = new AbortController();

    setIsLoading(true);
    setErrorMessage(null);
    setBreakdown(null);

    void getRiskBreakdown(
      selectedReceiptToken.receipt_token_id,
      controller.signal,
    )
      .then((response) => {
        if (controller.signal.aborted) {
          return;
        }
        setBreakdown(response);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        setBreakdown(null);
        setErrorMessage(toErrorMessage(error));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsLoading(false);
        }
      });

    return () => controller.abort();
  }, [selectedReceiptToken]);

  const totalUsd = useMemo(() => {
    if (!breakdown) {
      return 0;
    }

    return breakdown.items.reduce(
      (sum, item) => sum + (parseNumericValue(item.amount_usd) ?? 0),
      0,
    );
  }, [breakdown]);

  const summary = useMemo(() => {
    if (!breakdown || breakdown.items.length === 0) {
      return null;
    }

    let weightedThreshold = 0;
    let weightedBonus = 0;
    let largestItem = breakdown.items[0] ?? null;
    let largestItemUsd = largestItem
      ? (parseNumericValue(largestItem.amount_usd) ?? 0)
      : 0;

    for (const item of breakdown.items) {
      const amountUsd = parseNumericValue(item.amount_usd) ?? 0;
      const liquidationThreshold = parseNumericValue(
        item.liquidation_threshold,
      );
      const liquidationBonus = parseNumericValue(item.liquidation_bonus);

      if (amountUsd > largestItemUsd) {
        largestItem = item;
        largestItemUsd = amountUsd;
      }

      if (liquidationThreshold !== null) {
        weightedThreshold += liquidationThreshold * amountUsd;
      }

      if (liquidationBonus !== null) {
        weightedBonus += liquidationBonus * amountUsd;
      }
    }

    return {
      assetCount: breakdown.items.length,
      largestItem,
      weightedBonus: totalUsd > 0 ? weightedBonus / totalUsd : null,
      weightedThreshold: totalUsd > 0 ? weightedThreshold / totalUsd : null,
    };
  }, [breakdown, totalUsd]);

  if (!selectedReceiptToken) {
    return (
      <div
        className={css({
          borderRadius: 'md',
          borderStyle: 'solid',
          borderWidth: '1px',
          borderColor: 'border.subtle',
          bg: 'surface.subtle',
          p: '4',
        })}
      >
        <p className={css({ m: 0, fontSize: 'sm', color: 'text.muted' })}>
          Pick a receipt token to inspect its collateral backing.
        </p>
      </div>
    );
  }

  return (
    <div className={css({ display: 'grid', gap: '4' })}>
      <div
        className={flex({
          align: 'center',
          justify: 'space-between',
          gap: '4',
          wrap: 'wrap',
        })}
      >
        <div className={css({ display: 'grid', gap: '1' })}>
          <p
            className={css({
              m: 0,
              fontSize: 'xs',
              textTransform: 'uppercase',
              letterSpacing: '0.16em',
              color: 'text.muted',
            })}
          >
            Risk breakdown
          </p>
          <h3 className={css({ m: 0, fontSize: 'lg', color: 'text.strong' })}>
            {selectedReceiptToken.symbol}
          </h3>
          <p className={css({ m: 0, fontSize: 'sm', color: 'text.muted' })}>
            {selectedReceiptToken.protocol_name} · backing total{' '}
            {formatUsdValue(totalUsd)}
          </p>
        </div>

        <div className={flex({ gap: '2', wrap: 'wrap' })}>
          <span
            className={css({
              display: 'inline-flex',
              alignItems: 'center',
              borderRadius: 'sm',
              bg: 'surface.subtle',
              color: 'text.muted',
              fontSize: 'xs',
              px: '3',
              py: '1.5',
            })}
          >
            {selectedReceiptToken.underlying_symbol}
          </span>
          <span
            className={css({
              display: 'inline-flex',
              alignItems: 'center',
              borderRadius: 'sm',
              bg: 'surface.subtle',
              color: 'text.muted',
              fontSize: 'xs',
              px: '3',
              py: '1.5',
            })}
          >
            {breakdown ? `${breakdown.items.length} assets` : 'Loading'}
          </span>
        </div>
      </div>

      {isLoading ? <LoadingIndicator message="Loading risk breakdown" /> : null}

      {errorMessage ? (
        <div
          className={css({
            borderRadius: 'md',
            borderStyle: 'solid',
            borderWidth: '1px',
            borderColor: 'border.default',
            bg: 'surface.subtle',
            p: '4',
          })}
        >
          <p
            className={css({
              m: 0,
              fontSize: 'sm',
              fontWeight: 'semibold',
              color: 'text.strong',
            })}
          >
            Unable to load the risk breakdown.
          </p>
          <p
            className={css({
              m: 0,
              mt: '1.5',
              fontSize: 'sm',
              color: 'text.muted',
            })}
          >
            {errorMessage}
          </p>
        </div>
      ) : null}

      {!errorMessage && summary ? (
        <div
          className={css({
            display: 'grid',
            gridTemplateColumns: {
              base: '1fr',
              md: 'repeat(4, minmax(0, 1fr))',
            },
            gap: '3',
          })}
        >
          <SummaryMetric
            label="Total backing"
            value={formatUsdValue(totalUsd)}
            detail={`${summary.assetCount} collateral assets`}
          />
          <SummaryMetric
            label="Largest exposure"
            value={summary.largestItem ? summary.largestItem.symbol : '—'}
            detail={
              summary.largestItem
                ? `${formatUsdValue(summary.largestItem.amount_usd)} · ${formatPercentValue(summary.largestItem.backing_pct)}`
                : undefined
            }
          />
          <SummaryMetric
            label="Weighted LT"
            value={formatRatioPercent(summary.weightedThreshold)}
          />
          <SummaryMetric
            label="Weighted bonus"
            value={formatMultiplier(summary.weightedBonus)}
          />
        </div>
      ) : null}

      {!errorMessage && isLoading && !summary ? (
        <SkeletonStack count={4} itemHeight={88} />
      ) : null}

      {!errorMessage &&
      !isLoading &&
      breakdown &&
      breakdown.items.length === 0 ? (
        <div
          className={css({
            borderRadius: 'md',
            borderStyle: 'solid',
            borderWidth: '1px',
            borderColor: 'border.subtle',
            bg: 'surface.subtle',
            p: '4',
          })}
        >
          <p className={css({ m: 0, fontSize: 'sm', color: 'text.muted' })}>
            This receipt token returned no collateral items for the risk
            breakdown response.
          </p>
        </div>
      ) : null}

      {!errorMessage && (isLoading || breakdown) ? (
        <RiskTable items={breakdown?.items ?? []} isLoading={isLoading} />
      ) : null}
    </div>
  );
}
