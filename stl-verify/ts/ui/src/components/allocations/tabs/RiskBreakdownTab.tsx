import { useEffect, useMemo, useState } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { getRiskBreakdown } from '../../../lib/api';
import {
  formatMultiplier,
  formatPercentValue,
  formatRatioPercent,
  formatTokenAmount,
  formatUsdValue,
  parseNumericValue,
} from '../../../lib/dashboard';
import type {
  ReceiptTokenPosition,
  RiskBreakdown,
} from '../../../types/allocation';

type RiskBreakdownTabProps = {
  selectedReceiptToken: ReceiptTokenPosition | null;
};

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === 'AbortError';
}

function toErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'Unknown request failure.';
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

function RiskBreakdownSkeleton() {
  return Array.from({ length: 5 }, (_row, rowIndex) => (
    <tr
      key={rowIndex}
      className={css({
        borderBottomWidth: '1px',
        borderBottomStyle: 'solid',
        borderBottomColor: 'border.subtle',
      })}
    >
      {Array.from({ length: 7 }, (_cell, cellIndex) => (
        <td key={cellIndex} className={css({ px: '4', py: '3' })}>
          <div
            className={css({
              height: '7',
              borderRadius: 'sm',
              bg: 'surface.subtle',
              opacity: 0.85,
            })}
          />
        </td>
      ))}
    </tr>
  ));
}

export function RiskBreakdownTab({
  selectedReceiptToken,
}: RiskBreakdownTabProps) {
  const [breakdown, setBreakdown] = useState<RiskBreakdown | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [reloadKey, setReloadKey] = useState(0);

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

    void getRiskBreakdown(
      selectedReceiptToken.receipt_token_id,
      controller.signal,
    )
      .then((response) => {
        setBreakdown(response);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        setErrorMessage(toErrorMessage(error));
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsLoading(false);
        }
      });

    return () => controller.abort();
  }, [reloadKey, selectedReceiptToken]);

  const totalUsd = useMemo(() => {
    if (!breakdown) {
      return 0;
    }

    return breakdown.items.reduce(
      (sum, item) => sum + Number(item.amount_usd),
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
          <button
            type="button"
            onClick={() => setReloadKey((value) => value + 1)}
            className={css({
              mt: '4',
              borderRadius: 'md',
              borderStyle: 'solid',
              borderWidth: '1px',
              borderColor: 'border.default',
              bg: 'surface.default',
              color: 'text.strong',
              cursor: 'pointer',
              px: '3.5',
              py: '2',
              _hover: { bg: 'interactive.hover' },
            })}
          >
            Retry request
          </button>
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
                {[
                  'Symbol',
                  'Amount',
                  'Price USD',
                  'Amount USD',
                  'Backing %',
                  'Liquidation Threshold',
                  'Liquidation Bonus',
                ].map((label) => (
                  <th
                    key={label}
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
                    {label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {isLoading && !breakdown
                ? RiskBreakdownSkeleton()
                : breakdown?.items.map((item) => (
                    <tr
                      key={item.token_id}
                      className={css({
                        borderBottomWidth: '1px',
                        borderBottomStyle: 'solid',
                        borderBottomColor: 'border.subtle',
                      })}
                    >
                      <td className={css({ px: '4', py: '3' })}>
                        <p
                          className={css({
                            m: 0,
                            fontSize: 'sm',
                            fontWeight: 'semibold',
                            color: 'text.strong',
                          })}
                        >
                          {item.symbol}
                        </p>
                      </td>
                      <td className={css({ px: '4', py: '3' })}>
                        <p
                          className={css({
                            m: 0,
                            fontSize: 'sm',
                            color: 'text.strong',
                          })}
                        >
                          {formatTokenAmount(item.amount)}
                        </p>
                      </td>
                      <td className={css({ px: '4', py: '3' })}>
                        <p
                          className={css({
                            m: 0,
                            fontSize: 'sm',
                            color: 'text.strong',
                          })}
                        >
                          {formatUsdValue(item.price_usd)}
                        </p>
                      </td>
                      <td className={css({ px: '4', py: '3' })}>
                        <p
                          className={css({
                            m: 0,
                            fontSize: 'sm',
                            color: 'text.strong',
                          })}
                        >
                          {formatUsdValue(item.amount_usd)}
                        </p>
                      </td>
                      <td className={css({ px: '4', py: '3' })}>
                        <p
                          className={css({
                            m: 0,
                            fontSize: 'sm',
                            color: 'text.strong',
                          })}
                        >
                          {formatPercentValue(item.backing_pct)}
                        </p>
                      </td>
                      <td className={css({ px: '4', py: '3' })}>
                        <p
                          className={css({
                            m: 0,
                            fontSize: 'sm',
                            color: 'text.strong',
                          })}
                        >
                          {formatRatioPercent(item.liquidation_threshold)}
                        </p>
                      </td>
                      <td className={css({ px: '4', py: '3' })}>
                        <p
                          className={css({
                            m: 0,
                            fontSize: 'sm',
                            color: 'text.strong',
                          })}
                        >
                          {formatMultiplier(item.liquidation_bonus)}
                        </p>
                      </td>
                    </tr>
                  ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </div>
  );
}
