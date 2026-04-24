import { useEffect, useMemo, useState } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { getBadDebt } from '../../../lib/api';
import {
  formatTokenAmount,
  formatRatioPercent,
  formatUsdValue,
  getBadDebtTone,
  parseNumericValue,
} from '../../../lib/dashboard';
import { isAbortError, toErrorMessage } from '../../../lib/errors';
import type { Allocation, BadDebt } from '../../../types/allocation';

type BadDebtTabProps = {
  selectedReceiptToken: Allocation | null;
};

function getToneStyles(tone: ReturnType<typeof getBadDebtTone>) {
  switch (tone) {
    case 'green':
      return {
        badgeBg: { _dark: 'green.950', base: 'green.50' },
        badgeColor: { _dark: 'green.200', base: 'green.700' },
        valueColor: { _dark: 'green.300', base: 'green.700' },
      };
    case 'yellow':
      return {
        badgeBg: { _dark: 'yellow.950', base: 'yellow.50' },
        badgeColor: { _dark: 'yellow.200', base: 'yellow.800' },
        valueColor: { _dark: 'yellow.300', base: 'yellow.800' },
      };
    case 'neutral':
      return {
        badgeBg: { _dark: 'gray.900', base: 'gray.100' },
        badgeColor: { _dark: 'gray.400', base: 'gray.600' },
        valueColor: { _dark: 'gray.400', base: 'gray.600' },
      };
    default:
      return {
        badgeBg: { _dark: 'red.950', base: 'red.50' },
        badgeColor: { _dark: 'red.200', base: 'red.700' },
        valueColor: { _dark: 'red.300', base: 'red.700' },
      };
  }
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

export function BadDebtTab({ selectedReceiptToken }: BadDebtTabProps) {
  const [badDebt, setBadDebt] = useState<BadDebt | null>(null);
  const [debouncedGapPct, setDebouncedGapPct] = useState(0.25);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [sliderValue, setSliderValue] = useState(0.25);

  useEffect(() => {
    const timeoutId = window.setTimeout(() => {
      setDebouncedGapPct(sliderValue);
    }, 300);

    return () => window.clearTimeout(timeoutId);
  }, [sliderValue]);

  useEffect(() => {
    if (!selectedReceiptToken) {
      setBadDebt(null);
      setErrorMessage(null);
      setIsLoading(false);
      return;
    }

    const controller = new AbortController();

    setIsLoading(true);
    setErrorMessage(null);
    setBadDebt(null);

    void getBadDebt(
      selectedReceiptToken.receipt_token_id,
      debouncedGapPct.toFixed(2),
      controller.signal,
    )
      .then((response) => {
        setBadDebt(response);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        setErrorMessage(toErrorMessage(error));
        setBadDebt(null);
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsLoading(false);
        }
      });

    return () => controller.abort();
  }, [debouncedGapPct, selectedReceiptToken]);

  const tone = getBadDebtTone(badDebt?.bad_debt_usd);
  const toneStyles = getToneStyles(tone);
  const sliderLabel = formatRatioPercent(sliderValue, 0);
  const requestLabel = formatRatioPercent(
    badDebt?.gap_pct ?? debouncedGapPct,
    0,
  );
  const badDebtValue = parseNumericValue(badDebt?.bad_debt_usd) ?? 0;
  const hasProjectedShortfall = badDebtValue > 0;

  const statusLabel = useMemo(() => {
    switch (tone) {
      case 'green':
        return 'Contained';
      case 'yellow':
        return 'Monitor';
      default:
        return 'Escalating';
    }
  }, [tone]);

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
          Pick a receipt token to model bad debt under a liquidation gap.
        </p>
      </div>
    );
  }

  return (
    <div className={css({ display: 'grid', gap: '4' })}>
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
        <div
          className={flex({
            align: 'flex-start',
            justify: 'space-between',
            gap: '4',
            wrap: 'wrap',
          })}
        >
          <div className={css({ display: 'grid', gap: '2' })}>
            <p
              className={css({
                m: 0,
                fontSize: 'xs',
                textTransform: 'uppercase',
                letterSpacing: '0.16em',
                color: 'text.muted',
              })}
            >
              Bad debt model
            </p>
            <h3 className={css({ m: 0, fontSize: 'lg', color: 'text.strong' })}>
              {selectedReceiptToken.symbol}
            </h3>
            <p className={css({ m: 0, fontSize: 'sm', color: 'text.muted' })}>
              {selectedReceiptToken.protocol_name} · request gap {requestLabel}
            </p>
          </div>

          <span
            className={css({
              display: 'inline-flex',
              alignItems: 'center',
              borderRadius: 'sm',
              bg: toneStyles.badgeBg,
              color: toneStyles.badgeColor,
              fontSize: 'xs',
              fontWeight: 'semibold',
              px: '3',
              py: '1.5',
            })}
          >
            {statusLabel}
          </span>
        </div>

        <div className={css({ mt: '5' })}>
          <label
            htmlFor="bad-debt-gap"
            className={css({ display: 'grid', gap: '2' })}
          >
            <span
              className={css({
                fontSize: 'sm',
                fontWeight: 'semibold',
                color: 'text.strong',
              })}
            >
              Gap percentage
            </span>
            <div
              className={flex({
                align: 'center',
                justify: 'space-between',
                gap: '3',
                wrap: 'wrap',
              })}
            >
              <input
                id="bad-debt-gap"
                type="range"
                min="0.05"
                max="0.95"
                step="0.05"
                value={sliderValue}
                onChange={(event) => setSliderValue(Number(event.target.value))}
                className={css({
                  flex: '1',
                  minWidth: '16rem',
                  accentColor: 'interactive.accent',
                  cursor: 'pointer',
                })}
              />
              <span
                className={css({
                  fontSize: 'sm',
                  fontWeight: 'semibold',
                  color: 'text.strong',
                })}
              >
                {sliderLabel}
              </span>
            </div>
          </label>
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
            Unable to calculate bad debt.
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

      {!errorMessage ? (
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
          <SummaryMetric label="Requested gap" value={requestLabel} />
          <SummaryMetric
            label="Receipt token balance"
            value={`${formatTokenAmount(selectedReceiptToken.balance)} ${selectedReceiptToken.symbol}`}
          />
          <SummaryMetric
            label="Underlying asset"
            value={selectedReceiptToken.underlying_symbol}
          />
          <SummaryMetric
            label="Protocol"
            value={selectedReceiptToken.protocol_name}
          />
        </div>
      ) : null}

      {!errorMessage ? (
        <div
          className={css({
            borderRadius: 'md',
            borderStyle: 'solid',
            borderWidth: '1px',
            borderColor: 'border.subtle',
            bg: 'surface.default',
            p: '5',
          })}
        >
          <p
            className={css({
              m: 0,
              fontSize: 'xs',
              textTransform: 'uppercase',
              letterSpacing: '0.16em',
              color: 'text.muted',
            })}
          >
            Estimated bad debt
          </p>
          <p
            className={css({
              m: 0,
              mt: '3',
              fontSize: { base: '3xl', md: '4xl' },
              fontWeight: 'semibold',
              color: toneStyles.valueColor,
            })}
          >
            {badDebt
              ? formatUsdValue(badDebt.bad_debt_usd)
              : isLoading
                ? 'Loading…'
                : '—'}
          </p>
          <p
            className={css({
              m: 0,
              mt: '2',
              fontSize: 'sm',
              color: 'text.muted',
            })}
          >
            {hasProjectedShortfall
              ? `At ${requestLabel}, the model projects a shortfall on this receipt token after liquidation.`
              : `At ${requestLabel}, the model currently shows no bad debt for this receipt token.`}
          </p>
        </div>
      ) : null}
    </div>
  );
}
