import {
  LoadingIndicator,
} from '@archon-research/design-system';
import { useEffect, useMemo, useState } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { PercentageSlider, StatusBadge, SummaryMetric } from '../../shared';
import { getBadDebt } from '../../../lib/api';
import {
  formatRatioPercent,
  formatTokenAmount,
  formatUsdValue,
  getBadDebtTone,
  parseNumericValue,
} from '../../../lib/dashboard';
import { isAbortError, toErrorMessage } from '../../../lib/errors';
import { logging } from '../../../lib/logging';
import type { Allocation, BadDebt } from '../../../types/allocation';

type BadDebtTabProps = {
  selectedReceiptToken: Allocation | null;
};

function getToneStyles(tone: ReturnType<typeof getBadDebtTone>) {
  switch (tone) {
    case 'green':
      return {
        valueColor: { _dark: 'green.400', base: 'green.600' },
      };
    case 'yellow':
      return {
        valueColor: { _dark: 'yellow.400', base: 'yellow.700' },
      };
    case 'neutral':
      return {
        valueColor: { _dark: 'gray.400', base: 'gray.700' },
      };
    default:
      return {
        valueColor: { _dark: 'red.400', base: 'red.600' },
      };
  }
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

        logging.error('Failed to load bad debt calculation', {
          error,
          receiptTokenId: selectedReceiptToken.receipt_token_id,
          gapPct: debouncedGapPct,
        });
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
      case 'neutral':
        return 'Unavailable';
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
            {isLoading ? <LoadingIndicator message="Recalculating scenario" /> : null}
          </div>

          <StatusBadge tone={tone} label={statusLabel} />
        </div>

        <div className={css({ mt: '5' })}>
          <PercentageSlider
            id="bad-debt-gap"
            label="Gap percentage"
            value={sliderValue}
            onChange={setSliderValue}
          />
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
              md: 'repeat(3, minmax(0, 1fr))',
            },
            gap: '3',
          })}
        >
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
            {badDebt ? formatUsdValue(badDebt.bad_debt_usd) : isLoading ? '—' : '—'}
          </p>
          {isLoading && !badDebt ? (
            <div className={css({ mt: '3' })}>
              <LoadingIndicator message="Fetching bad debt model" />
            </div>
          ) : (
            <p
              className={css({
                m: 0,
                mt: '2',
                fontSize: 'sm',
                color: 'text.muted',
              })}
            >
              {hasProjectedShortfall
                ? `At ${requestLabel}, the model projects a shortfall after liquidation.`
                : `At ${requestLabel}, the model currently shows no bad debt.`}
            </p>
          )}
        </div>
      ) : null}

    </div>
  );
}
