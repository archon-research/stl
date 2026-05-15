import { LoadingIndicator } from '@archon-research/design-system';
import { useEffect, useMemo, useState } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { getRrc } from '../../../lib/api';
import {
  formatPercentValue,
  formatTokenAmount,
  formatUsdValue,
  getUsdTone,
  parseNumericValue,
} from '../../../lib/dashboard';
import { isAbortError, toErrorMessage } from '../../../lib/errors';
import { logging } from '../../../lib/logging';
import type { Allocation, Prime, Rrc } from '../../../types/allocation';
import {
  ProtocolLogo,
  StatusBadge,
  SummaryMetric,
  TokenLogo,
} from '../../shared';

type RrcTabProps = {
  selectedReceiptToken: Allocation | null;
  selectedPrime: Prime | null;
};

const MODEL_LABELS: Record<string, string> = {
  suraf: 'SURAF',
  gap_sweep: 'Gap sweep',
};

function getToneStyles(tone: ReturnType<typeof getUsdTone>) {
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

export function RrcTab({ selectedReceiptToken, selectedPrime }: RrcTabProps) {
  const [rrc, setRrc] = useState<Rrc | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (!selectedReceiptToken || !selectedPrime) {
      setRrc(null);
      setErrorMessage(null);
      setIsLoading(false);
      return;
    }

    const controller = new AbortController();

    setIsLoading(true);
    setErrorMessage(null);
    setRrc(null);

    void getRrc(
      selectedReceiptToken.receipt_token_id,
      selectedPrime.id,
      controller.signal,
    )
      .then((response) => {
        setRrc(response);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.error('Failed to load required risk capital (RRC)', {
          error,
          receiptTokenId: selectedReceiptToken.receipt_token_id,
          primeId: selectedPrime.id,
        });
        setErrorMessage(toErrorMessage(error));
        setRrc(null);
      })
      .finally(() => {
        if (!controller.signal.aborted) {
          setIsLoading(false);
        }
      });

    return () => controller.abort();
  }, [selectedPrime, selectedReceiptToken]);

  const tone = getUsdTone(rrc?.max_rrc_usd);
  const toneStyles = getToneStyles(tone);
  const maxRrcValue = parseNumericValue(rrc?.max_rrc_usd) ?? 0;
  const hasRiskCapital = maxRrcValue > 0;

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
          Pick a receipt token to inspect required risk capital.
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
              Required risk capital (RRC)
            </p>
            {isLoading ? (
              <LoadingIndicator message="Fetching required risk capital" />
            ) : null}
          </div>

          <StatusBadge tone={tone} label={statusLabel} />
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
            Unable to compute required risk capital.
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
            value={
              <>
                <TokenLogo
                  address={selectedReceiptToken.receipt_token_address}
                  chainId={selectedReceiptToken.chain_id}
                  symbol={selectedReceiptToken.symbol}
                  size="7"
                />
                {`${formatTokenAmount(selectedReceiptToken.balance)} ${selectedReceiptToken.symbol}`}
              </>
            }
          />
          <SummaryMetric
            label="Underlying asset"
            value={
              <>
                <TokenLogo
                  address={selectedReceiptToken.underlying_token_address}
                  chainId={selectedReceiptToken.chain_id}
                  symbol={selectedReceiptToken.underlying_symbol}
                  size="7"
                />
                {selectedReceiptToken.underlying_symbol}
              </>
            }
          />
          <SummaryMetric
            label="Protocol"
            value={
              <>
                <ProtocolLogo
                  protocolName={selectedReceiptToken.protocol_name}
                  size="5"
                />
                {selectedReceiptToken.protocol_name}
              </>
            }
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
            Max required risk capital across models
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
            {rrc ? formatUsdValue(rrc.max_rrc_usd) : '—'}
          </p>
          {isLoading && !rrc ? (
            <div className={css({ mt: '3' })}>
              <LoadingIndicator message="Computing required risk capital" />
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
              {rrc
                ? hasRiskCapital
                  ? `Max comparable capital ratio: ${formatPercentValue(rrc.max_crr_pct, 2)}.`
                  : 'Models report no required risk capital at default stress.'
                : 'Pick a prime and receipt token to compute required risk capital.'}
            </p>
          )}
        </div>
      ) : null}

      {!errorMessage && rrc && rrc.results.length > 0 ? (
        <div
          className={css({
            borderRadius: 'md',
            borderStyle: 'solid',
            borderWidth: '1px',
            borderColor: 'border.subtle',
            bg: 'surface.subtle',
            p: '4',
            display: 'grid',
            gap: '3',
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
            Per-model results
          </p>
          <ul
            className={css({
              listStyle: 'none',
              m: 0,
              p: 0,
              display: 'grid',
              gap: '2',
            })}
          >
            {rrc.results.map((result) => (
              <li
                key={result.risk_model}
                className={flex({
                  align: 'center',
                  justify: 'space-between',
                  gap: '4',
                  p: '3',
                  borderRadius: 'sm',
                  bg: 'surface.default',
                })}
              >
                <span
                  className={css({
                    fontSize: 'sm',
                    fontWeight: 'semibold',
                    color: 'text.strong',
                  })}
                >
                  {MODEL_LABELS[result.risk_model] ?? result.risk_model}
                </span>
                <span
                  className={css({
                    fontSize: 'sm',
                    color: 'text.muted',
                  })}
                >
                  {`${formatUsdValue(result.rrc_usd)} · CRR ${formatPercentValue(result.comparable_crr_pct, 2)}`}
                </span>
              </li>
            ))}
          </ul>
        </div>
      ) : null}
    </div>
  );
}
