import { ErrorState, SkeletonStack } from '@archon-research/design-system';
import { useEffect, useMemo, useState } from 'react';

import { css, cx } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { getRrc } from '../../../lib/api';
import {
  type UsdTone,
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
import { TabNotePanel } from './TabStatePanels';

type RrcTabProps = {
  isEnabled: boolean;
  selectedReceiptToken: Allocation | null;
  selectedPrime: Prime | null;
};

const MODEL_LABELS: Record<string, string> = {
  suraf: 'SURAF',
  gap_sweep: 'Gap sweep',
  core_model: 'CORE',
};

// A map of finished class names rather than a tone-to-token-path helper: see
// `lib/activity.tsx` for why Panda cannot extract the latter.
const TONE_VALUE_COLOR_CLASS: Record<UsdTone, string> = {
  green: css({ color: 'text.success' }),
  yellow: css({ color: 'text.warning' }),
  red: css({ color: 'text.critical' }),
  neutral: css({ color: 'text.muted' }),
};

export function RrcTab({
  isEnabled,
  selectedReceiptToken,
  selectedPrime,
}: RrcTabProps) {
  const [rrc, setRrc] = useState<Rrc | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  const receiptTokenId = selectedReceiptToken?.receipt_token_id ?? null;
  const chainId = selectedReceiptToken?.chain_id ?? null;
  const receiptTokenAddress =
    selectedReceiptToken?.receipt_token_address ?? null;
  const primeAddress = selectedPrime?.address ?? null;
  // The RRC endpoint scales a pool share to the exact (chain_id, prime
  // address) pair, so it only resolves for the chain selectedPrime.address
  // actually holds a position on — the prime's primary proxy's chain, today
  // always mainnet. A non-mainnet allocation would 503 (share_data_missing)
  // or hit an uncaught backend error rather than return a real breakdown.
  const isChainMismatch =
    chainId !== null &&
    selectedPrime !== null &&
    chainId !== selectedPrime.chain_id;

  useEffect(() => {
    if (
      !isEnabled ||
      chainId === null ||
      receiptTokenAddress === null ||
      primeAddress === null ||
      isChainMismatch
    ) {
      setRrc(null);
      setErrorMessage(null);
      setIsLoading(false);
      return;
    }

    const controller = new AbortController();

    setIsLoading(true);
    setErrorMessage(null);
    setRrc(null);

    void getRrc(chainId, receiptTokenAddress, primeAddress, controller.signal)
      .then((response) => {
        setRrc(response);
      })
      .catch((error: unknown) => {
        if (isAbortError(error)) {
          return;
        }

        logging.error('Failed to load required risk capital (RRC)', {
          error,
          chainId,
          receiptTokenId,
          receiptTokenAddress,
          primeAddress,
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
  }, [
    chainId,
    isChainMismatch,
    isEnabled,
    primeAddress,
    receiptTokenAddress,
    receiptTokenId,
  ]);

  const tone = getUsdTone(rrc?.max_rrc_usd);
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
      <TabNotePanel message="Pick a receipt token to inspect required risk capital." />
    );
  }

  if (receiptTokenId === null) {
    // FIX ME: add API to return applicable risk models per asset_id. SURAF
    // should eventually run on some direct-held assets (no receipt-token
    // wrapper), so this branch should query that registry instead of
    // hard-coding "no risk model" for every direct holding.
    return (
      <TabNotePanel message="Required risk capital is only computed for receipt-token positions. Direct asset holdings have no risk model." />
    );
  }

  if (isChainMismatch) {
    return (
      <TabNotePanel message="Required risk capital is not yet available for non-mainnet allocations." />
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
              <SkeletonStack
                count={1}
                itemHeight={16}
                className={css({ width: '32' })}
              />
            ) : null}
          </div>

          <StatusBadge tone={tone} label={statusLabel} />
        </div>
      </div>

      {errorMessage ? (
        <ErrorState
          title="Unable to compute required risk capital."
          description={errorMessage}
          tone="critical"
          size="inline"
        />
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
                  protocolName={selectedReceiptToken.protocol_name ?? ''}
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
            className={cx(
              css({
                m: 0,
                mt: '3',
                fontSize: { base: '3xl', md: '4xl' },
                fontWeight: 'semibold',
              }),
              TONE_VALUE_COLOR_CLASS[tone],
            )}
          >
            {rrc ? formatUsdValue(rrc.max_rrc_usd) : '—'}
          </p>
          {isLoading && !rrc ? (
            <div className={css({ mt: '3' })}>
              <SkeletonStack count={2} itemHeight={14} />
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
