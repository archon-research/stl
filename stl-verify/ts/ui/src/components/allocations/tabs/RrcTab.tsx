import {
  Badge,
  ErrorState,
  SkeletonStack,
} from '@archon-research/design-system';
import { useEffect, useMemo, useState } from 'react';

import { css, cx } from '#styled-system/css';
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
import { showsReference } from '../../../lib/provenance';
import type {
  Allocation,
  AllocationRiskCapital,
  PrimeRiskCapital,
  Prime,
  Rrc,
} from '../../../types/allocation';
import { ProtocolLogo, SummaryMetric, TokenLogo } from '../../shared';
import { TabNotePanel, unindexedChainMessage } from './TabStatePanels';

type RrcTabProps = {
  isEnabled: boolean;
  selectedReceiptToken: Allocation | null;
  // The response the page already fetched. Fetching it again here would issue a
  // second call to the slowest endpoint on the page every time the drawer opens.
  riskCapital: PrimeRiskCapital | null;
  selectedPrime: Prime | null;
};

const MODEL_LABELS: Record<string, string> = {
  suraf: 'SURAF',
  gap_sweep: 'Gap sweep',
  core_model: 'CORE',
};

type ReferenceFigures = { crrPct: string | null; rrcUsd: string | null };

// A zero is a figure Sky published rather than a missing one, so a position is
// dropped only when it reports neither.
function toReferenceFigures(
  rrcUsd: string | null | undefined,
  crrPct: string | null | undefined,
): ReferenceFigures | null {
  return rrcUsd == null && crrPct == null
    ? null
    : { crrPct: crrPct ?? null, rrcUsd: rrcUsd ?? null };
}

// Sky's own figures for this position. Under `both` they ride beside STL's in
// the `reference_` fields; under `reference` the bare fields are already Sky's.
function referenceFigures(
  entry: AllocationRiskCapital | null,
): ReferenceFigures | null {
  switch (entry?.source) {
    case 'both':
      return toReferenceFigures(
        entry.reference_required_risk_capital_usd,
        entry.reference_crr_pct,
      );
    case 'reference':
      return toReferenceFigures(entry.required_risk_capital_usd, entry.crr_pct);
    default:
      return null;
  }
}

/**
 * Bands mirror `getUsdTone`'s thresholds — the meter is that function drawn
 * out, so a reader sees what "Escalating" means: past the $1M band edge.
 */
const RRC_BAND_BOUNDS: readonly [number, number] = [1_000, 1_000_000];

const RRC_BANDS = [
  { tone: 'green', label: 'Contained', fill: css({ bg: 'green.500' }) },
  { tone: 'yellow', label: 'Monitor', fill: css({ bg: 'amber.400' }) },
  { tone: 'red', label: 'Escalating', fill: css({ bg: 'red.500' }) },
] as const;

/**
 * Each band spans a third of the track and the value falls within its band by
 * orders of magnitude — a linear USD scale would flatten the first two bands
 * into invisibility next to a $23M value. The red band caps at $1B.
 */
function rrcMeterFraction(value: number): number {
  const [low, high] = RRC_BAND_BOUNDS;
  if (value <= 0) {
    return 0;
  }
  if (value <= low) {
    return Math.log10(1 + value) / Math.log10(low + 1) / 3;
  }
  if (value <= high) {
    return 1 / 3 + Math.log10(value / low) / Math.log10(high / low) / 3;
  }
  return Math.min(1, 2 / 3 + Math.log10(value / high) / 3 / 3);
}

const meterMarkerClassName = css({
  position: 'absolute',
  top: '100%',
  transform: 'translateX(-50%)',
  width: '0',
  height: '0',
  borderLeft: '5px solid transparent',
  borderRight: '5px solid transparent',
  borderBottom: '6px solid',
  borderBottomColor: 'text.strong',
});

function RrcBandMeter({ valueUsd }: { valueUsd: number }) {
  const tone = getUsdTone(valueUsd);
  const activeIndex = RRC_BANDS.findIndex((band) => band.tone === tone);
  const valueText = formatUsdValue(valueUsd);

  return (
    <div>
      {/* The painted bands are decoration for the caption below, which carries
          the value and (screen-reader-only) the band it falls in. */}
      <div aria-hidden className={css({ position: 'relative' })}>
        <div className={flex({ gap: '0.5' })}>
          {RRC_BANDS.map((band, index) => (
            <div
              key={band.tone}
              className={cx(
                css({ height: '2.5', flex: '1' }),
                index === 0 ? css({ borderLeftRadius: 'full' }) : undefined,
                index === RRC_BANDS.length - 1
                  ? css({ borderRightRadius: 'full' })
                  : undefined,
                band.fill,
                index === activeIndex ? undefined : css({ opacity: 0.3 }),
              )}
            />
          ))}
        </div>
        <div
          className={meterMarkerClassName}
          style={{ insetInlineStart: `${rrcMeterFraction(valueUsd) * 100}%` }}
        />
      </div>
      <div aria-hidden className={flex({ mt: '2.5', gap: '0.5' })}>
        {RRC_BANDS.map((band, index) => (
          <span
            key={band.tone}
            className={cx(
              css({
                flex: '1',
                textAlign: 'center',
                fontSize: '2xs',
                textTransform: 'uppercase',
                letterSpacing: '0.08em',
              }),
              index === activeIndex
                ? css({ color: 'text.strong', fontWeight: 'semibold' })
                : css({ color: 'text.muted' }),
            )}
          >
            {band.label}
          </span>
        ))}
      </div>
      <p
        className={css({
          m: 0,
          mt: '1.5',
          fontSize: 'sm',
          color: 'text.muted',
        })}
      >
        {valueText} max across models
        <span className={css({ srOnly: true })}>
          {` — ${RRC_BANDS[activeIndex]?.label ?? 'unbanded'}`}
        </span>
      </p>
    </div>
  );
}

function ResultRow({
  isSelected = false,
  label,
  value,
}: {
  isSelected?: boolean;
  label: string;
  value: string;
}) {
  return (
    <li
      aria-current={isSelected ? 'true' : undefined}
      className={cx(
        flex({
          align: 'center',
          justify: 'space-between',
          gap: '4',
          p: '3',
          borderRadius: 'sm',
          borderWidth: '1px',
          borderStyle: 'solid',
        }),
        // Accent is what DESIGN.md spends on a selection; the transparent border
        // on every other row keeps the rows the same height.
        css({
          borderColor: isSelected ? 'interactive.accent' : 'transparent',
          bg: isSelected ? 'interactive.selected' : 'surface.default',
        }),
      )}
    >
      <span
        className={flex({
          align: 'center',
          gap: '2',
          fontSize: 'sm',
          fontWeight: 'semibold',
          color: 'text.strong',
        })}
      >
        {label}
        {isSelected ? (
          <Badge size="sm" variant="subtle">
            Selected
          </Badge>
        ) : null}
      </span>
      <span className={css({ fontSize: 'sm', color: 'text.muted' })}>
        {value}
      </span>
    </li>
  );
}

export function RrcTab({
  isEnabled,
  selectedReceiptToken,
  selectedPrime,
  riskCapital,
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

  // Which model the prime's reported requirement comes from, and Sky's figures
  // for the same position: the RRC response carries neither, since it runs every
  // model and takes no provenance.
  //
  // Joined on the published keys rather than `receipt_token_id`, which only
  // STL's own rows carry — a position Sky reports and STL does not index has
  // none, and those are the rows Sky prices highest.
  const riskCapitalEntry = useMemo(() => {
    if (!isEnabled || isChainMismatch || selectedReceiptToken === null) {
      return null;
    }

    const keys = new Set(selectedReceiptToken.position_keys ?? []);
    return (
      riskCapital?.per_allocation.find((entry) =>
        (entry.position_keys ?? []).some((key) => keys.has(key)),
      ) ?? null
    );
  }, [isChainMismatch, isEnabled, riskCapital, selectedReceiptToken]);

  const selectedModel = riskCapitalEntry?.model ?? null;
  const reference = showsReference ? referenceFigures(riskCapitalEntry) : null;
  // Sky's figure is the one every display prefers when it exists, so its row
  // carries the badge; a model row is "selected" only when Sky reports nothing.
  const skySelected = reference !== null;

  if (!selectedReceiptToken) {
    return (
      <TabNotePanel message="Pick a receipt token to inspect required risk capital." />
    );
  }

  if (selectedReceiptToken.chain_id === null) {
    return (
      <TabNotePanel
        message={unindexedChainMessage(
          selectedReceiptToken.network,
          'required risk capital',
        )}
      />
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

      {!errorMessage && isLoading && !rrc ? (
        <SkeletonStack count={2} itemHeight={12} />
      ) : null}
      {!errorMessage && rrc ? (
        <RrcBandMeter valueUsd={parseNumericValue(rrc.max_rrc_usd) ?? 0} />
      ) : null}

      {!errorMessage && rrc && rrc.results.length > 0 ? (
        <div
          className={css({
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
              <ResultRow
                key={result.risk_model}
                isSelected={!skySelected && result.risk_model === selectedModel}
                label={MODEL_LABELS[result.risk_model] ?? result.risk_model}
                value={`${formatUsdValue(result.rrc_usd)} · CRR ${formatPercentValue(result.comparable_crr_pct, 2)}`}
              />
            ))}
            {/* Its own row rather than a substitution: the tab compares model
                outputs, and Sky's figures disagree with STL's by design. */}
            {reference ? (
              <ResultRow
                isSelected={skySelected}
                label="Sky published figure"
                value={`${formatUsdValue(reference.rrcUsd)} · CRR ${formatPercentValue(reference.crrPct, 2)}`}
              />
            ) : null}
          </ul>
        </div>
      ) : null}
    </div>
  );
}
