import {
  Badge,
  ErrorState,
  SkeletonStack,
} from '@archon-research/design-system';
import { useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';

import { css, cx } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import {
  formatPercentValue,
  formatTokenAmount,
  formatUsdValue,
} from '../../shared/lib/dashboard';
import { toQueryErrorMessage } from '../../shared/lib/errors';
import { showsReference } from '../../shared/lib/provenance';
import {
  DISABLED_ADDRESS,
  DISABLED_CHAIN_ID,
  rrcQuery,
} from '../../shared/lib/queries';
import type {
  Allocation,
  AllocationRiskCapital,
  PrimeRiskCapital,
  Prime,
  Provenance,
  RrcResult,
} from '../../shared/types/allocation';
import { ProtocolLogo, SummaryMetric, TokenLogo } from '../../shared/ui';
import {
  TabNotePanel,
  unindexedChainMessage,
} from '../../shared/ui/TabStatePanels';

type RrcTabProps = {
  isEnabled: boolean;
  selectedReceiptToken: Allocation | null;
  // The response the page already fetched. Fetching it again here would issue a
  // second call to the slowest endpoint on the page every time the drawer opens.
  riskCapital: PrimeRiskCapital | null;
  selectedPrime: Prime | null;
};

const MODEL_LABELS: Record<string, string> = {
  suraf: 'SURAF (alpha)',
  gap_sweep: 'Gap sweep',
  core_model: 'CORE (alpha)',
};

// The table's own preference chain (PrimeRiskCapitalService._model_preference):
// under `source=both` it tries core_model alone — a position core_model can't
// price there falls back to Sky's reference figure, not gap_sweep; every other
// source (`indexed`, `reference`) also tries gap_sweep. suraf never appears
// there, so it's never a fallback candidate even if the drawer's own dispatch
// priced it.
const MODEL_PREFERENCE_BOTH = ['core_model'] as const;
const MODEL_PREFERENCE_DEFAULT = ['core_model', 'gap_sweep'] as const;

// Which model the table would have used for this position, when the table
// itself carries no model for it (unpriced there, or the row didn't join).
// Picked from what the drawer actually priced, so the badge never lands on a
// model that produced no result.
function fallbackSelectedModel(
  results: RrcResult[] | undefined,
  source: Provenance | undefined,
): string | null {
  const preference =
    source === 'both' ? MODEL_PREFERENCE_BOTH : MODEL_PREFERENCE_DEFAULT;
  return (
    preference.find((model) =>
      results?.some((result) => result.risk_model === model),
    ) ?? null
  );
}

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
//
// A pure `source=reference` fetch cannot be told apart from an `indexed` one
// by `entry.source` alone: `_reference_allocation` (the backend row builder
// for that path) never sets it, so it sits at the schema default
// (`indexed`) even though the whole response — and therefore every row — is
// Sky's. `responseSource`, the envelope's own `source`, is what actually
// says so; `entry.source === 'reference'` still fires correctly for a
// Sky-only row inside a `both` merge, which the backend does tag per-row.
function referenceFigures(
  entry: AllocationRiskCapital | null,
  responseSource: Provenance | undefined,
): ReferenceFigures | null {
  if (entry === null) {
    return null;
  }
  if (responseSource === 'reference' || entry.source === 'reference') {
    return toReferenceFigures(entry.required_risk_capital_usd, entry.crr_pct);
  }
  if (entry.source === 'both') {
    return toReferenceFigures(
      entry.reference_required_risk_capital_usd,
      entry.reference_crr_pct,
    );
  }
  return null;
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

const positionTilesClassName = css({
  display: 'grid',
  gridTemplateColumns: {
    base: '1fr',
    md: 'repeat(3, minmax(0, 1fr))',
  },
  gap: '3',
});

function PositionTiles({
  allocation,
  balanceLabel,
}: {
  allocation: Allocation;
  balanceLabel: string;
}) {
  return (
    <>
      <SummaryMetric
        // Sky reports USD exposure only, so a Sky-reported row has no token
        // quantity to show — the USD figure stands in.
        label={allocation.balance === null ? 'Position exposure' : balanceLabel}
        value={
          <>
            <TokenLogo
              address={allocation.receipt_token_address}
              chainId={allocation.chain_id}
              symbol={allocation.symbol}
              size="7"
            />
            {allocation.balance === null
              ? formatUsdValue(allocation.amount_usd)
              : `${formatTokenAmount(allocation.balance)} ${allocation.symbol}`}
          </>
        }
      />
      <SummaryMetric
        label="Underlying asset"
        value={
          <>
            <TokenLogo
              address={allocation.underlying_token_address}
              chainId={allocation.chain_id}
              symbol={allocation.underlying_symbol}
              size="7"
            />
            {allocation.underlying_symbol}
          </>
        }
      />
      {allocation.protocol_name == null ? null : (
        <SummaryMetric
          label="Protocol"
          value={
            <>
              <ProtocolLogo protocolName={allocation.protocol_name} size="5" />
              {allocation.protocol_name}
            </>
          }
        />
      )}
    </>
  );
}

export function RrcTab({
  isEnabled,
  selectedReceiptToken,
  selectedPrime,
  riskCapital,
}: RrcTabProps) {
  const receiptTokenId = selectedReceiptToken?.receipt_token_id ?? null;
  const chainId = selectedReceiptToken?.chain_id ?? null;
  // Falsy rather than nullish, matching the other two drawer reads: an empty
  // address would still build a request path that looks well-formed.
  const receiptTokenAddress =
    selectedReceiptToken?.receipt_token_address || null;
  const primeAddress = selectedPrime?.address || null;
  // The RRC endpoint scales a pool share to the exact (chain_id, prime
  // address) pair, so it only resolves for the chain selectedPrime.address
  // actually holds a position on — the prime's primary proxy's chain, today
  // always mainnet. A non-mainnet allocation would 503 (share_data_missing)
  // or hit an uncaught backend error rather than return a real breakdown.
  // `receipt_token_id` gates this like the grid's own rule: a row with no
  // receipt token (the off-chain custody leg, chain 0) can never resolve an
  // RRC breakdown anywhere, so calling it "cross-chain" would be wrong.
  const isChainMismatch =
    receiptTokenId !== null &&
    chainId !== null &&
    selectedPrime !== null &&
    chainId !== selectedPrime.chain_id;

  const canLoadRrc =
    isEnabled &&
    chainId !== null &&
    receiptTokenId !== null &&
    receiptTokenAddress !== null &&
    primeAddress !== null &&
    !isChainMismatch;

  const rrcResult = useQuery({
    ...rrcQuery(
      chainId ?? DISABLED_CHAIN_ID,
      receiptTokenAddress ?? DISABLED_ADDRESS,
      primeAddress ?? DISABLED_ADDRESS,
    ),
    enabled: canLoadRrc,
  });

  const rrc = rrcResult.data ?? null;
  const isLoading = canLoadRrc && rrcResult.isPending;
  const errorMessage = toQueryErrorMessage(rrcResult.error);

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

  const reference = showsReference
    ? referenceFigures(riskCapitalEntry, riskCapital?.source)
    : null;
  // Which row carries the badge. Under `source=reference` every row is Sky's,
  // so it always wins. Under `both` the table's own model leads (mirrors
  // `preferModelRiskFigure`): Sky wins only when the position is wholly
  // Sky-reported (no join, `entry.source === 'reference'`) or the model
  // priced nothing for it (`crr_pct` null) and Sky's is the only figure.
  const skySelected =
    reference !== null &&
    (riskCapital?.source !== 'both' ||
      riskCapitalEntry?.source === 'reference' ||
      riskCapitalEntry?.crr_pct == null);
  // The row's own model is the ground truth for what the table used. When the
  // table has no model for this position (unpriced there, or the position
  // didn't join), fall back to the same preference chain the backend tries so
  // the badge still lands on the model the table would have used.
  const selectedModel = skySelected
    ? null
    : (riskCapitalEntry?.model ??
      fallbackSelectedModel(rrc?.results, riskCapital?.source));

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
    // hard-coding "no STL model" for every direct holding.
    if (reference === null) {
      return (
        <TabNotePanel message="Required risk capital is only computed for receipt-token positions. Direct asset holdings have no risk model." />
      );
    }
    // No STL model runs here, but Sky prices the position — its published
    // figure is what the grid's RRC column already shows for this row.
    return (
      <div className={css({ display: 'grid', gap: '4' })}>
        <div className={positionTilesClassName}>
          <PositionTiles
            allocation={selectedReceiptToken}
            balanceLabel="Position balance"
          />
        </div>
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
              m: '0',
              fontSize: 'xs',
              textTransform: 'uppercase',
              // Wider than the widest token step (0.1em); kept exact.
              letterSpacing: '[0.16em]',
              color: 'text.muted',
            })}
          >
            Per-model results
          </p>
          <ul
            className={css({
              listStyle: 'none',
              m: '0',
              p: '0',
              display: 'grid',
              gap: '2',
            })}
          >
            <ResultRow
              isSelected
              label="Legacy figure"
              value={`${formatUsdValue(reference.rrcUsd)} · CRR ${formatPercentValue(reference.crrPct, 2)}`}
            />
          </ul>
          <p className={css({ m: '0', fontSize: 'sm', color: 'text.muted' })}>
            Verify&apos;s models run on receipt-token positions only; this is
            the legacy feed&apos;s published requirement for the position.
          </p>
        </div>
      </div>
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
        <div className={positionTilesClassName}>
          <PositionTiles
            allocation={selectedReceiptToken}
            balanceLabel="Receipt token balance"
          />
        </div>
      ) : null}

      {!errorMessage && isLoading && !rrc ? (
        <SkeletonStack count={2} itemHeight={12} />
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
              m: '0',
              fontSize: 'xs',
              textTransform: 'uppercase',
              // Wider than the widest token step (0.1em); kept exact.
              letterSpacing: '[0.16em]',
              color: 'text.muted',
            })}
          >
            Per-model results
          </p>
          <ul
            className={css({
              listStyle: 'none',
              m: '0',
              p: '0',
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
                label="Legacy figure"
                value={`${formatUsdValue(reference.rrcUsd)} · CRR ${formatPercentValue(reference.crrPct, 2)}`}
              />
            ) : null}
          </ul>
        </div>
      ) : null}
    </div>
  );
}
