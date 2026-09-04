import { css } from '#styled-system/css';

import { formatUsdValue } from '../../shared/lib/dashboard';
import type { AllocationGridRow, RiskFetchState } from './allocationGridRows';

export function AllocationRiskCapitalCell({
  risk,
}: {
  risk: AllocationGridRow['risk'];
}) {
  if (risk.chainMismatch) {
    return (
      <p
        title="Risk capital is not yet available for non-mainnet allocations."
        className={css({ m: '0', fontSize: 'sm', color: 'text.muted' })}
      >
        Not yet available
      </p>
    );
  }

  const unsettled = riskFetchPlaceholder(risk.state);
  if (unsettled) {
    return (
      <p
        title={unsettled.title}
        className={css({ m: '0', fontSize: 'sm', color: 'text.muted' })}
      >
        {unsettled.label}
      </p>
    );
  }

  if (risk.riskCapitalUsd === null) {
    return (
      <p className={css({ m: '0', fontSize: 'sm', color: 'text.muted' })}>
        n/a
      </p>
    );
  }

  return (
    <p
      title={derivedRiskTitle(risk)}
      className={css({
        m: '0',
        fontSize: 'sm',
        fontWeight: 'semibold',
        color: 'text.strong',
      })}
    >
      {formatUsdValue(risk.riskCapitalUsd)}
    </p>
  );
}

/**
 * A tooltip, not a chip: under the composite view most rows carry the
 * model's own ratio (`preferModelRiskFigure`), so a marker here is the
 * exception rather than the rule — unlike the Asset column's sole-reporter
 * badge, which is why that one *is* a chip.
 */
export function riskProvenanceTitle(risk: AllocationGridRow['risk']): string {
  return risk.fromReference ? 'Legacy figure' : 'Verify model figure';
}

/**
 * RRC and its share are derived here from the ratio and exposure on the row, so
 * neither is anyone's published figure — `riskProvenanceTitle` would claim
 * upstream stands behind a number it never issued. It names the ratio's source
 * and the arithmetic instead, which is what a reader checking the row against
 * its neighbours needs.
 */
export function derivedRiskTitle(risk: AllocationGridRow['risk']): string {
  const ratio = risk.fromReference ? "Legacy's CRR" : "Verify's CRR";
  return `Derived: ${ratio} x the exposure shown`;
}

/**
 * The muted stand-in for a risk cell whose figure is not settled, or null once
 * it is. Distinct from `n/a`: that asserts no model applies, which only the
 * settled state can claim.
 */
function riskFetchPlaceholder(
  state: RiskFetchState,
): { label: string; title: string } | null {
  if (state === 'loading') {
    return { label: '…', title: 'Loading risk capital' };
  }
  if (state === 'error') {
    return {
      label: 'unavailable',
      title: 'The risk-capital request failed; retry from the metrics band.',
    };
  }
  return null;
}

export function AllocationRatioCell({
  value,
  format,
  state,
  title,
}: {
  value: number | null;
  format: (value: number | null) => string;
  state: RiskFetchState;
  title?: string;
}) {
  const unsettled = riskFetchPlaceholder(state);
  if (unsettled) {
    return (
      <p
        title={unsettled.title}
        className={css({ m: '0', fontSize: 'sm', color: 'text.muted' })}
      >
        {state === 'loading' ? unsettled.label : '—'}
      </p>
    );
  }

  return (
    <p
      title={value === null ? undefined : title}
      className={css({
        m: '0',
        fontSize: 'sm',
        fontWeight: 'semibold',
        color: value === null ? 'text.muted' : 'text.strong',
      })}
    >
      {format(value)}
    </p>
  );
}
