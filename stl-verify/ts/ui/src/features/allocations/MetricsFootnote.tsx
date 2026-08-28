import { css } from '#styled-system/css';

import {
  formatRatioPercent,
  parseNumericValue,
  riskModelCaptionSuffix,
} from '../../shared/lib/dashboard';
import type {
  PrimeRiskCapital,
  Provenance,
} from '../../shared/types/allocation';

/**
 * The provenance line that used to sit under the metrics band.
 *
 * INTENTIONALLY NOT RENDERED. It attributed the risk-capital figures to their
 * source and stated how much of the exposure STL's model priced; with the band
 * trimmed it read as a disclaimer on cards that no longer make that claim. The
 * logic is parked here whole rather than deleted, so switching it back on is an
 * import and one call — nothing has to be reconstructed from the history.
 *
 * The coverage figure is the point of the reference/both split: no model ran
 * under `reference`, so a bare "model-derived" would claim numbers STL's model
 * did not make, and under `both` Sky's figure wins wherever it reports one.
 */
export function MetricsFootnote({
  provenance,
  riskCapital,
  isSkeleton,
}: {
  provenance: Provenance;
  riskCapital: PrimeRiskCapital | null;
  isSkeleton: boolean;
}) {
  if (isSkeleton || riskCapital === null) {
    return null;
  }

  const modeledPct =
    parseNumericValue(riskCapital.prime_modeled_pct) !== null
      ? formatRatioPercent(riskCapital.prime_modeled_pct)
      : 'partial';

  return (
    <p className={css({ m: 0, fontSize: 'xs', color: 'text.muted' })}>
      {provenance === 'reference' ? (
        // No model ran, so the coverage figure below would read as "STL priced
        // all of this" when nothing of STL's did. Attribute the figures to
        // their source instead.
        <>
          Reported by Sky&apos;s Star Agents Risk Capital &amp; Requirements
          Monitor · not STL&apos;s model
        </>
      ) : provenance === 'both' ? (
        // Sky's figure wins wherever it reports one, so a bare "model-derived"
        // would claim numbers STL's model did not make.
        <>
          Sky&apos;s published figures where reported, else model-derived (
          {riskCapital.model}
          {riskModelCaptionSuffix(riskCapital.model)}) · {modeledPct} of
          exposure modeled by STL
        </>
      ) : (
        <>
          Model-derived ({riskCapital.model}
          {riskModelCaptionSuffix(riskCapital.model)}) · {modeledPct} of
          exposure modeled
        </>
      )}
    </p>
  );
}
