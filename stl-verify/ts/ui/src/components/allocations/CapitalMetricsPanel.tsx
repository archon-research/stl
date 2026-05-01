import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

import { formatUsdValue, formatRatioPercent, parseNumericValue } from '../../lib/dashboard';
import type { CapitalMetrics } from '../../types/allocation';
import { SummaryMetric } from '../shared';

type CapitalMetricsPanelProps = {
  metrics: CapitalMetrics;
};

type RatioTone = 'safe' | 'warning' | 'danger';

function getRatioTone(ratio: string): RatioTone {
  const numeric = parseNumericValue(ratio);
  if (numeric === null) {
    return 'safe';
  }
  if (numeric < 0.8) {
    return 'safe';
  }
  if (numeric < 1.0) {
    return 'warning';
  }
  return 'danger';
}

const RATIO_TONE_STYLES: Record<RatioTone, { value: string; badge: string }> = {
  safe: {
    value: 'text.success',
    badge: 'bg.success',
  },
  warning: {
    value: 'text.warning',
    badge: 'bg.warning',
  },
  danger: {
    value: 'text.error',
    badge: 'bg.error',
  },
};

export function CapitalMetricsPanel({ metrics }: CapitalMetricsPanelProps) {
  const ratioTone = getRatioTone(metrics.risk_to_capital_ratio);
  const toneStyles = RATIO_TONE_STYLES[ratioTone];
  const ratioDisplay = formatRatioPercent(metrics.risk_to_capital_ratio);

  return (
    <div
      className={css({
        borderRadius: 'md',
        borderStyle: 'solid',
        borderWidth: '1px',
        borderColor: 'border.subtle',
        bg: 'surface.default',
        p: { base: '4', md: '5' },
      })}
    >
      <div className={flex({ align: 'center', justify: 'space-between', mb: '3', gap: '3', wrap: 'wrap' })}>
        <span
          className={css({
            fontSize: 'xs',
            fontWeight: 'semibold',
            letterSpacing: '0.14em',
            textTransform: 'uppercase',
            color: 'text.muted',
          })}
        >
          Capital Metrics
        </span>
        <span
          className={css({
            display: 'inline-flex',
            alignItems: 'center',
            gap: '1.5',
            fontSize: 'xs',
            fontWeight: 'semibold',
            color: toneStyles.value,
          })}
        >
          <span
            className={css({
              display: 'inline-block',
              width: '2',
              height: '2',
              borderRadius: 'full',
              bg: toneStyles.badge,
            })}
          />
          Risk-to-capital {ratioDisplay}
        </span>
      </div>
      <div
        className={css({
          display: 'grid',
          gridTemplateColumns: {
            base: 'repeat(2, minmax(0, 1fr))',
            md: 'repeat(4, minmax(0, 1fr))',
          },
          gap: '3',
        })}
      >
        <SummaryMetric
          label="Risk Capital"
          value={formatUsdValue(metrics.risk_capital)}
          detail="Onchain allocation exposure"
        />
        <SummaryMetric
          label="Total Capital"
          value={formatUsdValue(metrics.total_capital)}
          detail="Prime total buffer"
        />
        <SummaryMetric
          label="Capital Buffer"
          value={formatUsdValue(metrics.capital_buffer)}
          detail="Available buffer"
        />
        <SummaryMetric
          label="First Loss Capital"
          value={formatUsdValue(metrics.first_loss_capital)}
          detail="First-loss tranche"
        />
      </div>
      {metrics.validation_note ? (
        <p
          className={css({
            m: 0,
            mt: '3',
            fontSize: 'xs',
            color: 'text.muted',
            fontStyle: 'italic',
          })}
        >
          {metrics.validation_note}
        </p>
      ) : null}
    </div>
  );
}
