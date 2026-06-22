import type { ReactNode } from 'react';

import { css } from '#styled-system/css';
import { flex } from '#styled-system/patterns';

type SummaryMetricProps = {
  label: string;
  value: ReactNode;
  detail?: ReactNode;
  className?: string;
};

export function SummaryMetric({
  label,
  value,
  detail,
  className,
}: SummaryMetricProps) {
  return (
    <div
      className={
        className ??
        css({
          borderRadius: 'md',
          borderStyle: 'solid',
          borderWidth: '1px',
          borderColor: 'border.subtle',
          bg: 'surface.default',
          p: '3',
        })
      }
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
      <div
        className={flex({
          align: 'center',
          gap: '2',
          wrap: 'wrap',
          fontSize: 'lg',
          fontWeight: 'semibold',
          color: 'text.strong',
          minWidth: 0,
          overflowWrap: 'anywhere',
          wordBreak: 'break-word',
        })}
      >
        {value}
      </div>
      {detail ? (
        <div
          className={css({
            fontSize: 'xs',
            color: 'text.muted',
            overflowWrap: 'anywhere',
            wordBreak: 'break-word',
          })}
        >
          {detail}
        </div>
      ) : null}
    </div>
  );
}
