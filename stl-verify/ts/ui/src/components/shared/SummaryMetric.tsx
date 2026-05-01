import { css } from '#styled-system/css';

type SummaryMetricProps = {
  label: string;
  value: string;
  detail?: string;
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
