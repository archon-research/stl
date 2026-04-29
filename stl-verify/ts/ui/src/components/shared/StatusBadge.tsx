import { css } from '#styled-system/css';

type BadgeTone = 'green' | 'yellow' | 'red' | 'neutral';

type StatusBadgeProps = {
  tone: BadgeTone;
  label: string;
  className?: string;
};

function getToneStyles(tone: BadgeTone) {
  switch (tone) {
    case 'green':
      return {
        bg: { _dark: 'green.950', base: 'green.50' },
        color: { _dark: 'green.200', base: 'green.700' },
      };
    case 'yellow':
      return {
        bg: { _dark: 'yellow.950', base: 'yellow.50' },
        color: { _dark: 'yellow.200', base: 'yellow.800' },
      };
    case 'red':
      return {
        bg: { _dark: 'red.900', base: 'red.50' },
        color: { _dark: 'red.100', base: 'red.700' },
      };
    case 'neutral':
    default:
      return {
        bg: { _dark: 'gray.800', base: 'gray.100' },
        color: { _dark: 'gray.200', base: 'gray.600' },
      };
  }
}

export function StatusBadge({ tone, label, className }: StatusBadgeProps) {
  const toneStyles = getToneStyles(tone);

  return (
    <span
      className={
        className ??
        css({
          display: 'inline-flex',
          alignItems: 'center',
          borderRadius: 'sm',
          bg: toneStyles.bg,
          color: toneStyles.color,
          fontSize: 'xs',
          fontWeight: 'semibold',
          px: '3',
          py: '1.5',
        })
      }
    >
      {label}
    </span>
  );
}
