import { Badge } from '@archon-research/design-system';

type BadgeTone = 'green' | 'yellow' | 'red' | 'neutral';

type StatusBadgeProps = {
  tone: BadgeTone;
  label: string;
  className?: string;
};

function getToneStyles(tone: BadgeTone) {
  switch (tone) {
    case 'green':
      return 'success';
    case 'yellow':
      return 'warning';
    case 'red':
      return 'danger';
    case 'neutral':
    default:
      return 'neutral';
  }
}

export function StatusBadge({ tone, label, className }: StatusBadgeProps) {
  const badgeTone = getToneStyles(tone);

  return (
    <Badge tone={badgeTone} className={className}>
      {label}
    </Badge>
  );
}
