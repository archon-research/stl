import {
  Badge,
  type BadgeColorPalette,
  type BadgeVariant,
} from '@archon-research/design-system';

type BadgeTone = 'green' | 'yellow' | 'red' | 'neutral';

type StatusBadgeProps = {
  tone: BadgeTone;
  label: string;
  variant?: BadgeVariant;
  className?: string;
};

// The app's tone vocabulary is named for the colour it wants, so it maps
// straight onto a Badge hue. This replaces a double indirection that went
// green -> 'success' -> back to a palette inside Badge via the deprecated
// `tone` prop, which also pinned every badge to variant="subtle".
// `yellow` resolves to `amber`: the design system has no yellow palette, and
// this is the same hue Badge's own deprecated `warning` tone now maps to.
const TONE_COLOR_PALETTE = {
  green: 'green',
  yellow: 'amber',
  red: 'red',
  neutral: 'neutral',
} as const satisfies Record<BadgeTone, BadgeColorPalette>;

export function StatusBadge({
  tone,
  label,
  variant = 'subtle',
  className,
}: StatusBadgeProps) {
  return (
    <Badge
      colorPalette={TONE_COLOR_PALETTE[tone]}
      variant={variant}
      className={className}
    >
      {label}
    </Badge>
  );
}
