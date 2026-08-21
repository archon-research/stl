import { Badge, type BadgeColorPalette } from '@archon-research/design-system';

import type { UsdTone } from '../../lib/dashboard';

type StatusBadgeProps = {
  tone: UsdTone;
  label: string;
  className?: string;
};

// The app's tone vocabulary is named for the colour it wants, so it maps
// straight onto a Badge hue rather than going through the deprecated `tone`
// prop. `yellow` resolves to `amber`: the design system has no yellow palette,
// and this is the same hue Badge's own deprecated `warning` tone maps to.
const TONE_COLOR_PALETTE = {
  green: 'green',
  yellow: 'amber',
  red: 'red',
  neutral: 'neutral',
} as const satisfies Record<UsdTone, BadgeColorPalette>;

export function StatusBadge({ tone, label, className }: StatusBadgeProps) {
  return (
    <Badge
      colorPalette={TONE_COLOR_PALETTE[tone]}
      variant="subtle"
      className={className}
    >
      {label}
    </Badge>
  );
}
