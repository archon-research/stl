import { Tooltip } from '@archon-research/design-system';
import type { ReactNode } from 'react';

import { css } from '#styled-system/css';

type AppTooltipProps = {
  trigger: ReactNode;
  content: ReactNode;
  ariaLabel: string;
  fullWidth?: boolean;
};

// This wrapper is not a style preference. The design system re-exports Ark's
// Tooltip, which is headless, and ships no `tooltip` recipe — so an unwrapped
// Tooltip.Content renders as unstyled, unpositioned text over the page. The
// correct appearance also needs `overlay.tooltip` and `text.inverse`, which are
// local tokens with no preset equivalent (a tooltip fill is a semi-transparent
// always-dark scrim, so it cannot be a step on the opaque surface ramp).
// Reported upstream; delete the Content styling once a `tooltip` recipe ships.
export function AppTooltip({
  trigger,
  content,
  ariaLabel,
  fullWidth = false,
}: AppTooltipProps) {
  return (
    <Tooltip.Root positioning={{ placement: 'top', offset: { mainAxis: 8 } }}>
      <Tooltip.Trigger asChild>
        <button
          type="button"
          aria-label={ariaLabel}
          className={css({
            display: 'inline-flex',
            alignItems: 'center',
            minHeight: '11',
            border: '0',
            bg: 'transparent',
            color: 'inherit',
            font: 'inherit',
            lineHeight: 'inherit',
            px: '1',
            py: '0.5',
            cursor: 'help',
            width: fullWidth ? '100%' : 'auto',
            justifyContent: fullWidth ? 'stretch' : 'flex-start',
          })}
        >
          {trigger}
        </button>
      </Tooltip.Trigger>
      <Tooltip.Positioner>
        <Tooltip.Content
          className={css({
            maxW: '20rem',
            borderRadius: 'sm',
            borderStyle: 'solid',
            borderWidth: '1px',
            borderColor: 'border.default',
            bg: 'overlay.tooltip',
            px: '2.5',
            py: '2',
            boxShadow: 'lg',
            color: 'text.inverse',
            fontSize: 'xs',
            lineHeight: '1.4',
            zIndex: 'tooltip',
          })}
        >
          {content}
        </Tooltip.Content>
      </Tooltip.Positioner>
    </Tooltip.Root>
  );
}
