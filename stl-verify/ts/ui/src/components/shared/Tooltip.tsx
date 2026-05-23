import { Tooltip } from '@archon-research/design-system';
import type { ReactNode } from 'react';

import { css } from '#styled-system/css';

type AppTooltipProps = {
  trigger: ReactNode;
  content: ReactNode;
  ariaLabel: string;
};

export function AppTooltip({ trigger, content, ariaLabel }: AppTooltipProps) {
  return (
    <Tooltip.Root positioning={{ placement: 'top', offset: { mainAxis: 8 } }}>
      <Tooltip.Trigger asChild>
        <button
          type="button"
          aria-label={ariaLabel}
          className={css({
            display: 'inline-flex',
            alignItems: 'center',
            border: '0',
            bg: 'transparent',
            color: 'inherit',
            font: 'inherit',
            lineHeight: 'inherit',
            p: '0',
            cursor: 'help',
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
            bg: 'rgba(15, 23, 42, 0.96)',
            px: '2.5',
            py: '2',
            boxShadow: 'lg',
            color: 'white',
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
