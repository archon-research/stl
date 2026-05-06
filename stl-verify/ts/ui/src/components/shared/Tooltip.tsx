import { Tooltip } from '@base-ui/react/tooltip';
import type { ReactNode } from 'react';

import { css } from '#styled-system/css';

type AppTooltipProps = {
  trigger: ReactNode;
  content: ReactNode;
  ariaLabel: string;
};

export function AppTooltip({ trigger, content, ariaLabel }: AppTooltipProps) {
  return (
    <Tooltip.Provider>
      <Tooltip.Root>
        <Tooltip.Trigger
          aria-label={ariaLabel}
          render={
            <button
              type="button"
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
            />
          }
        >
          {trigger}
        </Tooltip.Trigger>
        <Tooltip.Portal>
          <Tooltip.Positioner side="top" sideOffset={8}>
            <Tooltip.Popup
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
            </Tooltip.Popup>
          </Tooltip.Positioner>
        </Tooltip.Portal>
      </Tooltip.Root>
    </Tooltip.Provider>
  );
}
