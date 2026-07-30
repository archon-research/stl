import { Portal, Tooltip } from '@archon-research/design-system';
import type { ReactNode } from 'react';

import { css } from '#styled-system/css';

type AppTooltipProps = {
  trigger: ReactNode;
  content: ReactNode;
  ariaLabel: string;
  fullWidth?: boolean;
};

type TruncatedLabelProps = {
  label: string;
  className?: string;
};

// These wrappers are not a style preference. The design system re-exports Ark's
// Tooltip, which is headless, and ships no `tooltip` recipe — so an unwrapped
// Tooltip.Content renders as unstyled, unpositioned text over the page. Delete
// this styling once a `tooltip` recipe ships.
const tooltipContentClassName = css({
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
});

const tooltipPositioning = {
  placement: 'top',
  offset: { mainAxis: 8 },
} as const;

export function AppTooltip({
  trigger,
  content,
  ariaLabel,
  fullWidth = false,
}: AppTooltipProps) {
  return (
    <Tooltip.Root positioning={tooltipPositioning}>
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
        <Tooltip.Content className={tooltipContentClassName}>
          {content}
        </Tooltip.Content>
      </Tooltip.Positioner>
    </Tooltip.Root>
  );
}

// Companion to `AppTooltip` for inline text that a caller has ellipsized. It
// exists because `AppTooltip`'s trigger is a 44px-tall <button>: the usual host
// for a truncated label is a table header, which DataTable already wraps in its
// own sortable <button>, so reusing `AppTooltip` there would nest a button in a
// button and blow out the header height.
//
// The <span> trigger is therefore not focusable and the tooltip is pointer-only.
// That is acceptable here and not an information loss: the full label is in the
// DOM (CSS does the truncating, so assistive tech reads all of it), and the
// enclosing sort button remains the keyboard affordance for the header.
export function TruncatedLabel({ label, className }: TruncatedLabelProps) {
  return (
    <Tooltip.Root positioning={tooltipPositioning}>
      <Tooltip.Trigger asChild>
        <span className={className}>{label}</span>
      </Tooltip.Trigger>
      {/* Portalled, unlike AppTooltip's inline positioner, because the host is a
          table header: DataTable's root scrolls (`overflowX: auto`), which clips
          a tooltip placed above the header row, and the header button would
          otherwise contain a <div>. */}
      <Portal>
        <Tooltip.Positioner>
          <Tooltip.Content className={tooltipContentClassName}>
            {label}
          </Tooltip.Content>
        </Tooltip.Positioner>
      </Portal>
    </Tooltip.Root>
  );
}
