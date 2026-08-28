import { Portal, Tooltip } from '@archon-research/design-system';

import { tooltip } from '#styled-system/recipes';

type TruncatedLabelProps = {
  label: string;
  className?: string;
};

// This wrapper is not a style preference. The design system re-exports Ark's
// Tooltip headless, so an unwrapped Tooltip.Content renders as unstyled,
// unpositioned text over the page. The bubble surface is the upstream `tooltip`
// recipe; what stays local is the trigger shape and the positioning.
const tooltipContentClassName = tooltip();

const tooltipPositioning = {
  placement: 'top',
  offset: { mainAxis: 8 },
} as const;

// For inline text that a caller has ellipsized. The trigger is a <span> rather
// than the usual 44px-tall <button>: the host for a truncated label is a table
// header, which DataTable already wraps in its own sortable <button>, so a
// button trigger would nest a button in a button and blow out the header height.
//
// The <span> is therefore not focusable and the tooltip is pointer-only. That is
// acceptable here and not an information loss: the full label is in the DOM (CSS
// does the truncating, so assistive tech reads all of it), and the enclosing sort
// button remains the keyboard affordance for the header.
export function TruncatedLabel({ label, className }: TruncatedLabelProps) {
  return (
    <Tooltip.Root positioning={tooltipPositioning}>
      <Tooltip.Trigger asChild>
        <span className={className}>{label}</span>
      </Tooltip.Trigger>
      {/* Portalled rather than positioned inline because the host is a table
          header: DataTable's root scrolls (`overflowX: auto`), which clips a
          tooltip placed above the header row, and the header button would
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
