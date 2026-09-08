import { css } from '#styled-system/css';

// Not `SkeletonStack`: it fills its items with `surface.subtle` inline, takes no
// tone, and spreads its `style` prop onto the wrapper rather than the items — so
// on any `surface.subtle` ground (a metric card, a data-table detail cell) its
// placeholders are the same colour as what they sit on, and no composed class
// outranks an inline style. Filed upstream as ORB-383; delete this once the kit
// takes a tone.
const placeholderClassName = css({
  bg: 'border.subtle',
  borderRadius: 'sm',
  animation: 'pulse',
  // Inline-block so it can stand in for a word mid-sentence as well as fill a
  // slot of its own; a block element inside a `<span>` breaks the line.
  display: 'inline-block',
  verticalAlign: 'middle',
});

// A `<span>`, not a `<div>`: one call site stands this in for the timestamp
// inside the header's label span, and only phrasing content is valid there.
// `inline-block` makes it behave identically to the div everywhere else,
// including as a grid item, where the display is blockified anyway.

/**
 * A loading block that stays visible on a recessed surface, not just on the page.
 *
 * Sized by the caller because it stands in for something whose footprint is
 * known — a figure, a caption, a chart — and a placeholder the full width of its
 * container reads as a filled element rather than a loading one.
 */
export function Placeholder({
  width,
  height,
}: {
  width: string;
  height: number;
}) {
  return (
    <span
      className={placeholderClassName}
      // Sizes vary per slot, so they ride the style attribute: Panda generates
      // its classes at build time and cannot see a value passed in.
      style={{ width, height: `${height}px` }}
    />
  );
}
