import type { ReactNode } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';

import { SummaryMetric } from './SummaryMetric';

const render = (detail?: ReactNode) =>
  renderToStaticMarkup(
    <SummaryMetric label="Total capital" value="—" detail={detail} />,
  );

describe('SummaryMetric', () => {
  // The tile's `sub` slot brings its own grid gap, so a detail that renders
  // nothing has to leave the markup identical to no detail at all.
  it('emits no detail slot for a falsy detail', () => {
    const bare = render();

    expect(render(0)).toBe(bare);
    expect(render('')).toBe(bare);
  });

  it('emits the detail slot for a detail that has something to say', () => {
    const withDetail = render('7 chains');

    expect(withDetail).toContain('7 chains');
    expect(withDetail).not.toBe(render());
  });
});
