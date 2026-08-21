/** Every endpoint `ui/src/lib/api.ts` calls, in one array. */
import type { MockHandler } from '@archon-research/http-client-msw';

import { allocationHandlers } from './allocations.ts';
import { eventHandlers } from './events.ts';
import { registryHandlers } from './registry.ts';
import { riskHandlers } from './risk.ts';
import { seriesHandlers } from './series.ts';

export function getMockHandlers(): MockHandler[] {
  return [
    ...registryHandlers(),
    ...allocationHandlers(),
    ...seriesHandlers(),
    ...riskHandlers(),
    ...eventHandlers(),
  ];
}
