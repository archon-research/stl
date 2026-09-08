/**
 * Node entry: msw's node interceptors over this repo's handlers.
 *
 * `listen()` has to run before `ui/src/shared/lib/api-client.ts` is imported; see the
 * README's "Gotchas worth knowing".
 */
import { setupMockServer } from '@archon-research/http-client-msw/node';
import type { MockServer } from '@archon-research/http-client-msw/node';

import { mocks } from './index.ts';

export const mockServer: MockServer = setupMockServer(mocks);
