/**
 * Offline mocks for the stl-verify UI's API. Environment wiring lives behind the
 * `/browser` and `/node` subpaths; this entry imports neither.
 *
 * No `onReset`, because there is no mutable state: the app issues only GETs, so
 * every fixture is rebuilt per request from a pure seed. Reintroduce a
 * `createMockStore` the day a POST handler exists — that is the version whose
 * reset is load-bearing.
 */
import { setupMocks } from '@archon-research/http-client-msw';

import { getMockHandlers } from './handlers/index.ts';

export const mocks = setupMocks(getMockHandlers());

export { MOCK_ORIGIN } from './mock-api.ts';
export { SPARK_TX_HASH } from './fixtures/allocations.ts';
export {
  GROVE_MAINNET_PROXY,
  SPARK_BASE_PROXY,
  SPARK_MAINNET_PROXY,
  SPARK_VAULT,
  SPUSDS,
} from './fixtures/registry.ts';
