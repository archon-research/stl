import {
  createApiClient,
  createQueryApi,
} from '@archon-research/http-client-react';

import type { paths } from '../generated/openapi-types';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '';

/**
 * The generated `paths`, as a shape `createQueryApi` will accept.
 *
 * `openapi-typescript` emits `paths` as an interface, and an interface carries
 * no implicit index signature — so it does not satisfy the `Record<string, …>`
 * bound, and inference falls back to it. The symptom is every `queryOptions`
 * call reporting its own path as `not assignable to never`.
 */
type ApiPaths = { [Path in keyof paths]: paths[Path] };

/**
 * Built at module scope, which is load-bearing for the offline mocks:
 * `openapi-fetch` snapshots `globalThis.fetch` here, so msw has to be listening
 * before anything imports this. See `mocks/README.md`, "Gotchas worth knowing".
 */
const apiClient = createApiClient<ApiPaths>(API_BASE_URL);

/**
 * The typed query surface, derived from the generated `paths`.
 *
 * No tag vocabulary: tags exist to be invalidated by mutations, and this app
 * issues none. Add one here when the first write lands, not before.
 */
export const api = createQueryApi(apiClient);
