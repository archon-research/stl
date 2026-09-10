import {
  createApiClient,
  createQueryApi,
} from '@archon-research/http-client-react';

import type { paths } from './contract.ts';

/**
 * The curation app's typed query surface.
 *
 * `shared/lib/api-client.ts` says of its own: *"No tag vocabulary: tags exist to
 * be invalidated by mutations, and this app issues none. Add one here when the
 * first write lands, not before."* This is that first write, so this client is
 * where the vocabulary lands — and it is a second client rather than an addition
 * to that one, because it is bound to a different (and, for now, invented)
 * contract.
 */
type ApiPaths = { [Path in keyof paths]: paths[Path] };

const apiClient = createApiClient<ApiPaths>('');

/**
 * The cache tags.
 *
 * Coarse on purpose. An append to one node invalidates the node reads wholesale
 * rather than one key, because a node append can change what a *different*
 * read returns: closing a window re-points the current row, and a new
 * BELONGS_TO edge changes which shapes are active and therefore which validity
 * gaps every list shows. Precise invalidation here would need the resolution
 * rules the database owns, so the client stays blunt and correct.
 */
const CURATION_TAGS = ['nodes', 'edges', 'validity', 'registers'] as const;

export type CurationTag = (typeof CURATION_TAGS)[number];

export const api = createQueryApi<ApiPaths, CurationTag>(apiClient, {
  tags: CURATION_TAGS,
});
