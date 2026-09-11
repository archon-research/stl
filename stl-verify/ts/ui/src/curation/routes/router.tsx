import {
  createHashHistory,
  createRootRoute,
  createRoute,
  createRouter,
  Navigate,
  notFound,
} from '@tanstack/react-router';

import { resourceByKey } from '../schema/registry.ts';
import { ClassifyView } from '../ui/ClassifyView.tsx';
import { CurationLayout } from '../ui/CurationLayout.tsx';
import { EdgeCreateView } from '../ui/EdgeCreateView.tsx';
import { IngestView } from '../ui/IngestView.tsx';
import { NodeDetailView } from '../ui/NodeDetailView.tsx';
import { ResourceCreateView } from '../ui/ResourceCreateView.tsx';
import { ResourceListView } from '../ui/ResourceListView.tsx';
import { WorklistView } from '../ui/WorklistView.tsx';

/**
 * The route tree, generated from the registry.
 *
 * Three parameterised routes cover every resource, rather than three routes per
 * resource: `/:resourceKey`, `/:resourceKey/new`, `/:resourceKey/:nodeId`. The
 * alternative — building a route object per registry row in a loop — produces
 * the same URLs but 24 route objects, each of which TanStack Router has to type
 * and match separately, and none of which says anything the registry did not.
 *
 * The trade is that the resource key is a runtime lookup, so an unknown key is a
 * 404 raised in the loader instead of a route that was never registered. That is
 * the honest failure mode anyway: the registry is data, so its keys cannot be
 * known to the type system without generating a literal union from it, and a
 * generated union is a build step this spike does not need.
 */
const rootRoute = createRootRoute({ component: CurationLayout });

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/',
  component: () => (
    <Navigate to="/$resourceKey" params={{ resourceKey: 'securities' }} />
  ),
});

const ingestRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/workflow/import-prices',
  component: IngestView,
});

const worklistRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/worklist',
  component: WorklistView,
});

const classifyRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/workflow/classify',
  component: ClassifyView,
});

/**
 * Resolves the registry row, or 404s.
 *
 * Takes the key as a plain string rather than the loader's context object: an
 * annotated `{ params: { resourceKey: string } }` parameter pins the route's
 * param type to exactly that shape, which silently drops `$nodeId` from the
 * detail route and leaves `useParams()` without it.
 */
function resolveResource(resourceKey: string) {
  const resource = resourceByKey(resourceKey);
  if (resource === undefined) {
    throw notFound();
  }

  return { resource };
}

const resourceListRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/$resourceKey',
  loader: ({ params }) => resolveResource(params.resourceKey),
  component: function ResourceList() {
    const { resource } = resourceListRoute.useLoaderData();

    return <ResourceListView resource={resource} />;
  },
});

const resourceCreateRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/$resourceKey/new',
  loader: ({ params }) => resolveResource(params.resourceKey),
  component: function ResourceCreate() {
    const { resource } = resourceCreateRoute.useLoaderData();

    // The one resource whose endpoint pickers depend on a sibling field, so it
    // takes the next rung of the override ladder. `EdgeCreateView` explains why.
    return resource.store === 'edge' ? (
      <EdgeCreateView />
    ) : (
      <ResourceCreateView resource={resource} />
    );
  },
});

const nodeDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/$resourceKey/$nodeId',
  loader: ({ params }) => resolveResource(params.resourceKey),
  component: function NodeDetail() {
    const { resource } = nodeDetailRoute.useLoaderData();
    const { nodeId } = nodeDetailRoute.useParams();

    return <NodeDetailView resource={resource} nodeId={nodeId} />;
  },
});

const routeTree = rootRoute.addChildren([
  indexRoute,
  worklistRoute,
  classifyRoute,
  ingestRoute,
  // `/new` before `/$nodeId`: both match a two-segment path, and the literal has
  // to be tried first or "new" is read as a node id.
  resourceCreateRoute,
  nodeDetailRoute,
  resourceListRoute,
]);

export const router = createRouter({
  routeTree,
  /**
   * Hash history, because the entry is a *file* — `/curation.html` — and not a
   * directory.
   *
   * With browser history the initial pathname is `/curation.html`, which matches
   * no route, and every generated link would point at a path (`/securities`)
   * that Vite's SPA fallback answers with the *main* app's `index.html`. A
   * `basepath` does not fix it either: `/curation.html/securities` is not a path
   * the dev server serves.
   *
   * The cost is `#/securities` in the address bar, which is the right price for
   * an entry that is deliberately excluded from the build. When this app earns
   * its own route (or its own directory entry), this line is what changes.
   */
  history: createHashHistory(),
  defaultNotFoundComponent: () => (
    <p>No such resource. Pick one from the sidebar.</p>
  ),
});

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}
