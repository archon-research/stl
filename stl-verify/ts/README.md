# STL Verify TypeScript Workspace

This is an npm monorepo containing the frontend applications for STL Verify.

## Workspace Structure

- **ui/** - Vite + React + TypeScript application for the web UI
- **mocks/** - `@stl-verify/mocks`: typed offline mocks for the UI's API, used by
  dev-with-no-backend and by the mock self-test. See
  [mocks/README.md](mocks/README.md).

## Setup

```bash
# Install dependencies (from the workspace root: stl-verify/ts)
npm install

# Or from this directory
npm install
```

## Development

```bash
# Start dev server for UI
npm run dev --workspace=@stl-verify/ui

# Or from ui directory
cd ui && npm run dev
```

### Offline, with no backend

```bash
VITE_API_MOCKS=1 npm run dev --workspace=@stl-verify/ui
```

Every `/v1` call is answered in the browser from `@stl-verify/mocks`. `API_URL`
is not required in this mode — there is no proxy target — and neither msw nor the
fixtures reach a normal build.

## Building

```bash
# Build the UI for production
npm run build --workspace=@stl-verify/ui

# Or from ui directory
cd ui && npm run build
```

The built assets are output to `ui/dist/` and will be copied into the Python Docker image's `static/` directory during the container build process.

## Local AI plugin setup (ts-scoped)

Use plugin setup from this `ts` folder when you want plugin behavior scoped to this consumer workspace only.

Claude (project-scoped):

```bash
claude plugin marketplace add https://github.com/archon-research/uikit.git#2c5f444eaf6debbd1ebdbc1b969e9c24e7e0fe81
claude plugin install uikit-agent-marketplace@uikit-plugins --scope project
```

Copilot workaround (until branch is merged):

```bash
make copilot-plugin-sync-from-sha
```

This target does a sparse, shallow clone and copies only
`packages/agent-marketplace/copilot-plugin` into `.copilot-marketplace/uikit-agent-marketplace`,
then updates/installs `uikit-agent-marketplace@uikit-plugins`.

Defaults:

- `UIKIT_PLUGIN_SHA=2c5f444eaf6debbd1ebdbc1b969e9c24e7e0fe81`

If you want commit pinning:

```bash
make copilot-plugin-sync-from-sha UIKIT_PLUGIN_SHA=<full-commit-sha>
```

## Docker Integration

When building the Docker image for the Python app, the UI is automatically built and its assets are copied to `/app/app/static` in the final image for static hosting via Uvicorn.
