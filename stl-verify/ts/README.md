# STL Verify TypeScript Workspace

This is an npm monorepo containing the frontend applications for STL Verify.

## Workspace Structure

- **ui/** - Vite + React + TypeScript application for the web UI

## Setup

```bash
# Install dependencies (from repository root)
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

## Building

```bash
# Build the UI for production
npm run build --workspace=@stl-verify/ui

# Or from ui directory
cd ui && npm run build
```

The built assets are output to `ui/dist/` and will be copied into the Python Docker image's `static/` directory during the container build process.

## Docker Integration

When building the Docker image for the Python app, the UI is automatically built and its assets are copied to `/app/app/static` in the final image for static hosting via Uvicorn.
