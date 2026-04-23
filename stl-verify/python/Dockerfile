# syntax=docker/dockerfile:1.7

# Build UI
FROM node:22-alpine AS ui-builder

WORKDIR /ui

COPY ts/package.json ts/package-lock.json ts/.npmrc ./
COPY ts/ui/package.json ui/package.json

RUN --mount=type=secret,id=uikit_token,env=UIKIT_GH_PACKAGES_READ_TOKEN \
    npm ci

COPY ts/ui ui/

RUN npm run build --workspace=@stl-verify/ui

# Build Python dependencies
FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.11.5 /uv /uvx /bin/

WORKDIR /app

# Install dependencies first for better layer caching
COPY python/pyproject.toml python/uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy application code and install the project
COPY python/.env.default ./.env.default
COPY python/app/ app/
COPY python/suraf/ suraf/
RUN uv sync --frozen --no-dev

FROM python:3.12-slim

ARG GIT_COMMIT=unknown
ENV GIT_COMMIT=${GIT_COMMIT}

RUN groupadd --system --gid 10001 appuser \
    && useradd --system --uid 10001 --gid 10001 --no-create-home appuser

WORKDIR /app

# Copy the virtual environment and app from builder
COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv
COPY --from=builder --chown=appuser:appuser /app/app /app/app
COPY --from=builder --chown=appuser:appuser /app/suraf /app/suraf
COPY --from=builder --chown=appuser:appuser /app/.env.default /app/.env.default

# Copy built UI assets for static hosting
COPY --from=ui-builder --chown=appuser:appuser /ui/ui/dist /app/app/static

USER 10001

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
