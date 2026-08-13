# syntax=docker/dockerfile:1.10
ARG PYTHON_VERSION

# Build UI on the native builder architecture — the output is static JS/HTML/CSS,
# so there's no reason to run npm under QEMU arm64 emulation (which crashes node).
FROM --platform=$BUILDPLATFORM node:24-alpine AS ui-builder

WORKDIR /ui

COPY ts/package.json ts/package-lock.json ts/.npmrc ./
COPY ts/ui/package.json ui/package.json

RUN npm ci

COPY ts/ui ui/

RUN npm run prepare --workspace=@stl-verify/ui

RUN npm run build --workspace=@stl-verify/ui

# Build Python dependencies
FROM python:${PYTHON_VERSION}-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.11.5 /uv /uvx /bin/

WORKDIR /app

# Install dependencies first for better layer caching
COPY python/pyproject.toml python/uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy application code and install the project
COPY python/.env.default ./.env.default
COPY python/app/ app/
COPY python/suraf/ suraf/
COPY python/cli/ cli/
RUN uv sync --frozen --no-dev

FROM python:${PYTHON_VERSION}-slim

ARG GIT_COMMIT=unknown
ENV GIT_COMMIT=${GIT_COMMIT}

# groupadd/useradd live in the passwd package, which the hardening step below
# removes — user creation must stay above it.
RUN groupadd --system --gid 10001 appuser \
    && useradd --system --uid 10001 --gid 10001 --no-create-home appuser

# The -slim base tag lags Debian security releases, so pull in OS package
# security updates at build time. Keep this after the per-commit GIT_COMMIT
# ENV: its value busts the cache each release, so the upgrade re-runs on
# every build instead of replaying a stale cached layer.
#
# Then strip OS packages the service never uses at runtime (perl, bash,
# ncurses, util-linux, gzip, tar, sed, coreutils, apt, ...). They exist only
# because Debian marks them Essential, and they are a recurring source of
# critical/high CVE findings with no trixie fix available. dpkg metadata stays
# accurate for everything that remains, so image scanners keep working.
# Constraints baked into the ordering:
#   - anything needing apt-get or extra packages must run above this step;
#     after it there is no apt, and no dpkg runs are possible (dpkg refuses to
#     start once rm/tar are gone — which is also why coreutils and tar must go
#     in the final dpkg invocation).
#   - --force-depends appears only in that final invocation, solely for the
#     dangling dpkg -> coreutils/tar edges; earlier invocations fail loudly if
#     a future base image adds a real dependency on a purged package.
#   - python's _uuid extension loses libuuid1 and falls back to the pure-python
#     uuid implementation; /bin/sh (dash), findutils, and grep remain for
#     debugging, python one-liners cover the rest.
RUN apt-get update \
    && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/* \
    && dpkg --purge --force-remove-essential --force-remove-protected \
        perl-base bash ncurses-bin ncurses-base libncursesw6 libreadline8t64 libtinfo6 \
        gzip login mount util-linux bsdutils libblkid1 libmount1 libsmartcols1 \
        liblastlog2-2 libuuid1 \
    && dpkg --purge --force-remove-essential --force-remove-protected apt adduser passwd sed \
    && dpkg --purge --force-remove-essential --force-depends coreutils tar libacl1

WORKDIR /app

# Copy the virtual environment and app from builder
COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv
COPY --from=builder --chown=appuser:appuser /app/app /app/app
COPY --from=builder --chown=appuser:appuser /app/suraf /app/suraf
COPY --from=builder --chown=appuser:appuser /app/.env.default /app/.env.default
COPY --chown=appuser:appuser python/cli /app/cli

# Copy built UI assets for static hosting
COPY --from=ui-builder --chown=appuser:appuser /ui/ui/dist /app/app/static

USER 10001

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
