# syntax=docker/dockerfile:1
# =============================================================================
# Unified Go service Dockerfile
# =============================================================================
# One multi-stage build shared by every Go service and cronjob. Pick the target
# with build args:
#   CMD_PATH  Go package to build, relative to the module root (e.g. cmd/base/watcher)
#   BIN       output binary name. Kept per-service on purpose: the k8s liveness
#             probes match the process with `pgrep -f <BIN>`, so the running
#             process name must stay the service-specific binary name.
# Defaults build the watcher, so a bare `docker build .` still works.
#
# IMPORTANT: the versioning args (GIT_COMMIT/GIT_BRANCH/BUILD_TIME) and the
# target args (CMD_PATH/BIN) are declared AFTER `go mod download`, so a changing
# BUILD_TIME never invalidates the cached dependency layers. Do not move them up.

# -----------------------------------------------------------------------------
# Build Stage
# -----------------------------------------------------------------------------
ARG GO_VERSION
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS builder

# Cross-compilation arg only; it is stable per target platform so it does not
# bust the dependency layers.
ARG TARGETARCH

# Install build dependencies
RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Copy go mod files first for better layer caching (stable until go.mod changes)
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Target + versioning args declared here, after the cached dependency layers, so
# they only invalidate the build step below.
ARG CMD_PATH=cmd/base/watcher
ARG BIN=watcher
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown
ARG BUILD_TIME=unknown

# Build the binary for the target architecture (ARM64 for Graviton). The
# --mount cache persists Go's compiled-package cache (GOCACHE) on the builder
# across the per-service builds in a run, so the shared standard library and
# internal packages compile once and are reused instead of recompiled for every
# service. Modules still come from the cached `go mod download` layer above, so
# /go/pkg/mod is intentionally not mounted (an empty mount would hide them).
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build \
    -ldflags="-w -s -extldflags '-static' -X main.GitCommit=${GIT_COMMIT} -X main.GitBranch=${GIT_BRANCH} -X main.BuildTime=${BUILD_TIME} -X github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo.GitCommit=${GIT_COMMIT} -X github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo.BuildTime=${BUILD_TIME}" \
    -o /${BIN} \
    ./${CMD_PATH}

# -----------------------------------------------------------------------------
# Runtime Stage
# -----------------------------------------------------------------------------
FROM alpine:3.21

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN adduser -D -g '' appuser

WORKDIR /app

# Copy the service binary, keeping its per-service name (see BIN note above)
ARG BIN=watcher
COPY --from=builder /${BIN} /app/${BIN}
ENV APP_BIN=/app/${BIN}

USER appuser

# exec replaces the shell so the binary is PID 1 and receives signals directly;
# "$@" forwards any args set by the k8s deployment. APP_BIN keeps the process
# name service-specific so `pgrep -f <BIN>` health probes keep matching.
ENTRYPOINT ["/bin/sh", "-c", "exec \"$APP_BIN\" \"$@\"", "--"]
