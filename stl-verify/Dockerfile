# =============================================================================
# Watcher Dockerfile
# =============================================================================
# Multi-stage build for minimal production image

# -----------------------------------------------------------------------------
# Build Stage
# -----------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

# Build arguments for cross-compilation and versioning
ARG TARGETARCH
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown
ARG BUILD_TIME=unknown

# Install build dependencies
RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Copy go mod files first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary for target architecture (ARM64 for Fargate)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build \
    -ldflags="-w -s -extldflags '-static' -X main.GitCommit=${GIT_COMMIT} -X main.GitBranch=${GIT_BRANCH} -X main.BuildTime=${BUILD_TIME}" \
    -o /watcher \
    ./cmd/watcher

# -----------------------------------------------------------------------------
# Runtime Stage
# -----------------------------------------------------------------------------
FROM alpine:3.21

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN adduser -D -g '' appuser

WORKDIR /app

# Copy binary from builder
COPY --from=builder /watcher /app/watcher

# Use non-root user
USER appuser

# Health check placeholder - actual health check in ECS task definition
HEALTHCHECK NONE

# Default command (can be overridden by ECS task definition)
ENTRYPOINT ["/app/watcher"]
