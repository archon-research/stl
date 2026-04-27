# Shared Dockerfile for all cronjob workers.
# Usage: docker build --build-arg CRONJOB=anchorage-indexer -f cmd/cronjobs/Dockerfile .
FROM --platform=$BUILDPLATFORM golang:1.26.2-alpine AS builder

ARG TARGETARCH
ARG CRONJOB
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown
ARG BUILD_TIME=unknown

RUN apk add --no-cache git ca-certificates

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build \
    -ldflags="-w -s -extldflags '-static' -X main.GitCommit=${GIT_COMMIT} -X main.GitBranch=${GIT_BRANCH} -X main.BuildTime=${BUILD_TIME} -X github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo.GitCommit=${GIT_COMMIT} -X github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo.BuildTime=${BUILD_TIME}" \
    -o /cronjob \
    ./cmd/cronjobs/${CRONJOB}

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata
RUN adduser -D -g '' appuser

WORKDIR /app
COPY --from=builder /cronjob /app/cronjob

USER appuser

ENTRYPOINT ["/app/cronjob"]
