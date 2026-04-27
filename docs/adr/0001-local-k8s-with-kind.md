# ADR-0001: Local k8s with kind

**Status**: Accepted  
**Proposed**: @angelostheodosiadis  
**Date**: 2026-03-03  
**Deciders**: @vector

## Context

Local development currently runs infrastructure as flat Docker containers via `docker-compose`
(`dev-up`) while application binaries are run directly with `go run`. We want a local
development environment that runs the full live-data pipeline inside Kubernetes, the orchestrator
we have adopted for production deployments on EKS.

## Decision

Use **kind** (Kubernetes IN Docker) as the local cluster runtime.

A `make kind-up` target will bring up a full kind cluster containing:
- TimescaleDB (`timescale/timescaledb:2.25.1-pg17`) — primary database and local "tigerdata"
  endpoint, eliminating the staging tunnel requirement
- Redis
- LocalStack (SNS + SQS with full fan-out topology)
- Jaeger (OpenTelemetry tracing)
- DB migration Job
- Watcher Deployment (core live-data pipeline)
- Downstream worker Deployments (oracle-price-worker, morpho-indexer, sparklend-position-tracker)

All infra services are accessible at `localhost` via NodePort + kind `extraPortMappings`
(no background port-forward processes needed).

## Application Config and Secrets

Non-sensitive configuration (log level, chain IDs, service URLs, feature flags) is stored in
**ConfigMaps** and mounted as environment variables or volume files into each pod.

Sensitive values (Alchemy API key, database credentials, AWS credentials) are **not** stored in
the cluster. Instead, the local kind cluster is configured with AWS credentials (via the
developer's `~/.aws` credentials or environment variables) and pods fetch secrets at startup
directly from **AWS Secrets Manager**, matching the production pattern used on EKS with IAM roles.

This means:
- No plaintext secrets in k8s manifests or the repository
- The local and production secret-fetching path are identical, catching misconfiguration early
- Rotating a secret requires no manifest change — pods pick it up on next restart

## Startup Targets

Two `make` targets cover different developer workflows:

**`make kind-cold`** — Cold start, no local image cache assumed:
- Pulls all third-party images from registries (TimescaleDB, Redis, LocalStack, Jaeger)
- Builds application images from source (`docker build`)
- Loads all images into the kind cluster
- Applies manifests and waits for all pods to be Ready

**`make kind-warm`** — Warm start, images already present in the kind cluster:
- Skips image pulls and builds for unchanged images
- Re-applies manifests (picks up config/manifest changes)
- Waits for rollout to complete

`make kind-up` is an alias for `make kind-cold`. Developers iterating on application code will
typically run `make kind-warm` after the initial cold start to avoid redundant image builds.

## Alternatives Considered

**minikube** — Requires a VM driver (VirtualBox, HyperKit, etc.), heavier resource footprint,
and slower cluster startup. Offers no meaningful advantage over kind for our use case.

**k3d** (k3s in Docker) — Lightweight and fast, but k3s diverges from upstream Kubernetes in
places (e.g., custom storage classes, bundled components). kind more closely mirrors the
upstream k8s API surface, matching our EKS environment.

## Consequences

**Positive:**
- Full pipeline can be exercised locally without any cloud or staging dependency
- Service-to-service communication uses Kubernetes DNS, matching production topology on EKS
- Image builds and k8s manifest correctness are validated before deploying to EKS
- TimescaleDB runs locally, removing the `tigerdata` staging tunnel requirement
- Cluster is ephemeral: `make kind-down` removes everything cleanly

**Negative / Trade-offs:**
- Developers need `kind` and `kubectl` installed in addition to `docker`
- `make kind-up` is slower than `make dev-up` (image builds + cluster provisioning)
- k8s manifests are a new maintenance surface in the repo
- The `go run` workflow (`make dev-up` + `make run-watcher`) remains the faster path for
  tight code-change loops; kind is for full pipeline / integration testing
