# ADR-0001: Local k8s with kind

**Status**: Proposed
**Date**: 2026-03-03

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
