# k8s — Kustomize manifests for Vector services

A single tree serves every environment. The `base/` manifests are env-agnostic
and reused by all overlays; each overlay layers in the environment-specific
image registry/tags, namespace, config, and secrets.

## Structure

```text
base/<service>/          # env-agnostic manifests (reused by every overlay)
  deployment.yaml        # resource sizes match ECS task (CPU/memory)
  serviceaccount.yaml    # no AWS annotations — IAM handled by Pod Identity (SEN-230)
  kustomization.yaml
overlays/
  prod/                  # namespace: vector, ECR: 030797368798 (EKS, ArgoCD)
  staging/               # namespace: vector, ECR: 579039992622 (EKS, ArgoCD)
  dev/                   # local kind: localhost/*:local images, shared config
    kustomization.yaml   #   core apps: watcher, python-api, cronjobs + stl-config
    config.yaml          #   stl-config ConfigMap (local, non-secret env)
    components/runtime/  #   shared patches: envFrom→stl-config/stl-secrets, imagePullPolicy:Never
    workers/             #   Alchemy-key workers (applied only when a key is set)
dev-infra/               # local-only artifacts (no EKS equivalent), applied imperatively by the Makefile
  timescaledb redis localstack jaeger temporal* mock-blockchain-server
  jobs/                  #   bootstrap-db, migrate, k6-stress-test
  kind.yaml              #   kind cluster definition (ports, persisted volumes)
```

The `dev` overlay reuses the same `base/` manifests as prod/staging. A Kustomize
[Component](overlays/dev/components/runtime/kustomization.yaml) swaps base's
per-service config for the local shared `stl-config` + `stl-secrets` and sets
`imagePullPolicy: Never`, while `images:` retags each service to its
`localhost/stl-*:local` image. EKS cronjobs and the local cronjobs now come from
the same base manifests (no generated manifests).

## How overlays work

Each overlay sets the target namespace and pins image names. For EKS, CI bumps
`newTag` per service on merge to main.

```bash
# Preview what gets applied
kubectl kustomize k8s/overlays/prod | kubectl diff -f -
kubectl kustomize k8s/overlays/dev          # local core apps
kubectl kustomize k8s/overlays/dev/workers  # local workers
```

## IAM / AWS credentials (EKS)

Service accounts have no IRSA annotations. AWS access is wired via **EKS Pod
Identity** associations in the infra repo (SEN-230) — no changes needed here.

## Namespaces

`vector` is managed by ArgoCD in the infra gitops repo. Do not add namespace
manifests for EKS here. (The local `dev-infra/temporal-00-namespace.yaml` creates
the `temporal` namespace in kind only.)

---

# Local development (kind)

Local Kubernetes environment for the STL live data pipeline using
[kind](https://kind.sigs.k8s.io/). Driven from `stl-verify/` via the Makefile,
which builds + loads local images and applies `overlays/dev` + `dev-infra/`.

## Prerequisites

- [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- Docker
- `.env.secrets` at the repo root — created automatically on first run with empty values; ask a team member for the API keys, then run `make kind-secrets`
- `psql` — for direct PostgreSQL access (`brew install libpq`)
- `redis-cli` — for direct Redis access (`brew install redis`)
- `aws` CLI — for LocalStack inspection (`brew install awscli`)
- [k9s](https://k9scli.io/topics/install/) (optional but recommended — terminal UI for browsing pods, logs, and events)

## Quick Start

```bash
cd stl-verify
make dev-up
```

This creates the `vector` kind cluster and deploys the full pipeline:

| Service | Host port |
|---------|-----------|
| TimescaleDB | 5432 |
| Temporal DB | 5433 |
| Redis | 6379 |
| LocalStack | 4566 |
| Jaeger UI | 16686 |
| OTLP gRPC | 4317 |
| Temporal gRPC | 7233 |
| Temporal UI | 8233 |
| Mock Blockchain RPC/WS | 8546 |
| Mock Blockchain Admin | 8547 |

All services are accessible at `localhost` — no port-forwarding needed.

The watcher uses the mock blockchain server by default instead of real Alchemy
(baked into the dev overlay). No API keys or AWS credentials are needed to get
started. Workers are deployed only when `ALCHEMY_API_KEY` is set in `.env.secrets`.

## Lifecycle

```bash
make dev-up          # warm start — reuses existing images if present
make dev-up-rebuild  # cold start — rebuilds all images from scratch
make dev-suspend     # suspend local kind nodes; preserve cluster state (local dev only)
make dev-resume      # resume suspended local kind nodes (local dev only)
make dev-down        # delete cluster; database data persists
make dev-reset       # dev-down + dev-up (warm)
make dev-wipe        # tear down cluster and delete all persisted data (prompts for confirmation)
```

`dev-suspend` / `dev-resume` are intended for local development only.
Do not use them in CI or production workflows.

Database data (TimescaleDB, Temporal) is persisted at `~/.vector/` and survives
`dev-down` / `dev-up` and `dev-suspend` / `dev-resume` cycles.

## Accessing Services

```bash
# PostgreSQL
psql postgres://postgres:postgres@localhost:5432/stl_verify

# Redis
redis-cli -h localhost -p 6379

# LocalStack
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url=http://localhost:4566 --region us-east-1 sns list-topics
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url=http://localhost:4566 --region us-east-1 sqs list-queues

# Jaeger UI
open http://localhost:16686

# Temporal UI
open http://localhost:8233/namespaces/sentinel/schedules
```

## Configuration

Non-sensitive environment variables are defined in
[overlays/dev/config.yaml](overlays/dev/config.yaml) (`stl-config`) and applied
automatically during `dev-up`. This includes database URLs, AWS endpoints,
Temporal host, chain ID, and blockchain provider URLs.

Secrets (`ALCHEMY_API_KEY`, `COINGECKO_API_KEY`, `ETHERSCAN_API_KEY`,
`ANCHORAGE_API_KEY`) are loaded from `.env.secrets` at the repo root and stored
in the `stl-secrets` Kubernetes secret. Run `make kind-secrets` to reapply them
(e.g. after rotating keys or on a fresh cluster).

To override a config value locally, edit `k8s/overlays/dev/config.yaml` and reapply:

```bash
kubectl --context=kind-vector apply -f k8s/overlays/dev/config.yaml -n vector
kubectl --context=kind-vector rollout restart deployment/watcher -n vector
```

## Switching Between Mock and Real Alchemy

By default, the watcher uses the mock blockchain server. To switch at any time
while the cluster is running:

```bash
# Switch to real Alchemy (requires ALCHEMY_API_KEY in .env.secrets)
make kind-secrets && make kind-use-alchemy

# Switch back to mock
make kind-use-mock
```

`kind-use-alchemy` strips the override from the running deployment, but the mock
default is baked into the dev overlay — a subsequent `make kind-deploy-apps`
(or `dev-up`) re-applies it and points the watcher back at the mock. Re-run
`make kind-use-alchemy` after redeploying if you want to stay on real Alchemy.

To load real block data (500 blocks from staging) into the mock server:

```bash
make kind-deploy-mock-blockchain-server-s3
```

## Fast Iteration

Rebuild and redeploy after code changes:

```bash
make kind-redeploy-watcher                          # watcher
make kind-redeploy-worker NAME=morpho-indexer       # any worker
make kind-redeploy-worker NAME=oracle-price-worker
make kind-redeploy-worker NAME=sparklend-position-tracker
make kind-redeploy-worker NAME=offchain-price-indexer
make kind-redeploy-worker NAME=watcher-data-validator
make kind-redeploy-worker NAME=anchorage-indexer    # deployed as spark-anchorage-indexer
```

Restart all deployments without rebuilding (e.g. after updating secrets or config):

```bash
kubectl --context=kind-vector rollout restart deployment -n vector
```

## Adding a service

Add a `base/<name>/` directory (deployment + serviceaccount + kustomization) like
the existing services, then reference it (with a local `images:` entry) from
`overlays/dev/kustomization.yaml` (or `overlays/dev/workers/`) and the prod/staging
overlays. Cronjobs follow the same pattern — there is no manifest generator.

## Updating Secrets

Update `.env.secrets` at the repo root then reapply:

```bash
make kind-secrets
kubectl --context=kind-vector rollout restart deployment -n vector
```
