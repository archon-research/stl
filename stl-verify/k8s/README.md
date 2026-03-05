# Local kind Cluster

Local Kubernetes environment for the STL live data pipeline using [kind](https://kind.sigs.k8s.io/)

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
make dev-up
```

This creates the `stl-local` kind cluster and deploys the full pipeline:

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

All services are accessible at `localhost` — no port-forwarding needed.

## Lifecycle

```bash
make dev-up          # warm start — reuses existing images if present
make dev-up-rebuild  # cold start — rebuilds all images from scratch
make dev-down        # delete cluster; database data persists
make dev-reset       # dev-down + dev-up (warm)
make dev-wipe        # tear down cluster and delete all persisted data (prompts for confirmation)
```

Database data (TimescaleDB, Temporal) is persisted at `~/.stl-local/` and survives
`dev-down` / `dev-up` cycles.

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

Non-sensitive environment variables are defined in `k8s/config/configmap.yaml` (`stl-config`) and applied automatically during `dev-up`. This includes database URLs, AWS endpoints, Temporal host, chain ID, and blockchain provider URLs.

Secrets (`ALCHEMY_API_KEY`, `COINGECKO_API_KEY`, `ETHERSCAN_API_KEY`) are loaded from `.env.secrets` at the repo root and stored in the `stl-secrets` Kubernetes secret. Run `make kind-secrets` to reapply them (e.g. after rotating keys or on a fresh cluster).

To override a config value locally, edit `k8s/config/configmap.yaml` and reapply:

```bash
kubectl apply -f k8s/config/configmap.yaml -n stl
kubectl rollout restart deployment/watcher -n stl
```

## Fast Iteration

Rebuild and redeploy after code changes:

```bash
make kind-redeploy-watcher                          # watcher
make kind-redeploy-worker NAME=morpho-indexer       # any worker
make kind-redeploy-worker NAME=oracle-price-worker
make kind-redeploy-worker NAME=sparklend-position-tracker
make kind-redeploy-worker NAME=temporal-worker
```

Restart all deployments without rebuilding (e.g. after updating secrets or config):

```bash
kubectl rollout restart deployment -n stl
```

## Updating Secrets

Update `.env.secrets` at the repo root then reapply:

```bash
make kind-secrets
kubectl rollout restart deployment -n stl
```
