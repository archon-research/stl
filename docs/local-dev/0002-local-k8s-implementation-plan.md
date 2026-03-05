# SEN-206: Local kind Cluster — Implementation Plan

**Linear**: SEN-206
**ADR**: ADR-0001 (SEN-205, `docs/adr/0001-local-k8s-with-kind.md`)
**Branch**: `angelos/SEN-206`

---

## Context

Implement the local Kubernetes environment. Replaces flat `docker-compose` with a `kind` 
cluster that mirrors production EKS. `make dev-up` now creates the kind cluster and 
deploys the full pipeline; the old docker-compose workflow is retired.

---

## Port Mapping Table (kind.yaml extraPortMappings)

| Host port | NodePort | Service       |
|-----------|----------|---------------|
| 5432      | 30432    | timescaledb   |
| 5433      | 30433    | temporal-db   |
| 6379      | 30379    | redis         |
| 4566      | 30566    | localstack    |
| 16686     | 30686    | jaeger UI     |
| 4317      | 30317    | OTLP gRPC     |
| 7233      | 30233    | temporal gRPC |
| 8233      | 30823    | temporal UI   |

---

## Files Created / Modified

**New files:**
```
stl-verify/
├── Dockerfile.migrate
└── k8s/
    ├── kind.yaml
    ├── namespace.yaml
    ├── infra/
    │   ├── timescaledb.yaml
    │   ├── redis.yaml
    │   ├── localstack.yaml            (embeds localstack-init/init-aws.sh as ConfigMap)
    │   ├── jaeger.yaml
    │   ├── temporal-db.yaml
    │   └── temporal.yaml              (includes temporal + temporal-ui)
    ├── jobs/
    │   └── migrate.yaml
    ├── config/
    │   └── configmap.yaml
    ├── apps/
    │   ├── watcher.yaml
    │   ├── temporal-worker.yaml
    │   └── workers/
    │       ├── oracle-price-worker.yaml
    │       ├── morpho-indexer.yaml
    │       └── sparklend-position-tracker.yaml
    └── README.md
```

**Modified files:**
- `stl-verify/Makefile` — updated `dev-up`/`dev-down`/`dev-reset`; added `dev-up-rebuild`, `dev-wipe`, `dev-wipe-data`; extended `docker-build-*` targets with `LOCAL=1`; added all `kind-*` sub-targets; updated `.PHONY`
- `stl-verify/Dockerfile.temporal-worker` — bumped Go base image from `1.25` to `1.26` to match `go.mod`
- `stl-verify/README.md` — updated Prerequisites (kind, kubectl, AWS profile)

---

## Step 1 — Cluster Config

### k8s/kind.yaml

Single control-plane node. All 8 services exposed via `extraPortMappings` (no `kubectl port-forward` needed).

Also mounts two host directories into the node for persistent storage (see Step 8):

```yaml
extraMounts:
  - hostPath: STL_DATA_DIR/timescaledb
    containerPath: /data/timescaledb
  - hostPath: STL_DATA_DIR/temporal-db
    containerPath: /data/temporal-db
```

`STL_DATA_DIR` is a placeholder substituted at cluster creation time with `$HOME/.stl-local`.

### k8s/namespace.yaml
Namespace `stl`.

**Makefile target:**
```makefile
kind-create:
	@mkdir -p $$HOME/.stl-local/timescaledb $$HOME/.stl-local/temporal-db
	kind get clusters | grep -q stl-local || \
	  sed "s|STL_DATA_DIR|$$HOME/.stl-local|g" k8s/kind.yaml | kind create cluster --config -
```

`kind-create` is idempotent: if the cluster already exists it is a no-op.

---

## Step 2 — Infra Manifests

All applied with: `kubectl apply -f k8s/namespace.yaml && kubectl apply -f k8s/infra/ -n stl`

### timescaledb.yaml
- `StatefulSet`, image `timescale/timescaledb:2.25.1-pg17`
- **hostPath** volume at `/data/timescaledb` (mapped from host via `extraMounts`) — data persists across cluster restarts
- Env: `POSTGRES_USER=postgres`, `POSTGRES_PASSWORD=postgres`, `POSTGRES_DB=stl_verify`
- `NodePort` service on 30432

### redis.yaml
- `Deployment`, image `redis:8.0-M04-alpine`
- Command: `redis-server --appendonly yes --maxmemory 4gb --maxmemory-policy allkeys-lru`
- `NodePort` service on 30379

### localstack.yaml
- `Deployment`, image `localstack/localstack:4.3`, env `SERVICES=sns,sqs`
- `ConfigMap` (`localstack-init`) containing `localstack-init/init-aws.sh` with `defaultMode: 0755`
- Mounted at `/etc/localstack/init/ready.d/init-aws.sh` (LocalStack auto-init)
- `NodePort` service on 30566

### jaeger.yaml
- `Deployment`, image `jaegertracing/all-in-one:1.76.0`, env `COLLECTOR_OTLP_ENABLED=true`
- Two `NodePort` services: 30686 (UI), 30317 (OTLP gRPC)

### temporal-db.yaml
- `StatefulSet`, image `postgres:16-alpine`
- **hostPath** volume at `/data/temporal-db` — Temporal workflow history persists across cluster restarts
- Env: `POSTGRES_USER=temporal`, `POSTGRES_PASSWORD=temporal`
- `NodePort` service on 30433

### temporal.yaml
- `Deployment` for `temporalio/auto-setup:1.25.2`
  - Env: `DB=postgres12`, `DB_PORT=5432`, `POSTGRES_USER=temporal`, `POSTGRES_PWD=temporal`, `POSTGRES_SEEDS=temporal-db`, `BIND_ON_IP=0.0.0.0`, `DEFAULT_NAMESPACE=sentinel`
  - `NodePort` service on 30233
- `Deployment` for `temporalio/ui:2.45.3`
  - Env: `TEMPORAL_ADDRESS=temporal:7233`
  - `enableServiceLinks: false` — prevents Kubernetes from injecting `TEMPORAL_PORT=tcp://...` which corrupts the UI's config file
  - `NodePort` service on 30823

**Makefile target:**
```makefile
kind-infra:
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/infra/ -n stl
	@echo "==> Waiting for timescaledb to be ready (may take a moment on first run — image pull)..."
	kubectl rollout status statefulset/timescaledb -n stl --timeout=120s
	@echo "==> Waiting for localstack to be ready..."
	kubectl rollout status deployment/localstack -n stl --timeout=120s
```

Note: `kubectl rollout status` is used instead of `kubectl wait --for=condition=ready` because the
latter races with pod scheduling and returns "no matching resources" if called too early.

---

## Step 3 — Dockerfile.migrate + migrate Job

### Dockerfile.migrate
Multi-stage build following the same pattern as other Dockerfiles in the repo:
- Builder: compiles `cmd/migrate`
- Runtime (`alpine:3.21`): non-root user, `WORKDIR /app`, copies binary + `db/migrations/`
- `ENTRYPOINT ["/app/migrate"]`

### k8s/jobs/migrate.yaml
```
Job:
  initContainer: postgres:alpine — loops on pg_isready -h timescaledb -U postgres
  container:     stl-migrate:local, imagePullPolicy: Never
                 envFrom: stl-config ConfigMap
  backoffLimit:  5
  restartPolicy: Never
```

**Makefile targets:**
```makefile
docker-build-migrate:
	docker build -t stl-migrate:local -f Dockerfile.migrate .

kind-load-migrate:
	kind load docker-image stl-migrate:local --name stl-local

kind-migrate:
	kubectl delete job/migrate --ignore-not-found -n stl
	kubectl apply -f k8s/jobs/migrate.yaml -n stl
	@echo "==> Waiting for migration to complete..."
	kubectl wait --for=condition=complete job/migrate -n stl --timeout=120s
	@echo "==> Migration logs:"
	kubectl logs -l job-name=migrate -n stl || true
```

Note: logs are shown after completion (not streamed) to avoid the race between the init container
finishing and the main container starting.

---

## Step 4 — ConfigMap + Secrets

### k8s/config/configmap.yaml (ConfigMap name: `stl-config`)

| Key | Value |
|-----|-------|
| `CHAIN_ID` | `1` |
| `AWS_REGION` | `us-east-1` *(see note)* |
| `AWS_ACCESS_KEY_ID` | `test` |
| `AWS_SECRET_ACCESS_KEY` | `test` |
| `AWS_SNS_TOPIC_ARN` | `arn:aws:sns:us-east-1:000000000000:stl-ethereum-blocks.fifo` |
| `AWS_SNS_ENDPOINT` | `http://localstack:4566` |
| `AWS_SQS_ENDPOINT` | `http://localstack:4566` |
| `DATABASE_URL` | `postgres://postgres:postgres@timescaledb:5432/stl_verify?sslmode=disable` |
| `REDIS_ADDR` | `redis:6379` |
| `JAEGER_ENDPOINT` | `jaeger:4317` |
| `ENVIRONMENT` | `development` |
| `ALCHEMY_HTTP_URL` | `https://eth-mainnet.g.alchemy.com/v2` |
| `ALCHEMY_WS_URL` | `wss://eth-mainnet.g.alchemy.com/v2` |
| `ENABLE_BACKFILL` | `false` |
| `TEMPORAL_HOST_PORT` | `temporal:7233` |
| `TEMPORAL_NAMESPACE` | `sentinel` |

> **Note on `AWS_REGION`**: Production EKS runs in `eu-west-1`, but LocalStack does not route to
> real AWS — the region is just a label used to construct consistent ARNs. The existing
> `localstack-init/init-aws.sh` already uses `us-east-1` for all topic/queue ARNs, so we keep
> `us-east-1` here to stay in sync with that script.

**Makefile target:**
```makefile
kind-config:
	kubectl apply -f k8s/config/configmap.yaml -n stl
```

`kind-config` must run before `kind-migrate` because the migrate Job references `stl-config`.

### Secret: `stl-secrets`

Contains `ALCHEMY_API_KEY`, `COINGECKO_API_KEY`, and `ETHERSCAN_API_KEY` — all fetched from AWS
Secrets Manager using the developer's local AWS credentials. No `.env` file needed; secrets never
touch the filesystem.

- **In EKS**: secrets will be injected via External Secrets Operator pulling from AWS Secrets
  Manager. The `stl-secrets` k8s Secret will be managed by the operator automatically.
- Both environments reference the same `stl-secrets` Secret via `envFrom` — app manifests are
  identical between local and EKS.

**Makefile target:**
```makefile
kind-secrets:
	@ALCHEMY_API_KEY=$$(aws secretsmanager get-secret-value \
	  --secret-id stl-sentinelstaging-alchemy-api-key \
	  --query SecretString --output text) || \
	  { echo "ERROR: Failed to fetch ALCHEMY_API_KEY. Set AWS_PROFILE to your staging account profile."; exit 1; }; \
	COINGECKO_API_KEY=$$(aws secretsmanager get-secret-value \
	  --secret-id coingecko_api_key \
	  --query SecretString --output text | jq -r '.coingecko_api_key // empty') || \
	  { echo "WARNING: Failed to fetch COINGECKO_API_KEY."; COINGECKO_API_KEY=''; }; \
	ETHERSCAN_API_KEY=$$(aws secretsmanager get-secret-value \
	  --secret-id etherscan_api_key \
	  --query SecretString --output text) || \
	  { echo "WARNING: Failed to fetch ETHERSCAN_API_KEY."; ETHERSCAN_API_KEY=''; }; \
	kubectl create secret generic stl-secrets \
	  --from-literal=ALCHEMY_API_KEY=$$ALCHEMY_API_KEY \
	  --from-literal=COINGECKO_API_KEY=$$COINGECKO_API_KEY \
	  --from-literal=ETHERSCAN_API_KEY=$$ETHERSCAN_API_KEY \
	  --namespace=stl \
	  --dry-run=client -o yaml | kubectl apply -f -
```

Requires `AWS_PROFILE` set to a profile with access to the staging account (e.g.
`export AWS_PROFILE=sentinelstaging`). The `--dry-run=client -o yaml | kubectl apply -f -` pattern
makes this idempotent on reruns.

---

## Step 5 — App Deployments

All deployments: `imagePullPolicy: Never`, `envFrom: [stl-config, stl-secrets]`.

### k8s/apps/watcher.yaml
- `Deployment`, image `stl-watcher:local`, `terminationGracePeriodSeconds: 30`

### k8s/apps/workers/oracle-price-worker.yaml
- Additional env: `AWS_SQS_QUEUE_URL=http://localstack:4566/000000000000/stl-ethereum-oracle-price.fifo`

### k8s/apps/workers/morpho-indexer.yaml
- Additional env: `AWS_SQS_QUEUE_URL=http://localstack:4566/000000000000/stl-ethereum-morpho-indexing.fifo`

### k8s/apps/workers/sparklend-position-tracker.yaml
- Additional env: `AWS_SQS_QUEUE_URL=http://localstack:4566/000000000000/stl-ethereum-sparklend-position.fifo`

### k8s/apps/temporal-worker.yaml
- `Deployment`, image `stl-temporal-worker:local`
- Creates Temporal schedules (`coingecko-price-fetch` every 5m, `data-validation` every 1h) on startup
- Requires `COINGECKO_API_KEY` (from `stl-secrets`); `ETHERSCAN_API_KEY` optional
- `TEMPORAL_HOST_PORT` and `TEMPORAL_NAMESPACE` from `stl-config`

### Makefile — `docker-build-*` targets with `LOCAL=1`

All 5 existing build targets (`watcher`, `oracle-price-worker`, `morpho-indexer`,
`sparklend-position-tracker`, `temporal-worker`) are wrapped with `ifdef LOCAL` to build
a native-arch `:local` image instead of the ECR ARM64 production build.

---

## Step 6 — Master Makefile Targets

### Warm/cold start

`dev-up` supports two modes via the `build-if-needed` helper:

```makefile
define build-if-needed
@if [ "$(COLD)" = "1" ] || ! docker image inspect $(1) >/dev/null 2>&1; then \
  $(MAKE) $(2); \
else \
  echo "  $(1) already present — skipping build (use COLD=1 to force rebuild)"; \
fi
endef
```

- **Warm start (default)**: skips `docker build` for any image that already exists locally.
  Useful for day-to-day `dev-down` / `dev-up` cycles.
- **Cold start**: `make dev-up-rebuild` forces a full rebuild of all images.
- **Auto cold**: if an image is missing (e.g. first run, or after `dev-wipe`), it is built
  automatically regardless of the `COLD` flag.

### `dev-up` structure (6 sections)

```
[1/6] Cluster        kind-create
[2/6] Infrastructure kind-infra, kind-config, kind-secrets
[3/6] Migrations     docker-build-migrate (if needed), kind-load-migrate, kind-migrate
[4/6] Watcher        docker-build-watcher (if needed), kind-load-watcher, kind-deploy-watcher
[5/6] Workers        docker-build-* (if needed), kind-load-workers, kind-deploy-workers,
                     docker-build-temporal-worker (if needed), kind-load/deploy-temporal-worker
[6/6] Ready          wait for all pods ready, kind-status
```

### Full target reference

```makefile
make dev-up          # warm start — reuses existing images if present
make dev-up-rebuild     # cold start — rebuilds all images from scratch
make dev-down        # delete cluster; data at ~/.stl-local persists
make dev-reset       # dev-down + dev-up (warm)
make dev-wipe        # prompts, then tears down cluster + deletes ~/.stl-local
make kind-secrets             # re-fetch secrets from AWS and reapply (then rollout restart if needed)
make kind-redeploy-watcher    # rebuild watcher image + reload + rollout restart
make kind-redeploy-worker NAME=<name>  # rebuild any worker + reload + rollout restart
```

---

## Step 7 — README

`k8s/README.md` covers prerequisites, quick start, service access, fast iteration, logs, secrets
update, teardown, and lifecycle commands.

---

## Step 8 — Persistent Storage

Database data survives `dev-down` / `dev-up` cycles via host-mounted directories:

| Data | Host path | In-cluster path |
|------|-----------|-----------------|
| TimescaleDB | `~/.stl-local/timescaledb` | `/data/timescaledb` |
| Temporal DB | `~/.stl-local/temporal-db` | `/data/temporal-db` |

The kind node mounts these via `extraMounts`; the StatefulSets use `hostPath` volumes pointing
to the in-cluster paths. Both directories are created automatically by `kind-create`.

To wipe all data and start fresh:
```bash
make dev-wipe        # prompts for confirmation, tears down cluster, deletes ~/.stl-local
make dev-up          # fresh cluster with empty databases
```

---

## Manual Steps Required (one-time, per developer)

1. Install `kind` and `kubectl`
2. Set `AWS_PROFILE` to your staging account profile (e.g. `export AWS_PROFILE=sentinelstaging`)

---

## Verification Checklist

1. `make dev-up` completes without errors, all pods `Running` or `Completed`
2. `psql postgres://postgres:postgres@localhost:5432/stl_verify -c '\dt'` — tables visible
3. `AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url=http://localhost:4566 --region us-east-1 sns list-topics` — shows `stl-ethereum-blocks.fifo`
4. `AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test aws --endpoint-url=http://localhost:4566 --region us-east-1 sqs list-queues` — shows all FIFO queues
5. `make kind-logs` — shows watcher block ingestion lines
6. http://localhost:16686 — Jaeger UI reachable
7. http://localhost:8233/namespaces/sentinel/schedules — shows `coingecko-price-fetch` and `data-validation` schedules
8. `make dev-down && make dev-up` — warm restart, Temporal schedules and DB data preserved
9. `make dev-wipe && make dev-up` — fresh start, empty databases
