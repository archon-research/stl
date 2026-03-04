# SEN-206: Local kind Cluster — Implementation Plan

**Linear**: SEN-206
**ADR**: ADR-0001 (SEN-205, `docs/adr/0001-local-k8s-with-kind.md`)
**Branch**: `angelos/SEN-206`

---

## Context

Implement the local Kubernetes environment described in ADR-0001 (SEN-205). Replaces flat
`docker-compose` with a `kind` cluster that mirrors production EKS. `make dev-up` now creates
the kind cluster and deploys the full pipeline; the old docker-compose workflow is retired.

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

## Files to Create / Modify

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
    │   └── workers/
    │       ├── oracle-price-worker.yaml
    │       ├── morpho-indexer.yaml
    │       └── sparklend-position-tracker.yaml
    └── README.md
```

**Modified files:**
- `stl-verify/Makefile` — replace `dev-up`/`dev-down`/`dev-reset` bodies with kind equivalents; extend existing `docker-build-*` targets with `LOCAL=1`; add new `kind-*` sub-targets; update `.PHONY`
- `stl-verify/README.md` — update Prerequisites (add `kind`, `kubectl`); update Quick Start to reflect `make dev-up` kind workflow

---

## Phase 1 — Cluster Config

### k8s/kind.yaml
Single control-plane node. All 8 services exposed via `extraPortMappings` so no `kubectl port-forward` is needed:

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: stl-local
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30432   # timescaledb
        hostPort: 5432
      - containerPort: 30433   # temporal-db
        hostPort: 5433
      - containerPort: 30379   # redis
        hostPort: 6379
      - containerPort: 30566   # localstack
        hostPort: 4566
      - containerPort: 30686   # jaeger UI
        hostPort: 16686
      - containerPort: 30317   # OTLP gRPC
        hostPort: 4317
      - containerPort: 30233   # temporal gRPC
        hostPort: 7233
      - containerPort: 30823   # temporal UI
        hostPort: 8233
```

### k8s/namespace.yaml
```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: stl
```

**Makefile targets (new):**
```makefile
kind-create:
	kind create cluster --config k8s/kind.yaml

kind-delete:
	kind delete cluster --name stl-local
```

---

## Phase 2 — Infra Manifests

All applied with: `kubectl apply -f k8s/namespace.yaml && kubectl apply -f k8s/infra/ -n stl`

### timescaledb.yaml
- `StatefulSet`, 1 replica, image `timescale/timescaledb:2.25.1-pg17`
- PVC 5Gi at `/var/lib/postgresql/data`
- Env: `POSTGRES_USER=postgres`, `POSTGRES_PASSWORD=postgres`, `POSTGRES_DB=stl_verify`
- `NodePort` service on 30432

### redis.yaml
- `Deployment`, image `redis:8.0-M04-alpine`
- Command: `redis-server --appendonly yes --maxmemory 4gb --maxmemory-policy allkeys-lru`
- `NodePort` service on 30379

### localstack.yaml
- `Deployment`, image `localstack/localstack:4.3`, env `SERVICES=sns,sqs`
- `ConfigMap` (`localstack-init`) containing the content of the existing `localstack-init/init-aws.sh`
- Mount at `/etc/localstack/init/ready.d/init-aws.sh` (LocalStack auto-init — runs after healthy)
- `NodePort` service on 30566

### jaeger.yaml
- `Deployment`, image `jaegertracing/all-in-one:latest`, env `COLLECTOR_OTLP_ENABLED=true`
- Two `NodePort` services: 30686 (UI), 30317 (OTLP gRPC)

### temporal-db.yaml
- `StatefulSet`, image `postgres:16-alpine`
- Env: `POSTGRES_USER=temporal`, `POSTGRES_PASSWORD=temporal`
- `NodePort` service on 30433

### temporal.yaml
- `Deployment` for `temporalio/auto-setup:1.25.2`
  - Env: `DB=postgres12`, `DB_PORT=5432`, `POSTGRES_USER=temporal`, `POSTGRES_PWD=temporal`, `POSTGRES_SEEDS=temporal-db`, `BIND_ON_IP=0.0.0.0`, `DEFAULT_NAMESPACE=sentinel`
  - `NodePort` service on 30233
- `Deployment` for temporal-ui: `temporalio/ui:2.45.3`, env `TEMPORAL_ADDRESS=temporal:7233`
  - `NodePort` service on 30823

**Makefile target (new):**
```makefile
kind-infra:
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/infra/ -n stl
	kubectl wait --for=condition=ready pod -l app=timescaledb -n stl --timeout=120s
	kubectl wait --for=condition=ready pod -l app=localstack   -n stl --timeout=120s
```

---

## Phase 3 — Dockerfile.migrate + migrate Job

### Dockerfile.migrate
Follows the same multi-stage pattern as all other Dockerfiles in the repo:
- Builder stage: compiles `cmd/migrate` binary
- Runtime stage (`alpine:3.21`):
  - `WORKDIR /app`
  - `COPY --from=builder /migrate /app/migrate`
  - `COPY db/migrations/ /app/db/migrations/`  ← binary looks for `./db/migrations` at runtime
  - Non-root user (`appuser`), same as other images
  - `ENTRYPOINT ["/app/migrate"]`

### k8s/jobs/migrate.yaml
```
Job:
  initContainer: postgres:alpine — loops on `pg_isready -h timescaledb -U postgres`
  container:     stl-migrate:local, imagePullPolicy: Never
                 env DATABASE_URL from stl-config ConfigMap
  backoffLimit:  5
  restartPolicy: Never
```

**Makefile targets (new):**
```makefile
docker-build-migrate:
	docker build -t stl-migrate:local -f Dockerfile.migrate .

kind-load-migrate:
	kind load docker-image stl-migrate:local --name stl-local

kind-migrate:
	kubectl delete job/migrate --ignore-not-found -n stl
	kubectl apply -f k8s/jobs/migrate.yaml -n stl
	kubectl wait --for=condition=complete job/migrate -n stl --timeout=120s
```

---

## Phase 4 — ConfigMap + Secrets

### k8s/config/configmap.yaml (ConfigMap name: `stl-config`)
Non-sensitive values shared across all pods:

| Key | Value |
|-----|-------|
| `CHAIN_ID` | `1` |
| `AWS_REGION` | `us-east-1` *(see note below)* |
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

> **Note on `AWS_REGION`**: Production EKS runs in `eu-west-1`, but LocalStack does not route to
> real AWS — the region is just a label used to construct consistent ARNs. The existing
> `localstack-init/init-aws.sh` already uses `us-east-1` for all topic/queue ARNs, so we keep it
> here to stay in sync and avoid divergence with that script.

### Secret: `stl-secrets`
- `ALCHEMY_API_KEY` — fetched directly from AWS Secrets Manager using the developer's local AWS
  credentials. No `.env.kind` file needed; secrets never touch the filesystem.
- **In EKS**: secrets will be injected via External Secrets Operator pulling from AWS Secrets
  Manager. The `stl-secrets` k8s Secret will be managed by the operator automatically.
- Both environments end up with the same `stl-secrets` k8s Secret referenced by pods via
  `envFrom` — app manifests are identical between local and EKS.

**Makefile target (new):**
```makefile
kind-secrets:
	kubectl create secret generic stl-secrets \
	  --from-literal=ALCHEMY_API_KEY=$$(aws secretsmanager get-secret-value \
	    --secret-id stl/alchemy-api-key --query SecretString --output text) \
	  --namespace=stl \
	  --dry-run=client -o yaml | kubectl apply -f -
```
(`--dry-run=client -o yaml | kubectl apply -f -` makes this idempotent on reruns.
Requires AWS CLI credentials configured locally — same requirement as the existing `make dev-env`.)

---

## Phase 5 — App Deployments

All deployments: `imagePullPolicy: Never`, `envFrom: [stl-config ConfigMap, stl-secrets Secret]`.
No liveness/readiness probes (watcher has no HTTP server; local dev only).

### k8s/apps/watcher.yaml
- `Deployment`, 1 replica, image `stl-watcher:local`
- `terminationGracePeriodSeconds: 30`

### k8s/apps/workers/oracle-price-worker.yaml
- `Deployment`, 1 replica, image `stl-oracle-price-worker:local`
- Additional env: `AWS_SQS_QUEUE_URL=http://localstack:4566/000000000000/stl-ethereum-oracle-price.fifo`

### k8s/apps/workers/morpho-indexer.yaml
- `Deployment`, 1 replica, image `stl-morpho-indexer:local`
- Additional env: `AWS_SQS_QUEUE_URL=http://localstack:4566/000000000000/stl-ethereum-morpho-indexing.fifo`

### k8s/apps/workers/sparklend-position-tracker.yaml
- `Deployment`, 1 replica, image `stl-sparklend-tracker:local`
- Additional env: `AWS_SQS_QUEUE_URL=http://localstack:4566/000000000000/stl-ethereum-sparklend-position.fifo`

### Makefile — modify existing `docker-build-*` targets

The 4 existing build targets are **modified** (not replaced) to support `LOCAL=1`. When passed,
they build a native-arch `:local` tagged image instead of the ECR ARM64 production build:

```makefile
# Pattern applied to all 4 existing targets:
docker-build-watcher:
ifdef LOCAL
	docker build -t stl-watcher:local -f Dockerfile .
else
	@$(MAKE) docker-build
endif

docker-build-oracle-price-worker:
ifdef LOCAL
	docker build -t stl-oracle-price-worker:local -f Dockerfile.oracle-price-worker .
else
	@$(MAKE) _docker-release-oracle-price-worker-internal  # existing behaviour
endif

# same pattern for morpho-indexer and sparklend-position-tracker
```

**New kind-specific targets:**
```makefile
kind-load-watcher:
	kind load docker-image stl-watcher:local --name stl-local
kind-load-workers:
	kind load docker-image stl-oracle-price-worker:local --name stl-local
	kind load docker-image stl-morpho-indexer:local       --name stl-local
	kind load docker-image stl-sparklend-tracker:local    --name stl-local

kind-deploy-watcher:
	kubectl apply -f k8s/apps/watcher.yaml -n stl
kind-deploy-workers:
	kubectl apply -f k8s/apps/workers/ -n stl

kind-logs:
	kubectl logs -f deployment/watcher -n stl

kind-redeploy-watcher:
	$(MAKE) docker-build-watcher LOCAL=1
	kind load docker-image stl-watcher:local --name stl-local
	kubectl rollout restart deployment/watcher -n stl
```

---

## Phase 6 — Master Makefile Targets

`dev-up`, `dev-down`, and `dev-reset` are **modified** to drive the kind cluster instead of
docker-compose. The `kind-*` targets remain as implementation building blocks.

```makefile
dev-up: kind-create kind-infra \
        docker-build-migrate kind-load-migrate kind-migrate \
        kind-secrets
	$(MAKE) docker-build-watcher LOCAL=1
	$(MAKE) kind-load-watcher kind-deploy-watcher
	$(MAKE) docker-build-oracle-price-worker LOCAL=1
	$(MAKE) docker-build-morpho-indexer LOCAL=1
	$(MAKE) docker-build-sparklend-position-tracker LOCAL=1
	$(MAKE) kind-load-workers kind-deploy-workers
	@echo "=== STL kind cluster ready ==="
	@$(MAKE) kind-status

dev-down: kind-delete

dev-reset: dev-down dev-up

kind-status:
	kubectl get pods -n stl
```

---

## Phase 7 — README

**`k8s/README.md`** — developer guide covering:
1. Prerequisites: `kind`, `kubectl`, `docker`, `aws` CLI with credentials configured
2. Quick start: `make dev-up` — what it does, expected output
3. Accessing services at localhost (no extra commands needed):
   - DB: `psql postgres://postgres:postgres@localhost:5432/stl_verify`
   - Redis: `redis-cli -h localhost -p 6379`
   - LocalStack: `aws --endpoint-url=http://localhost:4566 sns list-topics`
   - Jaeger UI: http://localhost:16686
   - Temporal UI: http://localhost:8233
4. Fast iteration: `make kind-redeploy-watcher`
5. Viewing logs: `make kind-logs`
6. Updating secrets: `make kind-secrets` (re-fetches from AWS Secrets Manager) + rollout restart
7. Tear down: `make dev-down`
8. `dev-up` now starts the kind cluster — the old docker-compose workflow is replaced

---

## Manual Steps Required (one-time, per developer)

1. Install `kind` and `kubectl` (documented in `stl-verify/README.md` Prerequisites)
2. Ensure AWS CLI is configured with credentials that have access to `stl/alchemy-api-key` in Secrets Manager

---

## Verification Checklist

1. `make dev-up` completes without errors
2. `kubectl get pods -n stl` — all pods `Running` or `Completed`
3. `psql postgres://postgres:postgres@localhost:5432/stl_verify -c '\dt'` — tables visible
4. `aws --endpoint-url=http://localhost:4566 sns list-topics` — shows `stl-ethereum-blocks.fifo`
5. `aws --endpoint-url=http://localhost:4566 sqs list-queues` — shows all FIFO queues
6. `make kind-logs` — shows watcher block ingestion log lines
7. http://localhost:16686 — Jaeger UI shows `watcher` service traces
8. http://localhost:8233 — Temporal UI accessible
9. `make dev-down && make dev-up` — full reset works (migrate Job reruns without error)
