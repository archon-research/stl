# Stress Test

End-to-end stress testing for the block watcher pipeline using a mock blockchain server
that replaces Alchemy with controllable, deterministic block data.

## Architecture

```
S3 block data (loaded into memory at startup)
        │
        ▼
mock-blockchain-server  ─── WebSocket (newHeads) ──► watcher ──► DB / SNS / SQS
        │                ─── HTTP JSON-RPC ──────────► watcher
        │
admin API (:8547)  ◄─── k6 scripts (control: speed, scenario, reorg, error injection)
```

Block data is loaded once at startup from S3 (500 blocks). The replayer cycles through
these templates indefinitely, deriving unique block numbers and hashes per loop, producing
an infinite valid chain from finite data. k6 tests need no AWS credentials at runtime.

---

## Environments

### Local (kind)

Uses LocalStack for S3. Block data is exported once from staging and persisted in the
kind cluster volume. k6 runs locally, talking to the cluster via NodePort.

### EKS (`stl-stress-test` namespace)

All services run in an isolated namespace using real AWS services — no LocalStack.
k6 runs as a Kubernetes Job inside the cluster. See [EKS setup](#eks-stress-test-namespace)
below for prerequisites.

---

## Running a test

### Local

```bash
# Default: Ethereum (~12s blocks), sustained scenario, 2 minutes
./stress-test/run.sh

# Base chain (~250ms blocks), burst scenario
./stress-test/run.sh --scenario burst --interval 250

# Skip DB truncate (e.g. continuing a run)
./stress-test/run.sh --no-clean
```

Run from the `stl-verify/` directory. The script truncates `block_states` before each
run so the finality boundary is clean.

### EKS (in-cluster Job)

```bash
# Point kubectl at the target cluster first
aws eks update-kubeconfig --region eu-west-1 --name archon-staging

make k6-stress-test                                        # defaults
make k6-stress-test SCENARIO=burst INTERVAL_MS=250         # Base burst
make k6-stress-test SCENARIO=reorg INTERVAL_MS=2000 DURATION=5m
```

---

## Scenarios

| Scenario | Default interval | What it tests |
|---|---|---|
| `sustained` | 12000ms (Ethereum) | Steady block processing at configurable rate |
| `burst` | 50ms | High-frequency ingestion, throughput under load |

Reorgs are an option, not a scenario — inject them into any scenario:

```bash
# Sustained Ethereum with reorgs every 30s at depth 5 (defaults)
./stress-test/run.sh --reorg

# Base burst with reorgs every 10s at depth 3
./stress-test/run.sh --scenario burst --interval 250 --reorg --reorg-depth 3 --reorg-interval 10
```

### Chain interval reference

| Chain | `--interval` |
|---|---|
| Ethereum | 12000 (default) |
| BNB Chain | 2000 |
| Base | 250 |

---

## Verifying results

```bash
# Local
psql $DATABASE_URL -v chain_id=1 -f stress-test/verify/checks.sql

# EKS
kubectl exec -n stl-stress-test timescaledb-0 -- psql -U postgres -d stl_verify \
  -f stress-test/verify/checks.sql
```

All three queries must return 0:
1. **gaps** — no missing block numbers in the canonical chain
2. **unexpected_orphans** — no orphaned blocks in the last 64 blocks (finality window)
3. **broken_parent_links** — parentHash continuity on the canonical chain

---

## Local setup (first time)

### 1. Start the kind cluster

```bash
make dev-up
```

### 2. Export block data to LocalStack (one-time, needs staging AWS credentials)

```bash
make stress-test-data
```

Copies 500 blocks from the staging backup bucket into LocalStack. Data persists across
kind restarts. Re-run only if you need different blocks.

### 3. Deploy the mock server

```bash
make kind-redeploy-mock-blockchain-server
```

### 4. Point the watcher at the mock server

```bash
make kind-use-mock
```

Restore to real Alchemy at any time:
```bash
make kind-use-alchemy
```

---

## EKS stress test namespace

> **Status**: blocked — see [Pass 7 in the implementation plan](../../workspace/stress-test-implementation.md).

Prerequisites before EKS stress tests can run:
- All app manifests present (allocation-tracker, backup, event-persister, backfill)
- Dedicated ElastiCache instance provisioned in infra repo
- TigerData stress test database created
- IRSA role for `stl-stress-test` namespace (S3, SNS, SQS permissions)
- ArgoCD Application deployed pointing at `k8s/stress-test/`

Once in place, the only difference from local is the kubectl context and the absence of
LocalStack — real AWS services are used throughout.

---

## Admin API reference

| Endpoint | Method | Body / Params | Description |
|---|---|---|---|
| `/start` | POST | — | Start block emission |
| `/stop` | POST | — | Stop; returns `{"last_block": N}` |
| `/speed` | POST | `{"interval_ms": 200}` | Set block interval (works while running) |
| `/status` | GET | — | Running state, blocks emitted, reorg count |
| `/reorg` | POST | `?depth=N` | Trigger chain reorg (depth 1–64) |
| `/disconnect` | POST | — | Close WebSocket; watcher will reconnect |
| `/error` | POST | `{"mode": "429"\|"500"\|"timeout"\|"once429"\|"once500"\|"none"}` | Inject HTTP errors |
