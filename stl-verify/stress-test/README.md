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

All tests run as a k6 Job inside the cluster via `make k6-stress-test`.

```bash
# Point kubectl at the target cluster first (local kind or EKS)
aws eks update-kubeconfig --region eu-west-1 --name archon-staging  # EKS only

make k6-stress-test                          # defaults: Ethereum pace, 2 minutes
make k6-stress-test INTERVAL_MS=50           # high-frequency burst
make k6-stress-test INTERVAL_MS=250          # Base chain pace
make k6-stress-test REORG=1                  # with reorg injection (default depth 5, every 30s)
make k6-stress-test TARGET=staging DURATION=5m  # run against real AWS staging
```

The target truncates `block_states`, flushes Redis, and purges the SQS queues before
running, so each run starts from a clean state.

### Options

| Option | Default | Description |
|---|---|---|
| `TARGET` | _(local)_ | Set to `staging` to target real AWS instead of LocalStack |
| `SCENARIO` | `stress` | Script to run — `watcher-${SCENARIO}.js` in `stress-test/k6/` |
| `INTERVAL_MS` | `12000` | Block emission interval in ms |
| `DURATION` | `2m` | k6 test duration (e.g. `30s`, `5m`, `1h`) |
| `REORG` | _(off)_ | Any value enables reorg injection |
| `REORG_DEPTH` | `5` | Blocks rolled back per reorg (1–64) |
| `REORG_INTERVAL_S` | `30` | Seconds between reorgs |

### Chain interval reference

| Chain | `INTERVAL_MS` |
|---|---|
| Ethereum | `12000` (default) |
| BNB Chain | `2000` |
| Base | `250` |
| High-frequency / burst | `50` |

### Adding a new scenario

Drop a `<pipeline-component>.js` file in `stress-test/k6/scenarios/` and run:

```bash
make k6-stress-test SCENARIO=<pipeline-component>
```

Shared modules live in `stress-test/k6/lib/` and can be imported by any scenario:

```js
import { createReorgRunner } from './reorg.js';
```

---

## Verifying results

```bash
# Local
psql $DATABASE_URL -v chain_id=1 -f stress-test/verify/checks.sql

# EKS (stream the local file into the pod; -f cannot reference a path inside the container)
kubectl exec -i -n stl-stress-test timescaledb-0 -- psql -U postgres -d stl_verify \
  -v chain_id=1 < stress-test/verify/checks.sql
```

All three queries must return 0:
1. **gaps** — no missing block numbers in the canonical chain
2. **unexpected_orphans** — no orphaned blocks in the last 64 blocks (finality window)
3. **broken_parent_links** — parentHash continuity on the canonical chain

---

## Local setup (first time)

`make dev-up` deploys the mock blockchain server and points the watcher at it by default.
The mock starts with synthetic fixture blocks. To load real block data for stress testing:

```bash
# One-time: sync 500 real blocks from staging S3 into LocalStack (requires staging AWS credentials)
make stress-test-data

# Restart the mock to pick up real data
make kind-redeploy-mock-blockchain-server
```

Data persists across kind restarts. Re-run `stress-test-data` only if you need different blocks.

To switch the watcher between mock and real Alchemy at any time:
```bash
make kind-use-alchemy   # switch to real Alchemy (requires ALCHEMY_API_KEY)
make kind-use-mock       # switch back to mock
```

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
