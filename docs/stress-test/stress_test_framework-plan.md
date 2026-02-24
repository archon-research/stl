# 001-Stress Testing Infrastructure for STL Verify

| Field       | Value                |
|-------------|----------------------|
| **Status**  | Accepted             |
| **Date**    | 2026-02-23           |
| **Authors** | Angelos Theodosiadis |

## Context

STL Verify is a mission-critical pipeline that ingests blockchain data in real time, and processes it through a hexagonal architecture (LiveService).

We have no way to validate system behavior under sustained load, measure throughput ceilings, or detect performance regressions before they hit production. Unit and integration tests verify correctness but not capacity or stability over time.

### Requirements

- **Maximum realism**: The watcher must run completely unmodified — no test-only code paths, no mock ports injected at runtime.
- **Reproducibility**: Tests must use the same dataset every run so results are comparable.
- **Phased rollout**: Phase 1 covers the watcher and byte-pass-through workers. Phase 2 extends to workers that make on-chain calls.
- **No production data access**: All seed data must come from the staging environment.
- **Deploy to dev AWS**: Stress tests run against real AWS infrastructure.

## Decision

### Phase Boundaries

The pipeline has two categories of downstream workers:

| Worker | Reads | On-chain RPC calls | Writes |
|--------|-------|---------------------|--------|
| raw-data-backup | SQS + Redis | None — pure byte copy | S3 |
| oracle-price-worker | SQS + PostgreSQL | eth\_call via Multicall3 (oracle prices per block) | PostgreSQL |
| sparklend-position-tracker | SQS + Redis | Heavy eth\_call via Multicall3 (user reserves, balances, configs) | PostgreSQL |

- **Phase 1**: Watcher + raw-data-backup. Neither makes on-chain RPC calls. All data is either opaque metadata (block\_states) or raw cached bytes (Redis to S3). The mock-blockchain-server only needs to serve the 5 JSON-RPC methods the watcher calls.
- **Phase 2**: oracle-price-worker + sparklend-position-tracker. Both make eth\_call/Multicall3 calls at specific block numbers. These responses are not cached anywhere — they are consumed, transformed into domain entities, and discarded. Supporting these workers requires capturing and replaying eth\_call responses (see Phase 2 section below).

### Mock Blockchain Server

Build a standalone Go binary (mock-blockchain-server) that speaks the real Ethereum JSON-RPC protocols — both WebSocket (subscription notifications) and HTTP (batch RPC). The watcher connects to it via ETH\_RPC\_WS\_URL and ETH\_RPC\_HTTP\_URL environment variable overrides, with zero code changes.

Why a mock server instead of alternatives:

| Alternative | Why rejected |
|-------------|-------------|
| Injecting mock ports at runtime | Bypasses adapters (Alchemy client, Redis, SNS) — not realistic |
| Public testnet (Sepolia/Holesky) | Unpredictable block times, can't control throughput, inconsistent data shapes |
| Replaying against real Alchemy | Costs money per request, rate-limited, can't accelerate beyond real time |
| k6 driving the watcher directly | Watcher is a consumer (WebSocket subscriber), not a server — k6 can't push to it |

### Template-Based Infinite Replay

Store a template of 500 real blocks from staging. The replayer loops through them infinitely, renumbering block numbers, recomputing deterministic hashes (SHA-256 of templateHash:loopIndex), and maintaining parentHash chain continuity. This avoids duplicate detection while keeping realistic data shapes (transaction counts, receipt sizes, trace depths).

The first block's parentHash is set to 0x0...0 (genesis-style) since tests always start from a clean database.

### Data Pipeline

> Staging Redis/S3 → **stress-data-export** → S3 bucket → **mock-blockchain-server** (loads into memory) → **watcher** (unmodified) → PostgreSQL + Redis + SNS FIFO → **raw-data-backup** (Phase 1) → S3

1. **stress-data-export** (ad-hoc CLI): Reads 500 consecutive blocks from staging Redis cache (or S3 backup). Uploads to S3 at `s3://{bucket}/stress-test/{chain}/{blockNumber}/{dataType}.json`. Requires `--env staging` flag — production access is forbidden.

2. **mock-blockchain-server**: Downloads the dataset from S3 into memory on startup. Serves:
   - **WebSocket**: `eth_subscribe` → subscription ID, then emits `eth_subscription` notifications at configurable intervals (default 12s, configurable down to 0 for max throughput). Implements ping/pong keepalive matching Alchemy's protocol (30s ping, 10s pong timeout).
   - **HTTP JSON-RPC**: `eth_getBlockByHash`, `eth_getBlockReceipts`, `trace_block`, `eth_getBlobSidecars`, `eth_blockNumber`. Supports both single and batch requests.
   - **Admin API** (HTTP): `/start`, `/stop`, `/speed`, `/status`, `/reorg` for k6 orchestration.

3. **k6 scripts**: Orchestrate the test in 3 phases:
   - **Pre-test**: Truncate `block_states` and `reorg_events` tables, call `/start` on mock server.
   - **Execute**: Monitor `/status` for progress, collect timing metrics from admin API.
   - **Drain & verify**: Call `/stop`, poll PostgreSQL until the last emitted block has `block_published = true`, then run verification queries (gap detection, reorg correctness, total block count).

### Reorg Simulation

The admin API exposes a `/reorg` endpoint that accepts a depth parameter. When triggered, the replayer:
1. Rewinds N blocks
2. Re-emits them with different hashes (same block numbers)
3. Resumes normal replay

This tests the watcher's `HandleReorgAtomic` path, which marks old blocks as orphaned, saves new versions, and republishes.

### Backfill Isolation

The watcher's backfill service is disabled during stress tests via `ENABLE_BACKFILL=false`. Backfill runs ad-hoc (not alongside the live pipeline), so this reflects realistic deployment configuration and prevents interference with measurements.

### Verification Strategy (Phase 1)

Phase 1 verifies the watcher's contract boundary and the byte-pass-through path to S3:

- `block_states` row count matches emitted block count
- Zero gaps in block number sequence (`FindGaps` query returns empty)
- All blocks have `block_published = true`
- Reorg events recorded correctly (orphaned blocks flagged, new versions present)
- No duplicate block numbers with the same version
- S3 backup objects exist for each block (raw-data-backup verification)

### Phase 2: On-Chain-Dependent Workers (Future)

Phase 2 extends stress testing to oracle-price-worker and sparklend-position-tracker. The core challenge: both workers make `eth_call`/Multicall3 calls against specific block numbers, and these responses are **never cached** in raw form — they're consumed, transformed into domain data (`onchain_token_price` rows, position/event rows), and discarded.

Since the mock server replays renumbered blocks that don't exist on-chain, the workers can't call real Alchemy — the block numbers won't correspond to real chain state.

**Approaches under consideration (to be decided before Phase 2 implementation):**

| Approach | How it works | Pros | Cons |
|----------|-------------|------|------|
| **Capture & replay `eth_call`** | During data export, replay the same Multicall3 calls against staging at the original block numbers. Store raw responses keyed by `(calldata, blockNumber)`. Mock server matches incoming `eth_call` by calldata + renumbered→original block mapping. | Full realism — exact same bytes | Complex: must capture all possible calldata patterns. Calldata varies by block (different users, different oracles). Large dataset. |
| **Static/shaped responses** | Mock server returns valid-structured but synthetic multicall results for any `eth_call`. Prices and balances are plausible but fake. | Simple to implement. Tests throughput and pipeline correctness. | Doesn't test real data transformation edge cases (zero prices, extreme values, new tokens). |
| **Hybrid: real Alchemy + original block numbers** | Don't renumber blocks for Phase 2 tests. Replay the original 500 block numbers (accepting they'll eventually repeat). Workers call real Alchemy with real block numbers. | Maximum realism for `eth_call` results. | Costs Alchemy credits. Can't accelerate beyond real-time easily. Block number collisions on repeat loops. |

**Additional data export work for Phase 2:**
- The `stress-data-export` tool would need to also execute and record `eth_call` responses for each block in the template set.
- Oracle-price-worker calls: Multicall3 aggregating all enabled oracle `latestAnswer()` calls, plus DirectCaller for Chronicle oracles.
- SparkLend-position-tracker calls: `getUserReservesData`, `ERC20.balanceOf` (batched), `getReserveConfigurationData`, `getReserveData` — calldata varies per block depending on which users interacted.

**Phase 2 verification** would check:
- `onchain_token_price` rows written for each block
- SparkLend position/event rows match expected counts
- No worker errors or SQS dead-letter queue messages

## Repository Structure

- stl/stress-test/ — Non-Go artifacts
  - k6/watcher-sustained.js — Sustained throughput test
  - k6/watcher-burst.js — Burst/accelerated test
  - k6/watcher-reorg.js — Reorg simulation test
  - verify/checks.sql — Post-test verification queries
  - README.md
- stl/stl-verify/cmd/mock-blockchain-server/main.go — Binary entry point
- stl/stl-verify/cmd/stress-data-export/main.go — Data exporter entry point
- stl/stl-verify/internal/testutil/mockchain/ — Mock server package (extends existing testutil patterns)
  - server.go — Top-level server (wires WS + HTTP + Admin)
  - websocket.go — eth\_subscribe, notifications, keepalive
  - jsonrpc.go — HTTP JSON-RPC handler (builds on testutil/ethrpc.go patterns)
  - admin.go — Admin API (start/stop/speed/status/reorg)
  - replayer.go — Template looping, renumbering, hash generation
  - reorg.go — Reorg simulation logic
  - datastore.go — In-memory block data store (loaded from S3)
- stl/stl-verify/internal/testutil/export/ — Data exporter package
  - redis.go — Read from staging Redis
  - s3.go — Read from staging S3 / upload to stress-test S3
- stl/stl-verify/Dockerfile.mock-blockchain-server

## Consequences

### Positive

- **Watcher runs unmodified** — stress tests exercise the exact code that runs in production, including the Alchemy adapter's WebSocket reconnection, JSON-RPC parsing, retry logic, and batch handling.
- **Reproducible** — same 500-block dataset, deterministic renumbering, stored in S3. Results are comparable across runs.
- **Cheap** — no Alchemy API calls during stress tests. Only AWS infrastructure costs (dev environment).
- **Extensible** — Phase 2 adds downstream consumers without changing the mock server. k6 scripts can verify additional tables.
- **Reorg testing** — first-class reorg simulation via admin API, exercising a critical correctness path that's hard to test in production.

### Negative

- **Mock server maintenance** — must stay in sync with any new JSON-RPC methods the watcher starts calling. Mitigated: the mock server returns 501 for unknown methods, which surfaces as a test failure immediately.
- **Memory footprint** — 500 blocks loaded into memory (~50-150MB compressed, more decompressed). Acceptable for a test tool.
- **No network realism** — mock server runs in the same VPC as the watcher, so latency is negligible vs. real Alchemy calls. Although this is not necessarily a problem, we could add artificial latency via admin API in the future.
- **Renumbered data isn't semantically valid** — hashes are deterministic but not real Keccak-256 hashes. Transaction hashes inside blocks don't change. Acceptable because the watcher treats block data as opaque bytes (caches and forwards without parsing transaction contents).

## Implementation Estimate

| Step | Effort |
|------|--------|
| Data exporter (staging Redis/S3 → S3) | ~2 days |
| Mock server core (WS + HTTP + in-memory store) | ~3 days |
| Replayer + renumbering + parentHash continuity | ~1.5 days |
| Admin API (start/stop/speed/status/reorg) | ~0.5 days |
| Dockerfile + Makefile targets | ~0.5 days |
| First k6 script (watcher-sustained) | ~1.5 days |
| Reorg simulation | ~1 day |
| Remaining k6 scripts + checks.sql | ~1.5 days |
| README + documentation | ~0.5 days |
| **Total** | **~12 days** |