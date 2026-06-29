# Vector — RPC transport runbook

Owner: vector team · Source rules: [alerts/vector-rpc.yaml](../../alerts/vector-rpc.yaml)

Every worker that dials Ethereum RPC through
`internal/pkg/rpchttp.DialEthereum` (oracle / morpho / sparklend / prime / psm3
indexers, dex bootstrap, and the backfillers via `NewBackfillerClient`) shares
one retry transport. It retries HTTP 429 / 5xx / network errors with capped
exponential backoff (250ms→10s, up to 5 retries) and emits transport-level
telemetry:

- `rpc_http_attempts_total{server_address, rpc_http_status_code}` — every HTTP attempt
- `rpc_http_retries_total{server_address, reason}` — each retry, `reason` ∈ `429` / `5xx` / `network`
- `rpc.retry` span events on the caller's active span (visible inline in a slow trace, e.g. `oracle.fetchPrices`)

These sit below the per-service logical-RPC metrics. A single slow logical RPC
call (e.g. `oracle_rpc_duration_seconds`) times the whole request including
silent backoff, so a high retry ratio here is what attributes that latency tail
to its upstream cause.

---

## VectorRPCRetryRatioHigh

**Severity:** warning · **For:** 15m

### What it means

For the labelled `service_name` and `server_address` (RPC host), more than 20%
of RPC attempts over the last 10m were retries. The transport masks upstream
flakiness as added latency, so this is the leading indicator of a
throttle-driven latency tail (it typically precedes / accompanies a
`Vector*IndexerRPCLatencyHigh` warning on the same chain).

### First checks (≤5 min)

1. **Break down by reason** — the single most useful query:
   ```promql
   sum by (reason) (rate(rpc_http_retries_total{k8s_namespace_name="vector", service_name="<svc>"}[10m]))
   ```
   - `reason="429"` dominating → upstream **rate-limiting** (Alchemy compute-unit
     throttling). The Alchemy API key is shared across all workers and chains,
     so a burst from any worker can throttle the whole account.
   - `reason="5xx"` → upstream **server errors**; usually transient provider
     degradation.
   - `reason="network"` → connection resets / DNS / TLS / dial failures.
2. **Confirm the latency impact** — check the matching per-service RPC latency
   metric (e.g. `oracle_rpc_duration_seconds` p99) and whether a
   `Vector*IndexerRPCLatencyHigh` alert is firing for the same chain.
3. **Alchemy status / quota** — https://status.alchemy.com/ and the Alchemy
   dashboard compute-unit usage for the account.

### Common causes

- **Account-wide compute-unit (CU) throttling on the shared Alchemy key**
  (`reason="429"`). Cross-worker bursts (many chains getting new blocks at once,
  or a heavy backfill) push the account over its per-second CU budget. The call
  itself is cheap; the seconds come from our backoff. This is the known shape
  behind the avalanche-c oracle latency tail.
- **Transient provider degradation** (`reason="5xx"`) — wait for upstream
  recovery; nothing to do on our side unless sustained.
- **Network / connection churn** (`reason="network"`) — check pod egress and
  the node's connectivity if it is localized to one pod.

### What to do

- Transient (ratio falls back under 20% within an interval or two): no action,
  this is the transport doing its job.
- Sustained 429s: raise the Alchemy CU/throughput limit, or reduce cross-worker
  burst pressure (stagger schedules / lower per-worker concurrency). As a
  freshness-over-completeness lever, a service can lower its dial timeout
  (`WithClientTimeout`) or retry budget so a throttled call fails fast and the
  block is reprocessed rather than blocking the worker.
- Sustained 5xx/network against one host only: consider failing that chain over
  to an alternate RPC provider if available.

### Related

- `docs/runbooks/vector-indexers.md` — the per-service RPC latency alerts this
  metric explains.
- The retry transport and its telemetry live in
  `internal/pkg/rpchttp/retry_transport.go`.
